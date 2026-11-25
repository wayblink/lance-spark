/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.read;

import org.lance.spark.utils.Optional;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.GeneralScalarExpression;
import org.apache.spark.sql.connector.expressions.Literal;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.And;
import org.apache.spark.sql.connector.expressions.filter.Not;
import org.apache.spark.sql.connector.expressions.filter.Or;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.types.DataTypes;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class FilterPushDown {
  /**
   * Renders a Spark {@code TimestampType} (micros-since-epoch UTC) as a {@code uuuu-MM-dd HH:mm:ss}
   * literal in UTC. Fractional seconds are emitted only when non-zero. Pinned to {@link
   * ZoneOffset#UTC} and {@link Locale#ROOT} so the pushed literal is independent of the driver
   * JVM's default time zone and locale.
   */
  private static final DateTimeFormatter UTC_TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern("uuuu-MM-dd HH:mm:ss")
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .toFormatter(Locale.ROOT);

  /**
   * Escape character used for the {@code LIKE} rewrites of {@code STARTS_WITH}, {@code ENDS_WITH}
   * and {@code CONTAINS}.
   *
   * <p>Spark's {@code StringPredicate} children are values, not LIKE patterns: {@code %} and {@code
   * _} in the needle must match literally. The rewrite therefore escapes those characters (plus the
   * escape character itself).
   *
   * <p>Lance's SQL planner accepts <em>only</em> the backslash as a LIKE escape character ({@code
   * rust/lance-datafusion/src/planner.rs}: "LIKE does not support escape_char other than the
   * backslash"), so the generated SQL always declares {@code ESCAPE '\'} and a backslash in the
   * needle is doubled.
   */
  private static final char LIKE_ESCAPE = '\\';

  /**
   * Functions whose pushdown has been shown row-for-row equivalent to Spark's evaluation by {@code
   * BaseFilterPushDownEquivalenceTest}.
   *
   * <p>{@code /} is deliberately absent: Spark's {@code Divide} returns a double for integral
   * operands, while the Lance/DataFusion plan keeps integral division, so the two are not
   * equivalent. {@code +}, {@code -}, {@code *} and {@code %} keep the operand type in both
   * engines.
   */
  private static final Set<String> SUPPORTED_SCALAR_FUNCTIONS =
      new LinkedHashSet<>(Arrays.asList("+", "-", "*", "%", "UPPER", "LOWER", "CHAR_LENGTH"));

  /** Comparison predicates rendered as a binary operator. */
  private static final Set<String> COMPARISON_PREDICATES =
      new LinkedHashSet<>(Arrays.asList("=", "<", "<=", ">", ">=", "<>"));

  /** String predicates rendered as a {@code LIKE} pattern. */
  private static final Set<String> STRING_PREDICATES =
      new LinkedHashSet<>(Arrays.asList("STARTS_WITH", "ENDS_WITH", "CONTAINS"));

  /**
   * Create SQL 'where clause' from Spark V2 predicates.
   *
   * @param predicates Supported V2 predicates
   * @return where clause, or Optional.empty() if no predicates compile to SQL
   */
  public static Optional<String> compileFiltersToSqlWhereClause(Predicate[] predicates) {
    if (predicates.length == 0) {
      return Optional.empty();
    }
    List<String> compiled = new ArrayList<>();
    for (Predicate predicate : predicates) {
      compilePredicate(predicate).ifPresent(compiled::add);
    }
    if (compiled.isEmpty()) {
      return Optional.empty();
    }
    String whereClause =
        compiled.stream().map(p -> "(" + p + ")").collect(Collectors.joining(" AND "));
    return Optional.of(whereClause);
  }

  /** Returns true if {@code predicate} references any of the given top-level columns. */
  public static boolean referencesAny(Predicate predicate, Set<String> columns) {
    if (columns.isEmpty()) {
      return false;
    }
    for (NamedReference ref : predicate.references()) {
      String[] names = ref.fieldNames();
      if (names.length > 0 && columns.contains(names[0])) {
        return true;
      }
    }
    return false;
  }

  public static boolean isPredicateSupported(Predicate predicate) {
    if (predicate instanceof And) {
      And and = (And) predicate;
      return isPredicateSupported(and.left()) && isPredicateSupported(and.right());
    }
    if (predicate instanceof Or) {
      Or or = (Or) predicate;
      return isPredicateSupported(or.left()) && isPredicateSupported(or.right());
    }
    if (predicate instanceof Not) {
      Not not = (Not) predicate;
      return isPredicateSupported(not.child());
    }
    String name = predicate.name();
    if (COMPARISON_PREDICATES.contains(name)) {
      return isBinaryShape(predicate);
    }
    if ("IS_NULL".equals(name) || "IS_NOT_NULL".equals(name)) {
      Expression[] children = predicate.children();
      return children.length == 1 && isSupportedOperand(children[0]);
    }
    if ("IN".equals(name)) {
      return isInShape(predicate);
    }
    if (STRING_PREDICATES.contains(name)) {
      return isStringPredicateShape(predicate);
    }
    if ("LIKE".equals(name)) {
      return isLikePredicateShape(predicate);
    }
    return false;
  }

  /**
   * A comparison is pushable when both sides are operands this class can render and the right side
   * is a literal. Spark hands comparisons literal-on-the-right (it flips the operands while
   * building the V2 predicate); requiring it here keeps the translation free of ordering
   * assumptions and leaves column-to-column comparisons to Spark's residual filter.
   */
  private static boolean isBinaryShape(Predicate predicate) {
    Expression[] children = predicate.children();
    if (children.length != 2) {
      return false;
    }
    if (!isSupportedOperand(children[0])) {
      return false;
    }
    return children[1] instanceof Literal && isSupportedLiteral((Literal<?>) children[1]);
  }

  private static boolean isInShape(Predicate predicate) {
    Expression[] children = predicate.children();
    if (children.length < 2 || !isSupportedOperand(children[0])) {
      return false;
    }
    for (int i = 1; i < children.length; i++) {
      if (!(children[i] instanceof Literal) || !isSupportedLiteral((Literal<?>) children[i])) {
        return false;
      }
    }
    return true;
  }

  private static boolean isStringPredicateShape(Predicate predicate) {
    Expression[] children = predicate.children();
    return children.length == 2 && isSupportedOperand(children[0]) && isStringLiteral(children[1]);
  }

  /**
   * A bare {@code LIKE} predicate is accepted only for a string literal pattern without a
   * backslash. The pattern is passed through verbatim (its wildcards are Spark's wildcards) and
   * Lance evaluates it with its default backslash escape.
   *
   * <p>Spark 3.5 and 4.0's {@code V2ExpressionBuilder} never produces a {@code LIKE} predicate: a
   * plain {@code LIKE 'prefix%'} is first simplified into {@code StartsWith}/{@code
   * EndsWith}/{@code Contains} and a pattern that cannot be simplified stays a Spark residual. This
   * arm therefore only matters for callers that build the predicate directly, but keeping it here
   * makes the whitelist complete instead of silently dropping a shape the V2 API allows.
   */
  private static boolean isLikePredicateShape(Predicate predicate) {
    Expression[] children = predicate.children();
    if (children.length != 2
        || !isSupportedOperand(children[0])
        || !(children[1] instanceof Literal)) {
      return false;
    }
    Literal<?> literal = (Literal<?>) children[1];
    String pattern = stringValue(literal);
    return pattern != null && pattern.indexOf('\\') < 0;
  }

  private static boolean isStringLiteral(Expression expression) {
    return expression instanceof Literal && stringValue((Literal<?>) expression) != null;
  }

  private static String stringValue(Literal<?> literal) {
    if (literal.value() == null) {
      return null;
    }
    if (literal.value() instanceof org.apache.spark.unsafe.types.UTF8String) {
      return literal.value().toString();
    }
    if (literal.value() instanceof String) {
      return (String) literal.value();
    }
    return null;
  }

  private static boolean isSupportedLiteral(Literal<?> literal) {
    if (isTimeTypeLiteral(literal)) {
      // Lance's custom SQL planner (lance-datafusion/src/planner.rs) does not yet support
      // SQLDataType::Time in parse_type(), so TimeType literals can't be pushed down. Reject
      // and let Spark evaluate the predicate post-scan.
      return false;
    }
    return true;
  }

  private static boolean isSupportedOperand(Expression expression) {
    if (expression instanceof NamedReference) {
      return true;
    }
    if (expression instanceof Literal) {
      return isSupportedLiteral((Literal<?>) expression);
    }
    if (expression instanceof GeneralScalarExpression) {
      GeneralScalarExpression scalar = (GeneralScalarExpression) expression;
      if (!SUPPORTED_SCALAR_FUNCTIONS.contains(scalar.name())) {
        return false;
      }
      Expression[] children = scalar.children();
      switch (scalar.name()) {
        case "+":
        case "-":
        case "*":
        case "%":
          if (children.length != 2) {
            return false;
          }
          break;
        default:
          if (children.length != 1) {
            return false;
          }
          break;
      }
      for (Expression child : children) {
        if (!isSupportedOperand(child)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Detects a Spark TimeType literal via class name to stay version-safe — {@code TimeType} only
   * exists in Spark 4.1+, so a direct {@code instanceof TimeType} would not compile against older
   * Spark versions in the base module.
   */
  private static boolean isTimeTypeLiteral(Literal<?> literal) {
    return literal.dataType() != null
        && "org.apache.spark.sql.types.TimeType".equals(literal.dataType().getClass().getName());
  }

  private static Optional<String> compilePredicate(Predicate predicate) {
    if (predicate instanceof And) {
      And and = (And) predicate;
      Optional<String> left = compilePredicate(and.left());
      Optional<String> right = compilePredicate(and.right());
      if (left.isEmpty() || right.isEmpty()) {
        // A conjunct that cannot be rendered must keep the whole conjunction out of the pushed
        // filter: dropping one side would silently widen the server-side filter.
        return Optional.empty();
      }
      return Optional.of(String.format("(%s) AND (%s)", left.get(), right.get()));
    }
    if (predicate instanceof Or) {
      Or or = (Or) predicate;
      Optional<String> left = compilePredicate(or.left());
      Optional<String> right = compilePredicate(or.right());
      if (left.isEmpty() || right.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(String.format("(%s) OR (%s)", left.get(), right.get()));
    }
    if (predicate instanceof Not) {
      Not not = (Not) predicate;
      Optional<String> child = compilePredicate(not.child());
      if (child.isEmpty()) {
        return child;
      }
      return Optional.of(String.format("NOT (%s)", child.get()));
    }

    Expression[] children = predicate.children();
    String name = predicate.name();
    switch (name) {
      case "=":
        return binaryOp(children, "==");
      case "<":
        return binaryOp(children, "<");
      case "<=":
        return binaryOp(children, "<=");
      case ">":
        return binaryOp(children, ">");
      case ">=":
        return binaryOp(children, ">=");
      case "<>":
        return binaryOp(children, "!=");
      case "IS_NULL":
        return unaryOp(children, "IS NULL");
      case "IS_NOT_NULL":
        return unaryOp(children, "IS NOT NULL");
      case "IN":
        return inOp(children);
      case "STARTS_WITH":
        return stringOp(children, true, false);
      case "ENDS_WITH":
        return stringOp(children, false, true);
      case "CONTAINS":
        return stringOp(children, true, true);
      case "LIKE":
        return likeOp(children);
      default:
        return Optional.empty();
    }
  }

  private static Optional<String> unaryOp(Expression[] children, String suffix) {
    if (children.length != 1) {
      return Optional.empty();
    }
    Optional<String> operand = compileOperand(children[0]);
    if (operand.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(operand.get() + " " + suffix);
  }

  private static Optional<String> binaryOp(Expression[] children, String op) {
    if (children.length != 2) {
      return Optional.empty();
    }
    Optional<String> left = compileOperand(children[0]);
    Optional<String> right = compileOperand(children[1]);
    if (left.isEmpty() || right.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(left.get() + " " + op + " " + right.get());
  }

  private static Optional<String> inOp(Expression[] children) {
    if (children.length < 2) {
      return Optional.empty();
    }
    Optional<String> column = compileOperand(children[0]);
    if (column.isEmpty()) {
      return Optional.empty();
    }
    List<String> values = new ArrayList<>(children.length - 1);
    for (int i = 1; i < children.length; i++) {
      Optional<String> value = compileOperand(children[i]);
      if (value.isEmpty()) {
        return Optional.empty();
      }
      values.add(value.get());
    }
    return Optional.of(column.get() + " IN (" + String.join(",", values) + ")");
  }

  /**
   * Renders {@code STARTS_WITH}/{@code ENDS_WITH}/{@code CONTAINS} as an anchored {@code LIKE} with
   * the needle's {@code %}/{@code _} escaped.
   */
  private static Optional<String> stringOp(
      Expression[] children, boolean trailingWildcard, boolean leadingWildcard) {
    if (children.length != 2) {
      return Optional.empty();
    }
    Optional<String> operand = compileOperand(children[0]);
    if (operand.isEmpty() || !(children[1] instanceof Literal)) {
      return Optional.empty();
    }
    String needle = stringValue((Literal<?>) children[1]);
    if (needle == null) {
      return Optional.empty();
    }
    StringBuilder pattern = new StringBuilder();
    if (leadingWildcard) {
      pattern.append('%');
    }
    pattern.append(escapeLike(needle));
    if (trailingWildcard) {
      pattern.append('%');
    }
    String escape = quoteSqlLiteral(String.valueOf(LIKE_ESCAPE));
    return Optional.of(
        operand.get() + " LIKE " + quoteSqlLiteral(pattern.toString()) + " ESCAPE " + escape);
  }

  /**
   * Renders a bare {@code LIKE} predicate. The pattern is used verbatim and the SQL default
   * backslash escape is declared explicitly. Patterns containing a backslash are rejected in {@link
   * #isLikePredicateShape} so this rendering never has to interpret Spark's escape sequences.
   */
  private static Optional<String> likeOp(Expression[] children) {
    if (children.length != 2) {
      return Optional.empty();
    }
    Optional<String> operand = compileOperand(children[0]);
    if (operand.isEmpty() || !(children[1] instanceof Literal)) {
      return Optional.empty();
    }
    String pattern = stringValue((Literal<?>) children[1]);
    if (pattern == null || pattern.indexOf('\\') >= 0) {
      return Optional.empty();
    }
    return Optional.of(operand.get() + " LIKE " + quoteSqlLiteral(pattern));
  }

  private static String escapeLike(String value) {
    StringBuilder escaped = new StringBuilder(value.length() + 4);
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == LIKE_ESCAPE || c == '%' || c == '_') {
        escaped.append(LIKE_ESCAPE);
      }
      escaped.append(c);
    }
    return escaped.toString();
  }

  private static String quoteSqlLiteral(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static Optional<String> compileOperand(Expression expr) {
    if (expr instanceof NamedReference) {
      return Optional.of(columnName((NamedReference) expr));
    }
    if (expr instanceof Literal) {
      return Optional.of(compileLiteral((Literal<?>) expr));
    }
    if (expr instanceof GeneralScalarExpression) {
      GeneralScalarExpression scalar = (GeneralScalarExpression) expr;
      List<String> children = new ArrayList<>(scalar.children().length);
      for (Expression child : scalar.children()) {
        Optional<String> compiled = compileOperand(child);
        if (compiled.isEmpty()) {
          return Optional.empty();
        }
        children.add(compiled.get());
      }
      switch (scalar.name()) {
        case "+":
        case "-":
        case "*":
        case "%":
          if (children.size() != 2) {
            return Optional.empty();
          }
          return Optional.of(
              "(" + children.get(0) + " " + scalar.name() + " " + children.get(1) + ")");
        case "UPPER":
          return Optional.of("upper(" + children.get(0) + ")");
        case "LOWER":
          return Optional.of("lower(" + children.get(0) + ")");
        case "CHAR_LENGTH":
          return Optional.of("char_length(" + children.get(0) + ")");
        default:
          return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private static String columnName(NamedReference reference) {
    return Arrays.stream(reference.fieldNames())
        .map(FilterPushDown::quoteIdentifierIfNeeded)
        .collect(Collectors.joining("."));
  }

  private static String quoteIdentifierIfNeeded(String identifier) {
    if (identifier.matches("[A-Za-z_][A-Za-z0-9_]*")) {
      return identifier;
    }
    return "`" + identifier.replace("`", "``") + "`";
  }

  private static String compileLiteral(Literal<?> literal) {
    Object value = literal.value();
    if (value == null) {
      return "NULL";
    }
    if (literal.dataType() == DataTypes.DateType && value instanceof Integer) {
      return "date '" + java.time.LocalDate.ofEpochDay((Integer) value) + "'";
    }
    if (literal.dataType() == DataTypes.TimestampType && value instanceof Long) {
      long micros = (Long) value;
      Instant instant =
          Instant.ofEpochSecond(
              Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
      // Spark TimestampType is micros-since-epoch UTC. Render the UTC wall clock explicitly:
      // java.sql.Timestamp.toString() renders in the JVM default zone, which would shift the
      // pushed literal on non-UTC drivers and silently drop or include the wrong rows.
      return "timestamp '" + UTC_TIMESTAMP_FORMATTER.format(instant.atOffset(ZoneOffset.UTC)) + "'";
    }
    if (value instanceof Date) {
      return "date '" + value.toString().replace("'", "''") + "'";
    }
    if (value instanceof Timestamp) {
      return "timestamp '" + value.toString().replace("'", "''") + "'";
    }
    if (value instanceof org.apache.spark.unsafe.types.UTF8String) {
      return "'" + value.toString().replace("'", "''") + "'";
    }
    if (value instanceof String) {
      return "'" + ((String) value).replace("'", "''") + "'";
    }
    if (value instanceof org.apache.spark.sql.types.Decimal) {
      return formatDecimalCast(((org.apache.spark.sql.types.Decimal) value).toJavaBigDecimal());
    }
    if (value instanceof BigDecimal) {
      return formatDecimalCast((BigDecimal) value);
    }
    return value.toString();
  }

  private static String formatDecimalCast(BigDecimal bd) {
    int scale = bd.scale();
    int precision = Math.max(bd.precision(), scale);
    return "CAST(" + bd.toPlainString() + " AS DECIMAL(" + precision + ", " + scale + "))";
  }
}
