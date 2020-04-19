//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlConstantColumn extends SqlColumn implements ISqlElement {
  private final SqlValueExpression expr;

  public SqlConstantColumn(SqlValueExpression val) {
    super(null, null);
    expr = val;
  }

  public SqlConstantColumn(String alias, SqlValueExpression val) {
    super(alias, alias);
    expr = val;
  }

  public /*#override#*/ SqlExpression getExpr() {
    return this.expr;
  }

  public /*#override#*/ Object clone() {
    SqlConstantColumn obj = new SqlConstantColumn(this.getAlias(), this.expr);
    obj.copy(this);
    return obj;
  }

  public /*#override#*/ boolean equals(String name) {
    return false;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return expr != null;
  }

  public /*#override#*/ SqlValue evaluate() {
    if (expr != null) {
      return expr.evaluate();
    }
    return SqlValue.getNullValueInstance();
  }

  public /*#override#*/ boolean hasAlias() {
    if (getAlias() != null) {
      return true;
    } else {
      return false;
    }
  }
}

