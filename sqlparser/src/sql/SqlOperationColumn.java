//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlOperationColumn extends SqlColumn implements ISqlElement {
  private final SqlOperationExpression expr;

  public SqlOperationColumn(SqlOperationExpression val) {
    super(null, null);
    this.expr = val;
  }

  public SqlOperationColumn(String alias, SqlOperationExpression val) {
    super(alias, alias);
    this.expr = val;
  }

  public /*#override#*/ SqlExpression getExpr() {
    return this.expr;
  }

  public /*#override#*/ boolean equals(String name) {
    return false;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return expr != null && expr.isEvaluatable();
  }

  public /*#override#*/ SqlValue evaluate() {
    if (isEvaluatable()) {
      return expr.evaluate();
    }
    return SqlValue.getNullValueInstance();
  }

  public /*#override#*/ Object clone() {
    SqlOperationColumn obj = new SqlOperationColumn(this.getColumnName(), this.expr);
    obj.copy(this);
    return obj;
  }

  public /*#override#*/ boolean hasAlias() {
    if (getAlias() != null) {
      return true;
    } else {
      return false;
    }
  }
}

