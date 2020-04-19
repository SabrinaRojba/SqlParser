//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlSubQueryColumn extends SqlColumn implements ISqlElement {
  private final SqlSubQueryExpression expr;

  public SqlSubQueryColumn(SqlSubQueryExpression query) {
    super(null, null);
    expr = query;
  }

  public SqlSubQueryColumn(String alias, SqlSubQueryExpression query) {
    super(alias, alias);
    expr = query;
  }

  public /*#override#*/ SqlExpression getExpr() {
    return this.expr;
  }

  public /*#override#*/ Object clone() {
    SqlSubQueryColumn obj = new SqlSubQueryColumn(this.getAlias(), this.expr);
    obj.copy(this);
    return obj;
  }

  public /*#override#*/ boolean equals(String name) {
    return false;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
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

