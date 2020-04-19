//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlExpressionList extends SqlExpression {
  private final SqlExpression[] list;
  public SqlExpressionList(SqlExpression[] list) {
    this.list = list;
  }

  public SqlExpression[] getExprList() {
    return list;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    return SqlValue.getNullValueInstance();
  }

  @Override
  public Object clone() {
    SqlExpressionList obj = new SqlExpressionList(list);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlExpression obj) {
  }
}
