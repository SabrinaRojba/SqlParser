//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlSubQueryExpression extends SqlExpression{
  private final SqlQueryStatement query;

  public SqlSubQueryExpression(SqlQueryStatement subQuery) {
    this.query = subQuery;
  }

  public SqlQueryStatement getQuery() {
    return this.query;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    //Do nothing now.
    return SqlValue.getNullValueInstance();
  }

  @Override
  public Object clone() {
    return new SqlSubQueryExpression((SqlQueryStatement)query.clone());
  }

  @Override
  protected void copy(SqlExpression obj) {
  }
}
