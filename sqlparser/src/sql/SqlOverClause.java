//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public final class SqlOverClause {
  private SqlCollection<SqlOrderSpec> _orderByClause = new SqlCollection<SqlOrderSpec>();
  private SqlCollection<SqlExpression> _partitionClause = new SqlCollection<SqlExpression>();
  private SqlFrameClause _frameClause;

  public SqlOverClause(SqlExpression [] partition,
                       SqlOrderSpec [] orderBy,
                       SqlFrameClause winFrame) {
    for (SqlOrderSpec o : orderBy) {
      this._orderByClause.add(o);
    }
    for (SqlExpression p : partition) {
      this._partitionClause.add(p);
    }
    this._frameClause = winFrame;
  }

  public SqlCollection<SqlOrderSpec> getOrderClause() {
    return this._orderByClause;
  }

  public SqlCollection<SqlExpression> getPartitionClause() {
    return this._partitionClause;
  }

  public SqlFrameClause getFrameClause() {
    return this._frameClause;
  }
}
