using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



using RSSBus.core;
using RSSBus;
namespace CData.Sql {

public sealed class SqlOverClause {
  private SqlCollection<SqlOrderSpec> _orderByClause = new SqlCollection<SqlOrderSpec>();
  private SqlCollection<SqlExpression> _partitionClause = new SqlCollection<SqlExpression>();
  private SqlFrameClause _frameClause;

  public SqlOverClause(SqlExpression [] partition,
                       SqlOrderSpec [] orderBy,
                       SqlFrameClause winFrame) {
    foreach(SqlOrderSpec o in orderBy) {
      this._orderByClause.Add(o);
    }
    foreach(SqlExpression p in partition) {
      this._partitionClause.Add(p);
    }
    this._frameClause = winFrame;
  }

  public SqlCollection<SqlOrderSpec> GetOrderClause() {
    return this._orderByClause;
  }

  public SqlCollection<SqlExpression> GetPartitionClause() {
    return this._partitionClause;
  }

  public SqlFrameClause GetFrameClause() {
    return this._frameClause;
  }
}
}

