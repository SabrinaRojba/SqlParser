using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



namespace CData.Sql {


public class SqlMergeUpdateSpec : SqlMergeOpSpec {
  private SqlCollection<SqlColumn> _updateColumns;

  public SqlMergeUpdateSpec(SqlCollection<SqlColumn> updateColumns) : base(SqlMergeOpType.MERGE_UPDATE) {
    this._updateColumns = updateColumns;
  }

  public SqlCollection<SqlColumn> GetUpdateColumns() {
    return this._updateColumns;
  }
}
}

