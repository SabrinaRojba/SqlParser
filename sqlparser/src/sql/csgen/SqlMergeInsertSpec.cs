using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



namespace CData.Sql {


public class SqlMergeInsertSpec : SqlMergeOpSpec {
  private SqlCollection<SqlColumn> _insertColumns;

  public SqlMergeInsertSpec(SqlCollection<SqlColumn> insertColumns) : base(SqlMergeOpType.MERGE_INSERT) {
    this._insertColumns = insertColumns;
  }

  public SqlCollection<SqlColumn> GetInsertColumns() {
    return this._insertColumns;
  }
}
}

