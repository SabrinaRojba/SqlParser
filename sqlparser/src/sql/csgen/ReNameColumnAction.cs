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


public sealed class ReNameColumnAction : AlterTableAction {
  private readonly SqlColumn _srcCol;
  private readonly SqlColumn _toCol;
  public ReNameColumnAction(SqlColumn col, SqlColumn columnTo) : base(SqlAlterOptions.RENAME_COLUMN,
        new SqlCollection<SqlColumnDefinition>(),
        false,
        false,
        false) {
    this._srcCol = col;
    this._toCol = columnTo;
  }

  public SqlColumn GetSrcColumn() {
    return this._srcCol;
  }

  public SqlColumn GetNameToColumn() {
    return this._toCol;
  }
}
}

