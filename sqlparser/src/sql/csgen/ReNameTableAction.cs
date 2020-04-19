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


public sealed class ReNameTableAction : AlterTableAction {
  private readonly SqlTable _tableTo;
  public ReNameTableAction(SqlTable tableTo) : base(SqlAlterOptions.RENAME_TABLE,
        new SqlCollection<SqlColumnDefinition>(),
        false,
        false,
        false) {
    this._tableTo = tableTo;
  }

  public SqlTable GetNameToTable() {
    return this._tableTo;
  }
}
}

