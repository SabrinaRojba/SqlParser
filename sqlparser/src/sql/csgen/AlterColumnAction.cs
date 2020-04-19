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


public sealed class AlterColumnAction : AlterTableAction {
  public AlterColumnAction(SqlCollection<SqlColumnDefinition> definitions) : base(SqlAlterOptions.ALTER_COLUMN, definitions, false, false, false) {
  }
}
}

