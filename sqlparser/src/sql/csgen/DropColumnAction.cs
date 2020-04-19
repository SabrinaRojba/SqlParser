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


public sealed class DropColumnAction : AlterTableAction {
  public DropColumnAction(SqlCollection<SqlColumnDefinition> definitions, bool hasIfExistsForColumn) : base(SqlAlterOptions.DROP_COLUMN, definitions, false, true, hasIfExistsForColumn) {
  }
}
}

