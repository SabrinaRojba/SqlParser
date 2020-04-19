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


public sealed class AddColumnAction : AlterTableAction {

  public AddColumnAction(SqlCollection<SqlColumnDefinition> definitions,
                         bool hasColumnKeyword,
                         bool hasIfNotExistsForColumn) : base(SqlAlterOptions.ADD_COLUMN, definitions, hasIfNotExistsForColumn, hasColumnKeyword, false) {
  }
}
}

