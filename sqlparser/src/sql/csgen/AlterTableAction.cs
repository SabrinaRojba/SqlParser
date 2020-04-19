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


public abstract class AlterTableAction : AlterAction {
  protected readonly SqlAlterOptions _alterType;
  protected readonly bool _hasIfNotExistsForColumn;
  protected readonly bool _hasColumnKeyword;
  protected readonly bool _hasIfExistsForColumn;
  protected SqlCollection<SqlColumnDefinition> _definitions;
  protected AlterTableAction(SqlAlterOptions option,
                             SqlCollection<SqlColumnDefinition> definitions,
                             bool hasIfNotExistsForColumn,
                             bool hasColumnKeyword,
                             bool hasIfExistsForColumn) {
    this._alterType = option;
    this._definitions = definitions;
    this._hasIfNotExistsForColumn = hasIfNotExistsForColumn;
    this._hasColumnKeyword = hasColumnKeyword;
    this._hasIfExistsForColumn = hasIfExistsForColumn;
  }

  protected AlterTableAction(SqlAlterOptions option) : this(option, new SqlCollection<SqlColumnDefinition>(), false, false, false) {
  }

  public SqlCollection<SqlColumnDefinition> GetColumnDefinitions() {
    return this._definitions;
  }

  public bool HasIfNotExistsForColumn() {
    return this._hasIfNotExistsForColumn;
  }

  public bool HasIfExistsForColumn() {
    return this._hasIfExistsForColumn;
  }

  public bool HasColumnKeyword() {
    return this._hasColumnKeyword;
  }

  public SqlAlterOptions GetAlterOption() {
    return this._alterType;
  }

  public void SetColumnDefinitions(SqlCollection<SqlColumnDefinition> columns) {
    this._definitions = columns;
  }
}
}

