//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public abstract class AlterTableAction extends AlterAction {
  protected final SqlAlterOptions _alterType;
  protected final boolean _hasIfNotExistsForColumn;
  protected final boolean _hasColumnKeyword;
  protected final boolean _hasIfExistsForColumn;
  protected SqlCollection<SqlColumnDefinition> _definitions;
  protected AlterTableAction(SqlAlterOptions option,
                             SqlCollection<SqlColumnDefinition> definitions,
                             boolean hasIfNotExistsForColumn,
                             boolean hasColumnKeyword,
                             boolean hasIfExistsForColumn) {
    this._alterType = option;
    this._definitions = definitions;
    this._hasIfNotExistsForColumn = hasIfNotExistsForColumn;
    this._hasColumnKeyword = hasColumnKeyword;
    this._hasIfExistsForColumn = hasIfExistsForColumn;
  }

  protected AlterTableAction(SqlAlterOptions option) {
    this(option, new SqlCollection<SqlColumnDefinition>(), false, false, false);
  }

  public SqlCollection<SqlColumnDefinition> getColumnDefinitions() {
    return this._definitions;
  }

  public boolean hasIfNotExistsForColumn() {
    return this._hasIfNotExistsForColumn;
  }

  public boolean hasIfExistsForColumn() {
    return this._hasIfExistsForColumn;
  }

  public boolean hasColumnKeyword() {
    return this._hasColumnKeyword;
  }

  public SqlAlterOptions getAlterOption() {
    return this._alterType;
  }

  public void setColumnDefinitions(SqlCollection<SqlColumnDefinition> columns) {
    this._definitions = columns;
  }
}

