//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class AddColumnAction extends AlterTableAction {

  public AddColumnAction(SqlCollection<SqlColumnDefinition> definitions,
                         boolean hasColumnKeyword,
                         boolean hasIfNotExistsForColumn) {
    super(SqlAlterOptions.ADD_COLUMN, definitions, hasIfNotExistsForColumn, hasColumnKeyword, false);
  }
}