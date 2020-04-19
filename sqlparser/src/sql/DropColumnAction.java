//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class DropColumnAction extends AlterTableAction {
  public DropColumnAction(SqlCollection<SqlColumnDefinition> definitions, boolean hasIfExistsForColumn) {
    super(SqlAlterOptions.DROP_COLUMN, definitions, false, true, hasIfExistsForColumn);
  }
}