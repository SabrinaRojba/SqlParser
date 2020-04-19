//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class AlterColumnAction extends AlterTableAction {
  public AlterColumnAction(SqlCollection<SqlColumnDefinition> definitions) {
    super(SqlAlterOptions.ALTER_COLUMN, definitions, false, false, false);
  }
}
