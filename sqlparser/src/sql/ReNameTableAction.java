//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class ReNameTableAction extends AlterTableAction {
  private final SqlTable _tableTo;
  public ReNameTableAction(SqlTable tableTo) {
    super(SqlAlterOptions.RENAME_TABLE,
        new SqlCollection<SqlColumnDefinition>(),
        false,
        false,
        false);
    this._tableTo = tableTo;
  }

  public SqlTable getNameToTable() {
    return this._tableTo;
  }
}
