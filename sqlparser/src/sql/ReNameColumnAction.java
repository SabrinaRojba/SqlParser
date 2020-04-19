//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class ReNameColumnAction extends AlterTableAction {
  private final SqlColumn _srcCol;
  private final SqlColumn _toCol;
  public ReNameColumnAction(SqlColumn col, SqlColumn columnTo) {
    super(SqlAlterOptions.RENAME_COLUMN,
        new SqlCollection<SqlColumnDefinition>(),
        false,
        false,
        false);
    this._srcCol = col;
    this._toCol = columnTo;
  }

  public SqlColumn getSrcColumn() {
    return this._srcCol;
  }

  public SqlColumn getNameToColumn() {
    return this._toCol;
  }
}
