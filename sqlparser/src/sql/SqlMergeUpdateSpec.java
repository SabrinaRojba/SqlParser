//@
package cdata.sql;
//@
/*#
namespace CData.Sql {
#*/

public class SqlMergeUpdateSpec extends SqlMergeOpSpec {
  private SqlCollection<SqlColumn> _updateColumns;

  public SqlMergeUpdateSpec(SqlCollection<SqlColumn> updateColumns) {
    super(SqlMergeOpType.MERGE_UPDATE);
    this._updateColumns = updateColumns;
  }

  public SqlCollection<SqlColumn> getUpdateColumns() {
    return this._updateColumns;
  }
}
