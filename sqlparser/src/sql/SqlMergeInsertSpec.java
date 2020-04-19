//@
package cdata.sql;
//@
/*#
namespace CData.Sql {
#*/

public class SqlMergeInsertSpec extends SqlMergeOpSpec {
  private SqlCollection<SqlColumn> _insertColumns;

  public SqlMergeInsertSpec(SqlCollection<SqlColumn> insertColumns) {
    super(SqlMergeOpType.MERGE_INSERT);
    this._insertColumns = insertColumns;
  }

  public SqlCollection<SqlColumn> getInsertColumns() {
    return this._insertColumns;
  }
}
