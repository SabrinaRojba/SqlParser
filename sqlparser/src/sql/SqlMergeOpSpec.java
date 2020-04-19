//@
package cdata.sql;
//@
/*#
namespace CData.Sql {
#*/

public abstract class SqlMergeOpSpec {
  SqlMergeOpType _mergeOpType;

  public SqlMergeOpSpec(SqlMergeOpType type) {
    this._mergeOpType = type;
  }

  public SqlMergeOpType getMergeOpType() {
    return this._mergeOpType;
  }
}
