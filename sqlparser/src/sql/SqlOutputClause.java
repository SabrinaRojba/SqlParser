//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlOutputClause implements ISqlCloneable {
  //https://docs.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-2017
  private final SqlCollection<SqlColumn> dml_select_list;
  private SqlExpression intoTarget;

  public SqlOutputClause(SqlCollection<SqlColumn> dml_select_list) {
    this(dml_select_list, null);
  }

  public SqlOutputClause(SqlCollection<SqlColumn> dml_select_list, SqlExpression intoTarget) {
    this.dml_select_list = dml_select_list;
    this.intoTarget = intoTarget;
  }

  public SqlCollection<SqlColumn> getDmlSelectList() {
    return this.dml_select_list;
  }

  public SqlExpression getIntoTarget() {
    return this.intoTarget;
  }

  public Object clone() {
    SqlOutputClause o = new SqlOutputClause((SqlCollection<SqlColumn>) this.dml_select_list.clone());
    if (this.intoTarget != null) {
      o.intoTarget = (SqlExpression) this.intoTarget.clone();
    }
    return o;
  }
}
