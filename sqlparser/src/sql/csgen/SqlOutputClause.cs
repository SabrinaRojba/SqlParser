using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



using RSSBus.core;
using RSSBus;
namespace CData.Sql {


public class SqlOutputClause : ISqlCloneable {
  //https://docs.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql?view=sql-server-2017
  private readonly SqlCollection<SqlColumn> dml_select_list;
  private SqlExpression intoTarget;

  public SqlOutputClause(SqlCollection<SqlColumn> dml_select_list) : this(dml_select_list, null) {
  }

  public SqlOutputClause(SqlCollection<SqlColumn> dml_select_list, SqlExpression intoTarget) {
    this.dml_select_list = dml_select_list;
    this.intoTarget = intoTarget;
  }

  public SqlCollection<SqlColumn> GetDmlSelectList() {
    return this.dml_select_list;
  }

  public SqlExpression GetIntoTarget() {
    return this.intoTarget;
  }

  public Object Clone() {
    SqlOutputClause o = new SqlOutputClause((SqlCollection<SqlColumn>) this.dml_select_list.Clone());
    if (this.intoTarget != null) {
      o.intoTarget = (SqlExpression) this.intoTarget.Clone();
    }
    return o;
  }
}
}

