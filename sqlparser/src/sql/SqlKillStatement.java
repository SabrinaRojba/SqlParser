//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.SqlExceptions;

public class SqlKillStatement extends SqlStatement {
  private String _queryId;

  public SqlKillStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public String getQueryId() {
    return _queryId;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    ;
  }

  public SqlKillStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    super(dialectProcessor);
    tokenizer.NextToken();
    tokenizer.EnsureNextToken("QUERY");
    this._queryId = tokenizer.NextToken().Value;
  }

  @Override
  public Object clone() {
    SqlKillStatement obj = new SqlKillStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlKillStatement o = (SqlKillStatement)obj;
    this._queryId = o._queryId;
  }
}

