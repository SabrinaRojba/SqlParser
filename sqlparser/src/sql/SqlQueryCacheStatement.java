//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

import core.ParserCore;

public class SqlQueryCacheStatement extends SqlStatement {
  private String query;

  public SqlQueryCacheStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    query = tokenizer.NextToken().Value;
    ParserCore.parseComment(this, tokenizer);
  }

  private SqlQueryCacheStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }


  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public String getQuery() {
    return this.query;
  }

  @Override
  public Object clone() {
    SqlQueryCacheStatement obj = new SqlQueryCacheStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlQueryCacheStatement o = (SqlQueryCacheStatement)obj;
    this.query = o.query;
  }
}

