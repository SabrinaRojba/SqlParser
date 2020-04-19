//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

import core.ParserCore;

public class SqlLoadMemoryQueryStatement extends SqlStatement {
  private String query;

  public SqlLoadMemoryQueryStatement(String text, Dialect dialectProcessor) {
    super(dialectProcessor);
    this.query = text;
  }

  public SqlLoadMemoryQueryStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    query = tokenizer.NextToken().Value;
    ParserCore.parseComment(this, tokenizer);
  }

  private SqlLoadMemoryQueryStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public String getQuery() {
    return this.query;
  }

  @Override
  public Object clone() {
    SqlLoadMemoryQueryStatement obj = new SqlLoadMemoryQueryStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlLoadMemoryQueryStatement o = (SqlLoadMemoryQueryStatement)obj;
    this.query = o.query;
  }
}

