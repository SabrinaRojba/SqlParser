//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;

public class SqlMetaQueryStatement extends SqlStatement {
  private String Query;

  public SqlMetaQueryStatement(String text, Dialect dialectProcessor) {
    super(dialectProcessor);
    this.Query = text;
  }

  public SqlMetaQueryStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    this.Query = tokenizer.NextToken().Value;
    ParserCore.parseComment(this, tokenizer);
  }

  private SqlMetaQueryStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public String getQuery() {
    return this.Query;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  @Override
  public Object clone() {
    SqlMetaQueryStatement obj = new SqlMetaQueryStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlMetaQueryStatement o = (SqlMetaQueryStatement)obj;
    this.Query = o.Query;
  }
}
