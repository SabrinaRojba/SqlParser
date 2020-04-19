//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;

public class SqlMemoryQueryStatement extends SqlStatement {
  private String Query;

  public SqlMemoryQueryStatement(String table, String text, Dialect dialectProcessor) {
    super(dialectProcessor);
    this.tableName = table;
    this.table = new SqlTable(tableName);
    this.Query = text;
  }

  public SqlMemoryQueryStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    tableName = tokenizer.NextIdentifier().Value;
    this.table = new SqlTable(tableName);
    this.Query = tokenizer.NextToken().Value;
    ParserCore.parseComment(this, tokenizer);
  }

  private SqlMemoryQueryStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public String getQuery() {
    return this.Query;
  }

  @Override
  public Object clone() {
    SqlMemoryQueryStatement obj = new SqlMemoryQueryStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlMemoryQueryStatement o = (SqlMemoryQueryStatement)obj;
    this.Query = o.Query;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }
}
