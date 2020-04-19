//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.SqlExceptions;

public class SqlResetStatement extends SqlStatement{
  private boolean ResetSchemaCache = false;
  private int MaxConnections = -1;

  public SqlResetStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    super(dialectProcessor);
    parseResetSchemaCache(tokenizer);
  }

  private SqlResetStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public void parseResetSchemaCache(SqlTokenizer tokenizer) throws Exception{
    tokenizer.NextToken();
    SqlToken next = tokenizer.LookaheadToken2();
    if(next.equals("SCHEMA")){
      tokenizer.NextToken(); // consume the current token
      tokenizer.EnsureNextIdentifier("CACHE");
      ResetSchemaCache = true;
    } else if(next.equals("MAX")){
      tokenizer.NextToken();
      tokenizer.EnsureNextIdentifier("CONNECTIONS");
      try{
        next = tokenizer.NextToken(); 
        MaxConnections = Integer.parseInt(next.Value);
      } catch (Exception e){
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SYNTAX_INT, next.Value));
      }
    } else {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SYNTAX, next.Value));
    }
  }

  public boolean getResetSchemaCache() {
    return ResetSchemaCache;
  }

  public int getMaxConnections() {
    return MaxConnections;
  }

  @Override
  public Object clone() {
    SqlResetStatement obj = new SqlResetStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlResetStatement o = (SqlResetStatement)obj;
    this.ResetSchemaCache = o.ResetSchemaCache;
    this.MaxConnections = o.MaxConnections;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }
}
