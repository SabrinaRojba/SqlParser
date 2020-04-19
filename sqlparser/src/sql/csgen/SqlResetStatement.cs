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


public class SqlResetStatement : SqlStatement{
  private bool ResetSchemaCache = false;
  private int MaxConnections = -1;

  public SqlResetStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    ParseResetSchemaCache(tokenizer);
  }

  private SqlResetStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public void ParseResetSchemaCache(SqlTokenizer tokenizer) {
    tokenizer.NextToken();
    SqlToken next = tokenizer.LookaheadToken2();
    if(next.Equals("SCHEMA")){
      tokenizer.NextToken(); // consume the current token
      tokenizer.EnsureNextIdentifier("CACHE");
      ResetSchemaCache = true;
    } else if(next.Equals("MAX")){
      tokenizer.NextToken();
      tokenizer.EnsureNextIdentifier("CONNECTIONS");
      try{
        next = tokenizer.NextToken(); 
        MaxConnections = int.Parse(next.Value);
      } catch (Exception e){
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SYNTAX_INT, next.Value));
      }
    } else {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SYNTAX, next.Value));
    }
  }

  public bool GetResetSchemaCache() {
    return ResetSchemaCache;
  }

  public int GetMaxConnections() {
    return MaxConnections;
  }

  public override Object Clone() {
    SqlResetStatement obj = new SqlResetStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlResetStatement o = (SqlResetStatement)obj;
    this.ResetSchemaCache = o.ResetSchemaCache;
    this.MaxConnections = o.MaxConnections;
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }
}
}

