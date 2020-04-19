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


public class SqlQueryCacheStatement : SqlStatement {
  private string query;

  public SqlQueryCacheStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    query = tokenizer.NextToken().Value;
    ParserCore.ParseComment(this, tokenizer);
  }

  private SqlQueryCacheStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }


  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public string GetQuery() {
    return this.query;
  }

  public override Object Clone() {
    SqlQueryCacheStatement obj = new SqlQueryCacheStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlQueryCacheStatement o = (SqlQueryCacheStatement)obj;
    this.query = o.query;
  }
}
}

