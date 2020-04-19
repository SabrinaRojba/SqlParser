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


public class SqlLoadMemoryQueryStatement : SqlStatement {
  private string query;

  public SqlLoadMemoryQueryStatement(string text, Dialect dialectProcessor) : base(dialectProcessor) {
    this.query = text;
  }

  public SqlLoadMemoryQueryStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    query = tokenizer.NextToken().Value;
    ParserCore.ParseComment(this, tokenizer);
  }

  private SqlLoadMemoryQueryStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public string GetQuery() {
    return this.query;
  }

  public override Object Clone() {
    SqlLoadMemoryQueryStatement obj = new SqlLoadMemoryQueryStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlLoadMemoryQueryStatement o = (SqlLoadMemoryQueryStatement)obj;
    this.query = o.query;
  }
}
}

