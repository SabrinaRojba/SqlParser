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


public class SqlMetaQueryStatement : SqlStatement {
  private string Query;

  public SqlMetaQueryStatement(string text, Dialect dialectProcessor) : base(dialectProcessor) {
    this.Query = text;
  }

  public SqlMetaQueryStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    this.Query = tokenizer.NextToken().Value;
    ParserCore.ParseComment(this, tokenizer);
  }

  private SqlMetaQueryStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public string GetQuery() {
    return this.Query;
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public override Object Clone() {
    SqlMetaQueryStatement obj = new SqlMetaQueryStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlMetaQueryStatement o = (SqlMetaQueryStatement)obj;
    this.Query = o.Query;
  }
}
}

