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


public class SqlKillStatement : SqlStatement {
  private string _queryId;

  public SqlKillStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public string GetQueryId() {
    return _queryId;
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    ;
  }

  public SqlKillStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    tokenizer.EnsureNextToken("QUERY");
    this._queryId = tokenizer.NextToken().Value;
  }

  public override Object Clone() {
    SqlKillStatement obj = new SqlKillStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlKillStatement o = (SqlKillStatement)obj;
    this._queryId = o._queryId;
  }
}
}

