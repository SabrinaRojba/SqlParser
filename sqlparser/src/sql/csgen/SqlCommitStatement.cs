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


public class SqlCommitStatement : SqlStatement {
  public SqlCommitStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public SqlCommitStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.EnsureNextIdentifier("COMMIT");
    SqlToken next = tokenizer.NextToken();
    if(!next.IsEmpty()){
      throw SqlExceptions.Exception("QueryException", SqlExceptions.UNEXPECTED_TOKEN, next.Value);
    }
  }

  public override Object Clone() {
    SqlCommitStatement obj = new SqlCommitStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
  }
}
}

