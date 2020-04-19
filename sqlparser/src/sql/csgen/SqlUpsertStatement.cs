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


public class SqlUpsertStatement : SqlXsertStatement {
  public SqlUpsertStatement(Dialect dialectProcessor): base(dialectProcessor) {
    Verb = "UPSERT";
  }
  public SqlUpsertStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(tokenizer, dialectProcessor) {
    Verb = "UPSERT";
  }

  public override Object Clone() {
    SqlUpsertStatement obj = new SqlUpsertStatement(null);
    obj.Copy(this);
    return obj;
  }
}
}

