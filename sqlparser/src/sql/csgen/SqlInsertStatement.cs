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


public class SqlInsertStatement : SqlXsertStatement {
  public SqlInsertStatement(Dialect dialectProcessor) : base(dialectProcessor) {
    Verb = "INSERT";
  }

  public SqlInsertStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(tokenizer, dialectProcessor) {
    Verb = "INSERT";
  }

  public override Object Clone() {
    SqlInsertStatement obj = new SqlInsertStatement(null);
    obj.Copy(this);
    return obj;
  }
}
}

