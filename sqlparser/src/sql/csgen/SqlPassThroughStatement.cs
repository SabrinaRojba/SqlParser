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


public class SqlPassThroughStatement : SqlStatement{
  private string _sqlText;

  public SqlPassThroughStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlPassThroughStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    this._sqlText = tokenizer.GetInputText();
    while(!tokenizer.EOF()) {
      tokenizer.NextToken();
    }
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    
  }

  public override Object Clone() {
    SqlPassThroughStatement obj = new SqlPassThroughStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlPassThroughStatement o = (SqlPassThroughStatement) obj;
    this._sqlText = o._sqlText;
  }
}
}

