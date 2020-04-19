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


public class SqlDropStatement : SqlStatement {
  private bool DropIfExist = false;
  public SqlDropStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public bool GetDropIfExist() {
    return DropIfExist;
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public SqlDropStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    tokenizer.EnsureNextIdentifier("TABLE");
    ParseTableName(tokenizer);
  }

  public override Object Clone() {
    SqlDropStatement obj = new SqlDropStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlDropStatement o = (SqlDropStatement)obj;
    this.DropIfExist = o.DropIfExist;
  }

  private void ParseTableName(SqlTokenizer tokenizer) {
    SqlToken tk = tokenizer.LookaheadToken2();
    if(tk.Equals("IF")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("EXISTS");
      this.DropIfExist = true;
      tk = tokenizer.LookaheadToken2();
    }
    if (tk.IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    string[] period = ParserCore.ParsePeriodTableName(tokenizer, this.GetDialectProcessor());
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }
}
}

