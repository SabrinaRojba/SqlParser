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


public class SqlMemoryQueryStatement : SqlStatement {
  private string Query;

  public SqlMemoryQueryStatement(string table, string text, Dialect dialectProcessor) : base(dialectProcessor) {
    this.tableName = table;
    this.table = new SqlTable(tableName);
    this.Query = text;
  }

  public SqlMemoryQueryStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    tableName = tokenizer.NextIdentifier().Value;
    this.table = new SqlTable(tableName);
    this.Query = tokenizer.NextToken().Value;
    ParserCore.ParseComment(this, tokenizer);
  }

  private SqlMemoryQueryStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public string GetQuery() {
    return this.Query;
  }

  public override Object Clone() {
    SqlMemoryQueryStatement obj = new SqlMemoryQueryStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlMemoryQueryStatement o = (SqlMemoryQueryStatement)obj;
    this.Query = o.Query;
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }
}
}

