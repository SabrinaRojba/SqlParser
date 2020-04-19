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


public class SqlCreateTableStatement : SqlStatement {
  private SqlCollection<SqlColumnDefinition> columnDefinitions = new SqlCollection<SqlColumnDefinition>();
  private bool createIfNotExists = false;
  public bool GetCreateIfNotExists() {
    return createIfNotExists;
  }

  public SqlCollection<SqlColumnDefinition> GetColumnDefinitions() {
    return columnDefinitions;
  }

  public void SetColumnDefinition(SqlCollection<SqlColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public SqlCreateTableStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlCreateTableStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    tokenizer.EnsureNextIdentifier("TABLE");
    ParseTableName(tokenizer);
    ParserCore.ParseColumnDefinitions(tokenizer, this.columnDefinitions, this.dialectProcessor);
  }
  
  private void ParseTableName(SqlTokenizer tokenizer) {
    SqlToken tk = tokenizer.LookaheadToken2();
    if(tk.Equals("IF")){
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("NOT");
      tokenizer.EnsureNextToken("EXISTS");
      this.createIfNotExists = true;
      tk = tokenizer.LookaheadToken2();
    }
    if (tk.IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    string[] period = ParserCore.ParsePeriodTableName(tokenizer, this.GetDialectProcessor());
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  public override Object Clone() {
    SqlCreateTableStatement obj = new SqlCreateTableStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlCreateTableStatement o = (SqlCreateTableStatement)obj;
    this.columnDefinitions = o.columnDefinitions == null ? null : (SqlCollection<SqlColumnDefinition>)o.columnDefinitions.Clone();
    this.createIfNotExists = o.createIfNotExists;
  }
}
}

