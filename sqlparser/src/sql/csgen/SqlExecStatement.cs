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


/**Syntax:
 *    EXECUTE my_proc @second = 2, @first = 1, @third = 3;
 *    EXEC my_proc @second = 2, @first = 1, @third = 3;
 *    EXECUTE my_proc second = @p1, first = @p2, third = @p3;
 */

public class SqlExecStatement : SqlStatement {
  private SqlCollection<SqlColumn> Columns;
  public SqlExecStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    ParseStoredProcedureName(tokenizer);
    ParseStoredProcedureParameters(tokenizer);
  }

  public SqlExecStatement(SqlTable table, SqlCollection<SqlColumn> columns, Dialect dialectProcessor): base(dialectProcessor) {
    this.Columns = columns;
    this.table = table;
    this.tableName = table.GetName();
  }

  private SqlExecStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public override SqlCollection<SqlColumn> GetColumns() {
    if (Columns != null) {
      return Columns;
    } else {
      return base.GetColumns();
    }
  }

  public override Object Clone() {
    SqlExecStatement obj = new SqlExecStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlExecStatement o = (SqlExecStatement)obj;
    this.Columns = o.Columns == null ? null : (SqlCollection<SqlColumn>)o.Columns.Clone();
  }

  private void ParseStoredProcedureName(SqlTokenizer tokenizer) {
    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    if (null == period[2] || 0 == period[2].Length) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_SP_NAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void ParseStoredProcedureParameters(SqlTokenizer tokenizer) {
    if(tokenizer.EOF()){
      return;
    } else if (tokenizer.LookaheadToken2().IsEmpty()){
      tokenizer.NextToken();
      return;
    }
    Columns = new SqlCollection<SqlColumn>();
    SqlToken tryNext;
    while (true){
      string name = tokenizer.NextToken().Value;
      tokenizer.EnsureNextToken("=");
      SqlGeneralColumn column = new SqlGeneralColumn(name, ParserCore.ReadExpression(tokenizer, this));
      this.Columns.Add(column);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
  }
}
}

