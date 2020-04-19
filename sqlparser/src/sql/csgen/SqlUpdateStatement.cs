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


public class SqlUpdateStatement : SqlStatement {
  /** UPDATE-SELECT: UPDATE TABLE SET (Name,City,Country) = SELECT (N,C,Country) FROM TABLE#TMP
   */
  private SqlQueryStatement Query;
  private SqlCollection<SqlColumn> Columns;
  private SqlConditionNode condition;
  private SqlCollection<SqlTable> _fromClause = new SqlCollection<SqlTable>();
  private SqlOutputClause _outputClause;

  public SqlUpdateStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlOutputClause GetOutputClause() {
    return this._outputClause;
  }

  public void SetOutputClause(SqlOutputClause clause) {
    this._outputClause = clause;
  }

  public SqlUpdateStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    ParseTableName(tokenizer);
    SqlToken nextToken = tokenizer.LookaheadToken2();
    if (nextToken.Equals("SET")) {
      tokenizer.NextToken();
      nextToken = tokenizer.LookaheadToken2();
    }
    if(nextToken.Equals("(")){
      tokenizer.NextToken(); // consume '('
      Columns = ParserCore.ParseColumns(tokenizer, this);
      tokenizer.EnsureNextToken(SqlToken.Close.Value);
      if (tokenizer.LookaheadToken2().Equals("=")) {
        tokenizer.NextToken();
      }
      if (ParserCore.IsQueryClause(tokenizer)) {
        Query = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
        if(Query.GetColumns().Size() != this.Columns.Size()){
          throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SQL_COLCOUNT));
        }
        for(int i=0;i<this.Columns.Size();i++){
          if(this.Columns.Get(i).GetColumnName().Equals("*") || this.Query.GetColumns().Get(i).GetColumnName().Equals("*")){
            throw SqlExceptions.Exception("QueryException", SqlExceptions.UPDATE_SELECT_STAR);
          }
        }
      }
    } else {
      ParseSetClause(tokenizer);
      this._outputClause = ParserCore.ReadOutputClause(tokenizer, this);
      nextToken = tokenizer.LookaheadToken2();
      if (nextToken.Equals("FROM")) {
        tokenizer.NextToken();
        SqlSelectStatement selectTemp = new SqlSelectStatement(null);
        ParserCore.ParseTableReferences(dialectProcessor, selectTemp, tokenizer);
        this._fromClause = selectTemp.GetTables();
      }
      condition = ParserCore.ParseWhere(tokenizer, this);
      ParserCore.ParseComment(this, tokenizer);
    }
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
    visitor.Visit(this.table);
    foreach(SqlColumn c in this.Columns) {
      visitor.Visit(c);
    }
    visitor.Visit(this.condition);
  }

  public override SqlCollection<SqlColumn> GetColumns() {
    if (Columns != null) {
      return Columns;
    } else {
      return base.GetColumns();
    }
  }

  public override void SetColumns(SqlCollection<SqlColumn> columns) {
    this.Columns = columns;
  }

  public override SqlConditionNode GetCriteria() {
    return condition;
  }

  public override void SetCriteria(SqlConditionNode cond) {
    this.condition = cond;
  }

  public override void AddCondition(SqlConditionNode cond) {
    if (condition == null) {
      condition = cond;
    } else {
      condition = new SqlCondition(condition, SqlLogicalOperator.And, cond);
    }
  }

  public SqlQueryStatement GetSelect(){
    return Query;
  }

  public SqlCollection<SqlTable> GetFromClause() {
    return this._fromClause;
  }

  public override Object Clone() {
    SqlUpdateStatement obj = new SqlUpdateStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlUpdateStatement o = (SqlUpdateStatement)obj;
    this.Query = o.Query == null ? null : (SqlQueryStatement)o.Query.Clone();
    this.Columns = o.Columns == null ? null : (SqlCollection<SqlColumn>)o.Columns.Clone();
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.Clone();
    this._fromClause = (SqlCollection<SqlTable>)o._fromClause.Clone();
    this._outputClause = o._outputClause == null ? null : (SqlOutputClause) o._outputClause.Clone();
  }

  private void ParseTableName(SqlTokenizer tokenizer) {
    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    if (Utilities.IsNullOrEmpty(period[2])) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void ParseSetClause(SqlTokenizer tokenizer) {
    Columns = new SqlCollection<SqlColumn>();
    while (true) {
      string name = tokenizer.NextToken().Value;
      tokenizer.EnsureNextToken("=");
      SqlExpression value = ParserCore.ReadExpression(tokenizer, this);
      SqlGeneralColumn column = new SqlGeneralColumn(name, value);
      Columns.Add(column);
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
  }

}
}

