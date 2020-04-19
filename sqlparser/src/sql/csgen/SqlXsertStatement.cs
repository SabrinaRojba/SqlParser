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


public class SqlXsertStatement : SqlStatement {
  protected string Verb = "INSERT";
  private SqlQueryStatement Query;
  private SqlCollection<SqlColumn> Columns;
  private bool UseDefaultValues = false;
  private SqlConditionNode condition;
  private SqlCollection<SqlExpression[]> _valuesRows = new SqlCollection<SqlExpression[]>();
  private SqlOutputClause _outputClause;

  public SqlXsertStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
    foreach(SqlColumn column in this.Columns) {
      column.Accept(visitor);
    }
    visitor.Visit(this.table);
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

  public SqlCollection<SqlExpression[]> GetValues() {
    return this._valuesRows;
  }

  public string GetVerb() {
    return this.Verb;
  }

  public SqlOutputClause GetOutputClause() {
    return this._outputClause;
  }

  public SqlXsertStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    ParseTableName(tokenizer);

    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals("DEFAULT")) {
      tokenizer.NextToken(); // consume look ahead
      tokenizer.EnsureNextIdentifier("VALUES");
      UseDefaultValues = true;
      return;
    } else if (next.Equals(SqlToken.Open)){
      tokenizer.EnsureNextToken(SqlToken.Open.Value);
      Columns = ParserCore.ParseColumns(tokenizer, this);
      tokenizer.EnsureNextToken(SqlToken.Close.Value);
    } else {
      this.Columns = new SqlCollection<SqlColumn>();
    }
    SqlToken lookAhead = tokenizer.LookaheadToken2();
    if (lookAhead.Equals("SELECT")) {
      Query = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
      if (this.Columns.Size() > 0 && Query.GetColumns().Size() != this.Columns.Size()) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SQL_COLCOUNT));
      }
      for (int i = 0; i < this.Columns.Size(); i++) {
        if (this.Columns.Get(i).GetColumnName().Equals("*") || this.Query.GetColumns().Get(i).GetColumnName().Equals("*")) {
          throw SqlExceptions.Exception("QueryException", SqlExceptions.INSERT_SELECT_STAR);
        }
      }
      return;
    }

    this._outputClause = ParserCore.ReadOutputClause(tokenizer, this);

    if (!tokenizer.NextToken().Equals("VALUES")) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_VALUES, tokenizer.LastToken()));
    }

    do {
      JavaArrayList<SqlExpression> values = new JavaArrayList<SqlExpression>();
      ParseValueList(tokenizer, values);
      this._valuesRows.Add(values.ToArray(typeof(SqlExpression)));
      SqlToken nt = tokenizer.LookaheadToken2();
      if (nt.Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    } while (true);

    ParserCore.ParseComment(this, tokenizer);
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

  public override Object Clone() {
    SqlXsertStatement obj = new SqlXsertStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlXsertStatement o = (SqlXsertStatement)obj;
    this.Verb = o.Verb;
    this.Query = o.Query == null ? null : (SqlQueryStatement)o.Query.Clone();
    this.Columns = o.Columns == null ? null : (SqlCollection<SqlColumn>)o.Columns.Clone();
    this._valuesRows = o._valuesRows == null ? null : (SqlCollection<SqlExpression[]>) o._valuesRows.Clone();
    this.UseDefaultValues = o.UseDefaultValues;
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.Clone();
    this._outputClause = o._outputClause == null ? null : (SqlOutputClause) o._outputClause.Clone();
  }

  private void ParseTableName(SqlTokenizer tokenizer) {
    tokenizer.EnsureNextToken("INTO");
    string[] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    if (Utilities.IsNullOrEmpty(period[2])) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void ParseValueList(SqlTokenizer tokenizer, JavaArrayList<SqlExpression> row) {
    if (!tokenizer.NextToken().Equals("(")) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_START_PAREN, tokenizer.LastToken()));
    }
    int columnCount = 0;
    while (true) {
      if (this.Columns.Size() > 0 && columnCount >= Columns.Size()) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.VALUE_CLAUSE_UNMATCHED));
      }

      SqlExpression value = ParserCore.ReadExpression(tokenizer, this);
      if (value == null) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_EXPECTED_VALUE));
      }
      if (value is SqlGeneralColumn) {
        value = new SqlValueExpression(SqlValueType.STRING, ((SqlGeneralColumn) value).GetColumnName());
      }

      row.Add(value);
      if (this.Columns.Size() > 0) {
        SqlGeneralColumn column = (SqlGeneralColumn) Columns.Get(columnCount);
        column = new SqlGeneralColumn(column.GetColumnName(), value);
        Columns.Set(columnCount, column);
      }

      columnCount++;
      if (tokenizer.LookaheadToken2().Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    if (Columns.Size() > 0 && columnCount < Columns.Size()) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.VALUE_CLAUSE_UNMATCHED));
    }
  }

  public SqlQueryStatement GetSelect() { // INSERT INTO dbf_name [(FieldName1 [, FieldName2, ...])] SELECT SELECTClauses..
    return Query;
  }

  public void SetSelect(SqlQueryStatement query) {
    this.Query = query;
  }

  public void SetOutputClause(SqlOutputClause clause) {
    this._outputClause = clause;
  }

  public bool GetUseDefaultValues() {
    return UseDefaultValues;
  }
}
}

