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


public class SqlMergeStatement : SqlStatement {
  private SqlTable _sourceTable;
  private SqlConditionNode _searchCondition;
  private SqlCollection<SqlMergeOpSpec> _mergeOpSpec = new SqlCollection<SqlMergeOpSpec>();

  public SqlMergeStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlMergeStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {

    if (tokenizer.EOF()) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EMPTY_STATEMENT, tokenizer.EOF()));
    }

    tokenizer.NextToken();
    tokenizer.EnsureNextToken("INTO");

    string[] periodTable = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    this.tableName = periodTable[2];
    this.table = new SqlTable(periodTable[0], periodTable[1], periodTable[2], ParserAlias(tokenizer, periodTable[2]));
    if (periodTable[2] == null || periodTable[2].Length < 0) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }

    tokenizer.EnsureNextToken("USING");
    string[] periodSrcTable = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    this._sourceTable = new SqlTable(periodSrcTable[0], periodSrcTable[1], periodSrcTable[2], ParserAlias(tokenizer, periodTable[2]));
    if (periodTable[2] == null || periodTable[2].Length < 0) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }

    tokenizer.EnsureNextToken("ON");
    this._searchCondition = ParseCondition(tokenizer, this);

    this.ParseMergeOperations(tokenizer);
  }

  public SqlTable GetSourceTable() {
    return this._sourceTable;
  }

  public SqlConditionNode GetSearchCondition() {
    return this._searchCondition;
  }

  public SqlCollection<SqlMergeOpSpec> GetMergeOpSpec() {
    return this._mergeOpSpec;
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    this._sourceTable.Accept(visitor);
    this._searchCondition.Accept(visitor);

    foreach(SqlMergeOpSpec mergeOp in this._mergeOpSpec) {
      SqlCollection<SqlColumn> mergeOpColumns = null;
      if (mergeOp is SqlMergeUpdateSpec) {
        mergeOpColumns = ((SqlMergeUpdateSpec) mergeOp).GetUpdateColumns();
      } else if (mergeOp is SqlMergeInsertSpec) {
        mergeOpColumns = ((SqlMergeInsertSpec) mergeOp).GetInsertColumns();
      }

      if (mergeOpColumns != null) {
        foreach(SqlColumn col in mergeOpColumns) {
          col.Accept(visitor);
        }
      }
    }
  }

  public override Object Clone() {
    SqlMergeStatement obj = new SqlMergeStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);

    SqlMergeStatement o = (SqlMergeStatement)obj;
    this._sourceTable = (SqlTable) o._sourceTable.Clone();
    this._searchCondition = (SqlConditionNode) o._searchCondition.Clone();

    foreach(SqlMergeOpSpec mergeOp in o._mergeOpSpec) {
      SqlMergeOpSpec newMergeOp = null;
      if (mergeOp.GetMergeOpType() == SqlMergeOpType.MERGE_UPDATE) {
        SqlCollection<SqlColumn> updateColumns = ((SqlMergeUpdateSpec)mergeOp).GetUpdateColumns();
        newMergeOp = new SqlMergeUpdateSpec((SqlCollection<SqlColumn>)updateColumns.Clone());
      } else if (mergeOp.GetMergeOpType() == SqlMergeOpType.MERGE_INSERT) {
        SqlCollection<SqlColumn> insertColumns = ((SqlMergeInsertSpec)mergeOp).GetInsertColumns();
        newMergeOp = new SqlMergeInsertSpec((SqlCollection<SqlColumn>)insertColumns.Clone());
      }
      this._mergeOpSpec.Add(newMergeOp);
    }
  }

  private void ParseMergeOperations(SqlTokenizer tokenizer) {
    while (!tokenizer.LookaheadToken2().IsEmpty()) {
      if (IsMatchedClause(tokenizer)) {
        tokenizer.NextToken(); //WHEN
        tokenizer.NextToken(); //MATCHED
        tokenizer.NextToken(); //THEN
        tokenizer.EnsureNextToken("UPDATE");
        tokenizer.EnsureNextToken("SET");
        SqlCollection<SqlColumn> updateColumns = ParseSetClause(tokenizer, this);
        this._mergeOpSpec.Add(new SqlMergeUpdateSpec(updateColumns));
      } else if (IsNotMatchedClause(tokenizer)) {
        tokenizer.NextToken(); //WHEN
        tokenizer.NextToken(); //NOT
        tokenizer.NextToken(); //MATCHED
        tokenizer.NextToken(); //THEN
        tokenizer.EnsureNextToken("INSERT");
        tokenizer.EnsureNextToken("(");
        SqlCollection<SqlColumn> insertColumns = ParserCore.ParseColumns(tokenizer, this);
        tokenizer.EnsureNextToken(")");
        tokenizer.EnsureNextToken("VALUES");
        tokenizer.EnsureNextToken("(");
        ParseValueList(tokenizer, insertColumns, this);
        tokenizer.EnsureNextToken(")");
        this._mergeOpSpec.Add(new SqlMergeInsertSpec(insertColumns));
      } else {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SYNTAX, tokenizer.CurrentPosition()));
      }
    }
  }

  private static string ParserAlias(SqlTokenizer tokenizer, string defAlias) {
    if (tokenizer.LookaheadToken2().Equals("AS")) {
      tokenizer.NextToken();
      return tokenizer.NextToken().Value;
    }

    return defAlias;
  }

  private static SqlConditionNode ParseCondition(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression cond = ParserCore.ReadExpression(tokenizer, stmt);
    if (cond is SqlValueExpression && cond.Evaluate().GetValueType() == SqlValueType.BOOLEAN){
      if (!cond.Evaluate().GetValueAsBool(true)) {
        cond = new SqlCriteria(null, ComparisonType.FALSE, null);
      }
    } else if (!(cond is SqlConditionNode)) {
      SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
      cond = new SqlCriteria(cond, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
    }

    return (SqlConditionNode)cond;
  }

  private static SqlCollection<SqlColumn> ParseSetClause(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlCollection<SqlColumn> columns = new SqlCollection<SqlColumn>();
    while (true) {
      SqlToken tryNext = tokenizer.LookaheadToken2();
      SqlExpression expr = ParserCore.ReadExpression(tokenizer, stmt);
      if (!(expr is SqlCriteria) || !(((SqlCriteria)expr).GetLeft() is SqlGeneralColumn)) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_COLNAME_FOUND_PARAM1, tryNext.Value));
      }
      if (((SqlCriteria)expr).GetOperator() != ComparisonType.EQUAL) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_EQUALS, ((SqlCriteria)expr).GetOperatorAsString()));
      }

      SqlGeneralColumn left = (SqlGeneralColumn)((SqlCriteria)expr).GetLeft();
      SqlExpression valExp = ((SqlCriteria)expr).GetRight();
      columns.Add(new SqlGeneralColumn(left.GetTable(), left.GetColumnName(), valExp));
      if (tokenizer.LookaheadToken2().Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }

    return columns;
  }

  private static void ParseValueList(SqlTokenizer tokenizer, SqlCollection<SqlColumn> columns, SqlStatement stmt) {
    int colIdx = 0;
    while (true) {
      if (colIdx >= columns.Size()) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.VALUE_CLAUSE_UNMATCHED));
      }

      SqlExpression valExp = ParserCore.ReadExpression(tokenizer, stmt);
      if (!(valExp is SqlExpression)) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_EXPECTED_VALUE));
      }

      if (columns.Size() > 0) {
        SqlGeneralColumn column = (SqlGeneralColumn) columns.Get(colIdx);
        column = new SqlGeneralColumn(column.GetTable(), column.GetColumnName(), valExp);
        columns.Set(colIdx, column);
      }

      colIdx++;
      if (tokenizer.LookaheadToken2().Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
  }

  private static bool IsMatchedClause(SqlTokenizer tokenizer) {
    bool retVal = false;
    if (tokenizer != null && !tokenizer.EOF()) {
      int originalPos = tokenizer.CurrentPosition();
      if (tokenizer.NextToken().Equals("WHEN") &&
          tokenizer.NextToken().Equals("MATCHED") &&
          tokenizer.NextToken().Equals("THEN")) {
        retVal = true;
      }
      tokenizer.Backtrack(originalPos);
    }

    return retVal;
  }

  private static bool IsNotMatchedClause(SqlTokenizer tokenizer) {
    bool retVal = false;
    if (tokenizer != null && !tokenizer.EOF()) {
      int originalPos = tokenizer.CurrentPosition();
      if (tokenizer.NextToken().Equals("WHEN") &&
          tokenizer.NextToken().Equals("NOT") &&
          tokenizer.NextToken().Equals("MATCHED") &&
          tokenizer.NextToken().Equals("THEN")) {
        retVal = true;
      }
      tokenizer.Backtrack(originalPos);
    }

    return retVal;
  }
}
}

