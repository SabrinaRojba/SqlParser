//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.*;

public class SqlMergeStatement extends SqlStatement {
  private SqlTable _sourceTable;
  private SqlConditionNode _searchCondition;
  private SqlCollection<SqlMergeOpSpec> _mergeOpSpec = new SqlCollection<SqlMergeOpSpec>();

  public SqlMergeStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlMergeStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);

    if (tokenizer.EOF()) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EMPTY_STATEMENT, tokenizer.EOF()));
    }

    tokenizer.NextToken();
    tokenizer.EnsureNextToken("INTO");

    String[] periodTable = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    this.tableName = periodTable[2];
    this.table = new SqlTable(periodTable[0], periodTable[1], periodTable[2], parserAlias(tokenizer, periodTable[2]));
    if (periodTable[2] == null || periodTable[2].length() < 0) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }

    tokenizer.EnsureNextToken("USING");
    String[] periodSrcTable = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    this._sourceTable = new SqlTable(periodSrcTable[0], periodSrcTable[1], periodSrcTable[2], parserAlias(tokenizer, periodTable[2]));
    if (periodTable[2] == null || periodTable[2].length() < 0) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }

    tokenizer.EnsureNextToken("ON");
    this._searchCondition = parseCondition(tokenizer, this);

    this.parseMergeOperations(tokenizer);
  }

  public SqlTable getSourceTable() {
    return this._sourceTable;
  }

  public SqlConditionNode getSearchCondition() {
    return this._searchCondition;
  }

  public SqlCollection<SqlMergeOpSpec> getMergeOpSpec() {
    return this._mergeOpSpec;
  }

  @Override
  public void accept(ISqlQueryVisitor visitor) throws Exception {
    this._sourceTable.accept(visitor);
    this._searchCondition.accept(visitor);

    for (SqlMergeOpSpec mergeOp : this._mergeOpSpec) {
      SqlCollection<SqlColumn> mergeOpColumns = null;
      if (mergeOp instanceof SqlMergeUpdateSpec) {
        mergeOpColumns = ((SqlMergeUpdateSpec) mergeOp).getUpdateColumns();
      } else if (mergeOp instanceof SqlMergeInsertSpec) {
        mergeOpColumns = ((SqlMergeInsertSpec) mergeOp).getInsertColumns();
      }

      if (mergeOpColumns != null) {
        for (SqlColumn col : mergeOpColumns) {
          col.accept(visitor);
        }
      }
    }
  }

  @Override
  public Object clone() {
    SqlMergeStatement obj = new SqlMergeStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);

    SqlMergeStatement o = (SqlMergeStatement)obj;
    this._sourceTable = (SqlTable) o._sourceTable.clone();
    this._searchCondition = (SqlConditionNode) o._searchCondition.clone();

    for (SqlMergeOpSpec mergeOp : o._mergeOpSpec) {
      SqlMergeOpSpec newMergeOp = null;
      if (mergeOp.getMergeOpType() == SqlMergeOpType.MERGE_UPDATE) {
        SqlCollection<SqlColumn> updateColumns = ((SqlMergeUpdateSpec)mergeOp).getUpdateColumns();
        newMergeOp = new SqlMergeUpdateSpec((SqlCollection<SqlColumn>)updateColumns.clone());
      } else if (mergeOp.getMergeOpType() == SqlMergeOpType.MERGE_INSERT) {
        SqlCollection<SqlColumn> insertColumns = ((SqlMergeInsertSpec)mergeOp).getInsertColumns();
        newMergeOp = new SqlMergeInsertSpec((SqlCollection<SqlColumn>)insertColumns.clone());
      }
      this._mergeOpSpec.add(newMergeOp);
    }
  }

  private void parseMergeOperations(SqlTokenizer tokenizer) throws Exception {
    while (!tokenizer.LookaheadToken2().IsEmpty()) {
      if (isMatchedClause(tokenizer)) {
        tokenizer.NextToken(); //WHEN
        tokenizer.NextToken(); //MATCHED
        tokenizer.NextToken(); //THEN
        tokenizer.EnsureNextToken("UPDATE");
        tokenizer.EnsureNextToken("SET");
        SqlCollection<SqlColumn> updateColumns = parseSetClause(tokenizer, this);
        this._mergeOpSpec.add(new SqlMergeUpdateSpec(updateColumns));
      } else if (isNotMatchedClause(tokenizer)) {
        tokenizer.NextToken(); //WHEN
        tokenizer.NextToken(); //NOT
        tokenizer.NextToken(); //MATCHED
        tokenizer.NextToken(); //THEN
        tokenizer.EnsureNextToken("INSERT");
        tokenizer.EnsureNextToken("(");
        SqlCollection<SqlColumn> insertColumns = ParserCore.parseColumns(tokenizer, this);
        tokenizer.EnsureNextToken(")");
        tokenizer.EnsureNextToken("VALUES");
        tokenizer.EnsureNextToken("(");
        parseValueList(tokenizer, insertColumns, this);
        tokenizer.EnsureNextToken(")");
        this._mergeOpSpec.add(new SqlMergeInsertSpec(insertColumns));
      } else {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SYNTAX, tokenizer.currentPosition()));
      }
    }
  }

  private static String parserAlias(SqlTokenizer tokenizer, String defAlias) throws Exception {
    if (tokenizer.LookaheadToken2().equals("AS")) {
      tokenizer.NextToken();
      return tokenizer.NextToken().Value;
    }

    return defAlias;
  }

  private static SqlConditionNode parseCondition(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlExpression cond = ParserCore.readExpression(tokenizer, stmt);
    if (cond instanceof SqlValueExpression && cond.evaluate().getValueType() == SqlValueType.BOOLEAN){
      if (!cond.evaluate().getValueAsBool(true)) {
        cond = new SqlCriteria(null, ComparisonType.FALSE, null);
      }
    } else if (!(cond instanceof SqlConditionNode)) {
      SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
      cond = new SqlCriteria(cond, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
    }

    return (SqlConditionNode)cond;
  }

  private static SqlCollection<SqlColumn> parseSetClause(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlCollection<SqlColumn> columns = new SqlCollection<SqlColumn>();
    while (true) {
      SqlToken tryNext = tokenizer.LookaheadToken2();
      SqlExpression expr = ParserCore.readExpression(tokenizer, stmt);
      if (!(expr instanceof SqlCriteria) || !(((SqlCriteria)expr).getLeft() instanceof SqlGeneralColumn)) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_COLNAME_FOUND_PARAM1, tryNext.Value));
      }
      if (((SqlCriteria)expr).getOperator() != ComparisonType.EQUAL) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_EQUALS, ((SqlCriteria)expr).getOperatorAsString()));
      }

      SqlGeneralColumn left = (SqlGeneralColumn)((SqlCriteria)expr).getLeft();
      SqlExpression valExp = ((SqlCriteria)expr).getRight();
      columns.add(new SqlGeneralColumn(left.getTable(), left.getColumnName(), valExp));
      if (tokenizer.LookaheadToken2().equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }

    return columns;
  }

  private static void parseValueList(SqlTokenizer tokenizer, SqlCollection<SqlColumn> columns, SqlStatement stmt) throws Exception {
    int colIdx = 0;
    while (true) {
      if (colIdx >= columns.size()) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.VALUE_CLAUSE_UNMATCHED));
      }

      SqlExpression valExp = ParserCore.readExpression(tokenizer, stmt);
      if (!(valExp instanceof SqlExpression)) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_EXPECTED_VALUE));
      }

      if (columns.size() > 0) {
        SqlGeneralColumn column = (SqlGeneralColumn) columns.get(colIdx);
        column = new SqlGeneralColumn(column.getTable(), column.getColumnName(), valExp);
        columns.set(colIdx, column);
      }

      colIdx++;
      if (tokenizer.LookaheadToken2().equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
  }

  private static boolean isMatchedClause(SqlTokenizer tokenizer) throws Exception {
    boolean retVal = false;
    if (tokenizer != null && !tokenizer.EOF()) {
      int originalPos = tokenizer.currentPosition();
      if (tokenizer.NextToken().equals("WHEN") &&
          tokenizer.NextToken().equals("MATCHED") &&
          tokenizer.NextToken().equals("THEN")) {
        retVal = true;
      }
      tokenizer.Backtrack(originalPos);
    }

    return retVal;
  }

  private static boolean isNotMatchedClause(SqlTokenizer tokenizer) throws Exception {
    boolean retVal = false;
    if (tokenizer != null && !tokenizer.EOF()) {
      int originalPos = tokenizer.currentPosition();
      if (tokenizer.NextToken().equals("WHEN") &&
          tokenizer.NextToken().equals("NOT") &&
          tokenizer.NextToken().equals("MATCHED") &&
          tokenizer.NextToken().equals("THEN")) {
        retVal = true;
      }
      tokenizer.Backtrack(originalPos);
    }

    return retVal;
  }
}
