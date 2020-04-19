//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;
import core.SqlExceptions;
import rssbus.oputils.common.Utilities;

import java.util.ArrayList;

public class SqlXsertStatement extends SqlStatement {
  protected String Verb = "INSERT";
  private SqlQueryStatement Query;
  private SqlCollection<SqlColumn> Columns;
  private boolean UseDefaultValues = false;
  private SqlConditionNode condition;
  private SqlCollection<SqlExpression[]> _valuesRows = new SqlCollection<SqlExpression[]>();
  private SqlOutputClause _outputClause;

  public SqlXsertStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
    for (SqlColumn column : this.Columns) {
      column.accept(visitor);
    }
    visitor.visit(this.table);
  }

  public /*#override#*/ SqlCollection<SqlColumn> getColumns() {
    if (Columns != null) {
      return Columns;
    } else {
      return super.getColumns();
    }
  }

  public /*#override #*/void setColumns(SqlCollection<SqlColumn> columns) {
    this.Columns = columns;
  }

  public SqlCollection<SqlExpression[]> getValues() {
    return this._valuesRows;
  }

  public String getVerb() {
    return this.Verb;
  }

  public SqlOutputClause getOutputClause() {
    return this._outputClause;
  }

  public SqlXsertStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    ParseTableName(tokenizer);

    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals("DEFAULT")) {
      tokenizer.NextToken(); // consume look ahead
      tokenizer.EnsureNextIdentifier("VALUES");
      UseDefaultValues = true;
      return;
    } else if (next.equals(SqlToken.Open)){
      tokenizer.EnsureNextToken(SqlToken.Open.Value);
      Columns = ParserCore.parseColumns(tokenizer, this);
      tokenizer.EnsureNextToken(SqlToken.Close.Value);
    } else {
      this.Columns = new SqlCollection<SqlColumn>();
    }
    SqlToken lookAhead = tokenizer.LookaheadToken2();
    if (lookAhead.equals("SELECT")) {
      Query = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
      if (this.Columns.size() > 0 && Query.getColumns().size() != this.Columns.size()) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SQL_COLCOUNT));
      }
      for (int i = 0; i < this.Columns.size(); i++) {
        if (this.Columns.get(i).getColumnName().equals("*") || this.Query.getColumns().get(i).getColumnName().equals("*")) {
          throw SqlExceptions.Exception("QueryException", SqlExceptions.INSERT_SELECT_STAR);
        }
      }
      return;
    }

    this._outputClause = ParserCore.readOutputClause(tokenizer, this);

    if (!tokenizer.NextToken().equals("VALUES")) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_VALUES, tokenizer.LastToken()));
    }

    do {
      ArrayList<SqlExpression> values = new ArrayList<SqlExpression>();
      ParseValueList(tokenizer, values);
      this._valuesRows.add(values.toArray(/*@*/new SqlExpression[0]/*@*//*#typeof(SqlExpression)#*/));
      SqlToken nt = tokenizer.LookaheadToken2();
      if (nt.equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    } while (true);

    ParserCore.parseComment(this, tokenizer);
  }

  public /*#override#*/ SqlConditionNode getCriteria() {
    return condition;
  }

  public /*#override #*/void setCriteria(SqlConditionNode cond) {
    this.condition = cond;
  }

  public /*#override #*/void addCondition(SqlConditionNode cond) {
    if (condition == null) {
      condition = cond;
    } else {
      condition = new SqlCondition(condition, SqlLogicalOperator.And, cond);
    }
  }

  @Override
  public Object clone() {
    SqlXsertStatement obj = new SqlXsertStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlXsertStatement o = (SqlXsertStatement)obj;
    this.Verb = o.Verb;
    this.Query = o.Query == null ? null : (SqlQueryStatement)o.Query.clone();
    this.Columns = o.Columns == null ? null : (SqlCollection<SqlColumn>)o.Columns.clone();
    this._valuesRows = o._valuesRows == null ? null : (SqlCollection<SqlExpression[]>) o._valuesRows.clone();
    this.UseDefaultValues = o.UseDefaultValues;
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.clone();
    this._outputClause = o._outputClause == null ? null : (SqlOutputClause) o._outputClause.clone();
  }

  private void ParseTableName(SqlTokenizer tokenizer) throws Exception {
    tokenizer.EnsureNextToken("INTO");
    String[] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    if (Utilities.isNullOrEmpty(period[2])) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void ParseValueList(SqlTokenizer tokenizer, ArrayList<SqlExpression> row) throws Exception {
    if (!tokenizer.NextToken().equals("(")) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_START_PAREN, tokenizer.LastToken()));
    }
    int columnCount = 0;
    while (true) {
      if (this.Columns.size() > 0 && columnCount >= Columns.size()) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.VALUE_CLAUSE_UNMATCHED));
      }

      SqlExpression value = ParserCore.readExpression(tokenizer, this);
      if (value == null) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_EXPECTED_VALUE));
      }
      if (value instanceof SqlGeneralColumn) {
        value = new SqlValueExpression(SqlValueType.STRING, ((SqlGeneralColumn) value).getColumnName());
      }

      row.add(value);
      if (this.Columns.size() > 0) {
        SqlGeneralColumn column = (SqlGeneralColumn) Columns.get(columnCount);
        column = new SqlGeneralColumn(column.getColumnName(), value);
        Columns.set(columnCount, column);
      }

      columnCount++;
      if (tokenizer.LookaheadToken2().equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    if (Columns.size() > 0 && columnCount < Columns.size()) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.VALUE_CLAUSE_UNMATCHED));
    }
  }

  public SqlQueryStatement getSelect() { // INSERT INTO dbf_name [(FieldName1 [, FieldName2, ...])] SELECT SELECTClauses..
    return Query;
  }

  public void setSelect(SqlQueryStatement query) {
    this.Query = query;
  }

  public void setOutputClause(SqlOutputClause clause) {
    this._outputClause = clause;
  }

  public boolean getUseDefaultValues() {
    return UseDefaultValues;
  }
}

