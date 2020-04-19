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

public class SqlUpdateStatement extends SqlStatement {
  /** UPDATE-SELECT: UPDATE TABLE SET (Name,City,Country) = SELECT (N,C,Country) FROM TABLE#TMP
   */
  private SqlQueryStatement Query;
  private SqlCollection<SqlColumn> Columns;
  private SqlConditionNode condition;
  private SqlCollection<SqlTable> _fromClause = new SqlCollection<SqlTable>();
  private SqlOutputClause _outputClause;

  public SqlUpdateStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlOutputClause getOutputClause() {
    return this._outputClause;
  }

  public void setOutputClause(SqlOutputClause clause) {
    this._outputClause = clause;
  }

  public SqlUpdateStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    ParseTableName(tokenizer);
    SqlToken nextToken = tokenizer.LookaheadToken2();
    if (nextToken.equals("SET")) {
      tokenizer.NextToken();
      nextToken = tokenizer.LookaheadToken2();
    }
    if(nextToken.equals("(")){
      tokenizer.NextToken(); // consume '('
      Columns = ParserCore.parseColumns(tokenizer, this);
      tokenizer.EnsureNextToken(SqlToken.Close.Value);
      if (tokenizer.LookaheadToken2().equals("=")) {
        tokenizer.NextToken();
      }
      if (ParserCore.isQueryClause(tokenizer)) {
        Query = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
        if(Query.getColumns().size() != this.Columns.size()){
          throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SQL_COLCOUNT));
        }
        for(int i=0;i<this.Columns.size();i++){
          if(this.Columns.get(i).getColumnName().equals("*") || this.Query.getColumns().get(i).getColumnName().equals("*")){
            throw SqlExceptions.Exception("QueryException", SqlExceptions.UPDATE_SELECT_STAR);
          }
        }
      }
    } else {
      ParseSetClause(tokenizer);
      this._outputClause = ParserCore.readOutputClause(tokenizer, this);
      nextToken = tokenizer.LookaheadToken2();
      if (nextToken.equals("FROM")) {
        tokenizer.NextToken();
        SqlSelectStatement selectTemp = new SqlSelectStatement(null);
        ParserCore.parseTableReferences(dialectProcessor, selectTemp, tokenizer);
        this._fromClause = selectTemp.getTables();
      }
      condition = ParserCore.ParseWhere(tokenizer, this);
      ParserCore.parseComment(this, tokenizer);
    }
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
    visitor.visit(this.table);
    for (SqlColumn c : this.Columns) {
      visitor.visit(c);
    }
    visitor.visit(this.condition);
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

  public SqlQueryStatement getSelect(){
    return Query;
  }

  public SqlCollection<SqlTable> getFromClause() {
    return this._fromClause;
  }

  @Override
  public Object clone() {
    SqlUpdateStatement obj = new SqlUpdateStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlUpdateStatement o = (SqlUpdateStatement)obj;
    this.Query = o.Query == null ? null : (SqlQueryStatement)o.Query.clone();
    this.Columns = o.Columns == null ? null : (SqlCollection<SqlColumn>)o.Columns.clone();
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.clone();
    this._fromClause = (SqlCollection<SqlTable>)o._fromClause.clone();
    this._outputClause = o._outputClause == null ? null : (SqlOutputClause) o._outputClause.clone();
  }

  private void ParseTableName(SqlTokenizer tokenizer) throws Exception {
    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    if (Utilities.isNullOrEmpty(period[2])) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void ParseSetClause(SqlTokenizer tokenizer) throws Exception {
    Columns = new SqlCollection<SqlColumn>();
    while (true) {
      String name = tokenizer.NextToken().Value;
      tokenizer.EnsureNextToken("=");
      SqlExpression value = ParserCore.readExpression(tokenizer, this);
      SqlGeneralColumn column = new SqlGeneralColumn(name, value);
      Columns.add(column);
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
  }

}

