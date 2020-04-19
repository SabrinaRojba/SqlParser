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

public class SqlDeleteStatement extends SqlStatement {

  private SqlQueryStatement query;
  private SqlConditionNode condition;
  private SqlOutputClause _outputClause;

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this.table);
  }

  public SqlOutputClause getOutputClause() {
    return this._outputClause;
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

  public SqlDeleteStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlDeleteStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.EnsureNextIdentifier("DELETE");
    ParseTableName(tokenizer);
    int pos = tokenizer.currentPosition();
    if (tokenizer.NextToken().equals("WHERE") && tokenizer.NextToken().equals("EXISTS")) {
      this.query = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
    } else {
      tokenizer.Backtrack(pos);
      condition = ParserCore.ParseWhere(tokenizer, this);
      ParserCore.parseComment(this, tokenizer);
    }
  }

  public SqlQueryStatement getSelect() {
    return query;
  }

  public void setSelect(SqlQueryStatement query) {
    this.query = query;
  }

  public void setOutputClause(SqlOutputClause clause) {
    this._outputClause = clause;
  }

  @Override
  public Object clone() {
    SqlDeleteStatement obj = new SqlDeleteStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlDeleteStatement o = (SqlDeleteStatement)obj;
    this.query = o.query == null ? null : (SqlQueryStatement)o.query.clone();
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.clone();
    this._outputClause = o._outputClause == null ? null : (SqlOutputClause) o._outputClause.clone();
  }

  private void ParseTableName(SqlTokenizer tokenizer) throws Exception {
    SqlToken tN = tokenizer.LookaheadToken2();
    if (tN.equals("FROM")) {
      tokenizer.EnsureNextToken("FROM");
    }
    this._outputClause = ParserCore.readOutputClause(tokenizer, this);
    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    if (Utilities.isNullOrEmpty(period[2])) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }
}

