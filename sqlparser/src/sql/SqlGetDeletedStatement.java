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

/**
 * Syntax:
 *         GETDELETED FROM TABLENAME WHERE PARAM= ? [...]
 */
public class SqlGetDeletedStatement extends SqlStatement{
  private SqlConditionNode condition;

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public /*#override#*/  SqlConditionNode getCriteria() {
    return condition;
  }

  public /*#override#*/  void setCriteria(SqlConditionNode criteria) {
    condition = criteria;
  }
  
  public /*#override #*/void addCondition(SqlConditionNode cond) {
    if (condition == null) {
      condition = cond;
    } else {
      condition = new SqlCondition(condition, SqlLogicalOperator.And, cond);
    }
  }

  public SqlGetDeletedStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    tokenizer.EnsureNextToken("FROM");
    parseTableName(tokenizer);
    parseQuery(tokenizer);
  }

  public SqlGetDeletedStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  @Override
  public Object clone() {
    SqlGetDeletedStatement obj = new SqlGetDeletedStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlGetDeletedStatement o = (SqlGetDeletedStatement)obj;
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.clone();
  }

  private void parseQuery(SqlTokenizer tokenizer) throws Exception {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals("WHERE")) {
      this.condition = ParserCore.ParseWhere(tokenizer, this);
    }
  }
  
  private void parseTableName(SqlTokenizer tokenizer) throws Exception {
    SqlToken nextToken = tokenizer.LookaheadToken2();
    if(nextToken.IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    if(nextToken.equals("WHERE")){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_TABLENAME_BEFORE_WHERE));
    }

    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.getDialectProcessor());
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }
}

