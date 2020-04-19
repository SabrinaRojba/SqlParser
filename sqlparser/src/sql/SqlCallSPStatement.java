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

public class SqlCallSPStatement extends SqlStatement {
  public SqlCallSPStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    parseStoredProcedureName(tokenizer);
    SqlToken lookahead = tokenizer.LookaheadToken2();
    if(lookahead.equals(SqlToken.Open.Value)) {
      tokenizer.NextToken();
      parseStoredProcedureParameters(tokenizer);
    }
  }

  private SqlCallSPStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  private void parseStoredProcedureName(SqlTokenizer tokenizer) throws Exception {
    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    if (null == period[2] || 0 == period[2].length()) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_SP_NAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void parseStoredProcedureParameters(SqlTokenizer tokenizer) throws Exception{
    while (true) {
      SqlExpression para = ParserCore.readExpression(tokenizer, this);
      if (!ParserCore.isParameterExpression(para)) {
        if (para instanceof SqlValueExpression) {
          this.parasList.add((SqlValueExpression)para);
        } else {
          throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_ANON_PARAM, tokenizer.LastToken().Value));
        }
      }
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if(tryNext.equals(",")){
        tokenizer.NextToken();
      } else {
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
  }

  public String getSPName() {
    return this.tableName;
  }

  @Override
  public Object clone() {
    SqlCallSPStatement obj = new SqlCallSPStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
  }
}

