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


/**
 * Syntax:
 *         GETDELETED FROM TABLENAME WHERE PARAM= ? [...]
 */
public class SqlGetDeletedStatement : SqlStatement{
  private SqlConditionNode condition;

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public override  SqlConditionNode GetCriteria() {
    return condition;
  }

  public override  void SetCriteria(SqlConditionNode criteria) {
    condition = criteria;
  }
  
  public override void AddCondition(SqlConditionNode cond) {
    if (condition == null) {
      condition = cond;
    } else {
      condition = new SqlCondition(condition, SqlLogicalOperator.And, cond);
    }
  }

  public SqlGetDeletedStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    tokenizer.EnsureNextToken("FROM");
    ParseTableName(tokenizer);
    ParseQuery(tokenizer);
  }

  public SqlGetDeletedStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public override Object Clone() {
    SqlGetDeletedStatement obj = new SqlGetDeletedStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlGetDeletedStatement o = (SqlGetDeletedStatement)obj;
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.Clone();
  }

  private void ParseQuery(SqlTokenizer tokenizer) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals("WHERE")) {
      this.condition = ParserCore.ParseWhere(tokenizer, this);
    }
  }
  
  private void ParseTableName(SqlTokenizer tokenizer) {
    SqlToken nextToken = tokenizer.LookaheadToken2();
    if(nextToken.IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    if(nextToken.Equals("WHERE")){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_TABLENAME_BEFORE_WHERE));
    }

    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.GetDialectProcessor());
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }
}
}

