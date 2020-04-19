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


public class SqlDeleteStatement : SqlStatement {

  private SqlQueryStatement query;
  private SqlConditionNode condition;
  private SqlOutputClause _outputClause;

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this.table);
  }

  public SqlOutputClause GetOutputClause() {
    return this._outputClause;
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

  public SqlDeleteStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlDeleteStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.EnsureNextIdentifier("DELETE");
    ParseTableName(tokenizer);
    int pos = tokenizer.CurrentPosition();
    if (tokenizer.NextToken().Equals("WHERE") && tokenizer.NextToken().Equals("EXISTS")) {
      this.query = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
    } else {
      tokenizer.Backtrack(pos);
      condition = ParserCore.ParseWhere(tokenizer, this);
      ParserCore.ParseComment(this, tokenizer);
    }
  }

  public SqlQueryStatement GetSelect() {
    return query;
  }

  public void SetSelect(SqlQueryStatement query) {
    this.query = query;
  }

  public void SetOutputClause(SqlOutputClause clause) {
    this._outputClause = clause;
  }

  public override Object Clone() {
    SqlDeleteStatement obj = new SqlDeleteStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlDeleteStatement o = (SqlDeleteStatement)obj;
    this.query = o.query == null ? null : (SqlQueryStatement)o.query.Clone();
    this.condition = o.condition == null ? null : (SqlConditionNode)o.condition.Clone();
    this._outputClause = o._outputClause == null ? null : (SqlOutputClause) o._outputClause.Clone();
  }

  private void ParseTableName(SqlTokenizer tokenizer) {
    SqlToken tN = tokenizer.LookaheadToken2();
    if (tN.Equals("FROM")) {
      tokenizer.EnsureNextToken("FROM");
    }
    this._outputClause = ParserCore.ReadOutputClause(tokenizer, this);
    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    if (Utilities.IsNullOrEmpty(period[2])) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }
}
}

