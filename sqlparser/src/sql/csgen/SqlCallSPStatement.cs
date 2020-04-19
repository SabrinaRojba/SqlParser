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


public class SqlCallSPStatement : SqlStatement {
  public SqlCallSPStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    ParseStoredProcedureName(tokenizer);
    SqlToken lookahead = tokenizer.LookaheadToken2();
    if(lookahead.Equals(SqlToken.Open.Value)) {
      tokenizer.NextToken();
      ParseStoredProcedureParameters(tokenizer);
    }
  }

  private SqlCallSPStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  private void ParseStoredProcedureName(SqlTokenizer tokenizer) {
    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    if (null == period[2] || 0 == period[2].Length) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_SP_NAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void ParseStoredProcedureParameters(SqlTokenizer tokenizer) {
    while (true) {
      SqlExpression para = ParserCore.ReadExpression(tokenizer, this);
      if (!ParserCore.IsParameterExpression(para)) {
        if (para is SqlValueExpression) {
          this.parasList.Add((SqlValueExpression)para);
        } else {
          throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_ANON_PARAM, tokenizer.LastToken().Value));
        }
      }
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if(tryNext.Equals(",")){
        tokenizer.NextToken();
      } else {
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
  }

  public override void Accept(ISqlQueryVisitor visitor) {
  }

  public string GetSPName() {
    return this.tableName;
  }

  public override Object Clone() {
    SqlCallSPStatement obj = new SqlCallSPStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
  }
}
}

