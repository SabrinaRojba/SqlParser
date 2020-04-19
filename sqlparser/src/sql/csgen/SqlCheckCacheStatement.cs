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
 *         CHECKCACHE [Table] [WITH REPAIR] START [Start Date] END [End Date]
 *         or
 *         CHECKCACHE [CacheTableName] AGAINST [Table] [WITH REPAIR] START [Start Date] END [End Date]
 *         or
 *         CHECKCACHE [CacheTableName] AGAINST [QUERY] [WITH REPAIR] START [Start Date] END [End Date]
 *
 */
public class SqlCheckCacheStatement : SqlStatement {
  private bool withRepair = false;
  private bool withAgainst = false;
  private bool skipDeleted = false;
  private RSBDateTime startDate;
  private RSBDateTime endDate;
  private SqlTable sourceTable = null;
  private SqlQueryStatement srcQueryStatement = null;

  public SqlCheckCacheStatement(Dialect dialectProcessor, SqlTable table, SqlQueryStatement queryStmt, SqlTable cacheTable, bool withRepair, bool skipDeleted, RSBDateTime startDate, RSBDateTime endDate) : base(dialectProcessor) {
    this.table = cacheTable;
    this.tableName = cacheTable.GetName();
    this.withRepair = withRepair;
    this.skipDeleted = skipDeleted;
    this.startDate = startDate;
    this.endDate = endDate;
    if (table != null) {
      if (!table.Equals(cacheTable)) {
        this.withAgainst = true;
        this.sourceTable = table;
      }
    } else if (queryStmt != null) {
      this.withAgainst = true;
      this.srcQueryStatement = queryStmt;
    }
  }

  public SqlCheckCacheStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    ParseTableName(tokenizer);
  }

  public bool IsWithRepair() {
    return withRepair;
  }

  public bool IsWithAgainst() {
    return withAgainst;
  }

  public bool IsSkipDeleted() {
    return skipDeleted;
  }

  public RSBDateTime GetStartDate() {
    return startDate;
  }

  public RSBDateTime GetEndDate() {
    return endDate;
  }

  public SqlTable GetSourceTable() {
    if (this.sourceTable == null) {
      return this.table;
    }

    return this.sourceTable;
  }

  public SqlQueryStatement GetSrcQueryStatement() {
    return srcQueryStatement;
  }

  public virtual string GetStatementName() {
    return "CHECKCACHE ";
  }

  private void ParseTableName(SqlTokenizer tokenizer) {
    tokenizer.NextToken();
    string[] period = ParserCore.ParsePeriodTableName(tokenizer);    
    SqlToken tk = tokenizer.NextToken();
    this.table = new SqlTable(period[0], period[1], period[2], period[2]);
    if(tk.Equals("AGAINST")) {
      this.withAgainst = true;
      tk = tokenizer.LookaheadToken2();
      if (tk.Equals("SELECT")) {
        srcQueryStatement = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
      } else {
        period = ParserCore.ParsePeriodTableName(tokenizer);
        this.sourceTable = new SqlTable(period[0], period[1], period[2], period[2]);
      }
      tk = tokenizer.NextToken();
    }

    if (tk.Equals("SKIP")) {
      tokenizer.EnsureNextIdentifier("DELETED");
      tk = tokenizer.NextToken();
      skipDeleted = true;
    }

    if (tk.Equals("WITH")) {
      tokenizer.EnsureNextIdentifier("REPAIR");
	    withRepair = true;
	    tk = tokenizer.NextToken();
    }

    if(!tk.Value.Equals("")) {
      if (!tk.Equals("START")) {
        throw tokenizer.MalformedSql("The CheckCache statement must have a start date.");
      }
      tk = tokenizer.NextToken();
      startDate = RSBDateTime.Parse(tk.Value);

      tk = tokenizer.NextToken();
      if (!tk.Equals("END")) {
        throw tokenizer.MalformedSql("The CheckCache statement must have an end date.");
      }
      tk = tokenizer.NextToken();
      endDate = RSBDateTime.Parse(tk.Value);
    }
  }

  public override Object Clone() {
    SqlCheckCacheStatement obj = new SqlCheckCacheStatement(null, this.sourceTable, this.GetSrcQueryStatement(), this.table, this.withRepair, this.skipDeleted, this.startDate, this.endDate);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    SqlCheckCacheStatement o = (SqlCheckCacheStatement)obj;
    this.srcQueryStatement = o.srcQueryStatement;
    base.Copy(obj);
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }
}
}

