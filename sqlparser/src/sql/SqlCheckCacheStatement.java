//@
package cdata.sql;
import core.ParserCore;
//@
/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

import rssbus.oputils.common.RSBDateTime;

/**
 * Syntax:
 *         CHECKCACHE [Table] [WITH REPAIR] START [Start Date] END [End Date]
 *         or
 *         CHECKCACHE [CacheTableName] AGAINST [Table] [WITH REPAIR] START [Start Date] END [End Date]
 *         or
 *         CHECKCACHE [CacheTableName] AGAINST [QUERY] [WITH REPAIR] START [Start Date] END [End Date]
 *
 */
public class SqlCheckCacheStatement extends SqlStatement {
  private boolean withRepair = false;
  private boolean withAgainst = false;
  private boolean skipDeleted = false;
  private RSBDateTime startDate;
  private RSBDateTime endDate;
  private SqlTable sourceTable = null;
  private SqlQueryStatement srcQueryStatement = null;

  public SqlCheckCacheStatement(Dialect dialectProcessor, SqlTable table, SqlQueryStatement queryStmt, SqlTable cacheTable, boolean withRepair, boolean skipDeleted, RSBDateTime startDate, RSBDateTime endDate) {
    super(dialectProcessor);
    this.table = cacheTable;
    this.tableName = cacheTable.getName();
    this.withRepair = withRepair;
    this.skipDeleted = skipDeleted;
    this.startDate = startDate;
    this.endDate = endDate;
    if (table != null) {
      if (!table.equals(cacheTable)) {
        this.withAgainst = true;
        this.sourceTable = table;
      }
    } else if (queryStmt != null) {
      this.withAgainst = true;
      this.srcQueryStatement = queryStmt;
    }
  }

  public SqlCheckCacheStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    parseTableName(tokenizer);
  }

  public boolean isWithRepair() {
    return withRepair;
  }

  public boolean isWithAgainst() {
    return withAgainst;
  }

  public boolean isSkipDeleted() {
    return skipDeleted;
  }

  public RSBDateTime getStartDate() {
    return startDate;
  }

  public RSBDateTime getEndDate() {
    return endDate;
  }

  public SqlTable getSourceTable() {
    if (this.sourceTable == null) {
      return this.table;
    }

    return this.sourceTable;
  }

  public SqlQueryStatement getSrcQueryStatement() {
    return srcQueryStatement;
  }

  public /*#virtual#*/ String getStatementName() {
    return "CHECKCACHE ";
  }

  private void parseTableName(SqlTokenizer tokenizer) throws Exception {
    tokenizer.NextToken();
    String[] period = ParserCore.parsePeriodTableName(tokenizer);    
    SqlToken tk = tokenizer.NextToken();
    this.table = new SqlTable(period[0], period[1], period[2], period[2]);
    if(tk.equals("AGAINST")) {
      this.withAgainst = true;
      tk = tokenizer.LookaheadToken2();
      if (tk.equals("SELECT")) {
        srcQueryStatement = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
      } else {
        period = ParserCore.parsePeriodTableName(tokenizer);
        this.sourceTable = new SqlTable(period[0], period[1], period[2], period[2]);
      }
      tk = tokenizer.NextToken();
    }

    if (tk.equals("SKIP")) {
      tokenizer.EnsureNextIdentifier("DELETED");
      tk = tokenizer.NextToken();
      skipDeleted = true;
    }

    if (tk.equals("WITH")) {
      tokenizer.EnsureNextIdentifier("REPAIR");
	    withRepair = true;
	    tk = tokenizer.NextToken();
    }

    if(!tk.Value.equals("")) {
      if (!tk.equals("START")) {
        throw tokenizer.MalformedSql("The CheckCache statement must have a start date.");
      }
      tk = tokenizer.NextToken();
      startDate = RSBDateTime.parse(tk.Value);

      tk = tokenizer.NextToken();
      if (!tk.equals("END")) {
        throw tokenizer.MalformedSql("The CheckCache statement must have an end date.");
      }
      tk = tokenizer.NextToken();
      endDate = RSBDateTime.parse(tk.Value);
    }
  }

  @Override
  public Object clone() {
    SqlCheckCacheStatement obj = new SqlCheckCacheStatement(null, this.sourceTable, this.getSrcQueryStatement(), this.table, this.withRepair, this.skipDeleted, this.startDate, this.endDate);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    SqlCheckCacheStatement o = (SqlCheckCacheStatement)obj;
    this.srcQueryStatement = o.srcQueryStatement;
    super.copy(obj);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }
}