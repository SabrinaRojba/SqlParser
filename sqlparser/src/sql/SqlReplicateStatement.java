//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.*;

/**
 * Syntax:
 *         REPLICATE [Table] [RENAME TO NEW_TABLE_NAME] [AUTOCOMMIT] [SCHEMA ONLY] QUERY
 */
public class SqlReplicateStatement extends SqlCacheStatement {
  private SqlCollection<SqlTable> includedTables = null;
  private SqlCollection<SqlTable> excludedTables = null;
  private SqlCollection<SqlColumn> excludedColumns = null;
  private boolean replicateAllQuery    = false;

  public SqlCollection<SqlColumn> getExcludedColumns() {
    if (this.excludedColumns == null)
      return new SqlCollection<SqlColumn>();
    return this.excludedColumns;
  }

  public void setExcludedColumns(SqlCollection<SqlColumn> columns) {
    this.excludedColumns = columns;
  }

  public SqlCollection<SqlTable> getExcludedTables() {
    if (this.excludedTables == null)
      return new SqlCollection<SqlTable>();
    return this.excludedTables;
  }

  public void setExcludedTables(SqlCollection<SqlTable> tables) {
    this.excludedTables = tables;
  }

  public SqlCollection<SqlTable> getIncludedTables() {
    if (this.includedTables == null)
      return new SqlCollection<SqlTable>();
    return this.includedTables;
  }

  public void setIncludedTables(SqlCollection<SqlTable> tables) {
    this.includedTables = tables;
  }

  public boolean isReplicateAllQuery() {
    return this.replicateAllQuery;
  }

  public void setReplicateAllQuery(boolean replicateAllQuery) {
    this.replicateAllQuery = replicateAllQuery;
  }

  public SqlReplicateStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlReplicateStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    SqlToken tk = tokenizer.LookaheadToken2();
    if (tk.equals("ALL") ) {
      replicateAllQuery = true;
      tokenizer.NextToken();
      parseAllStatement(tokenizer);
    } else if (tk.equals("TABLES")) {
      tokenizer.NextToken();
      includedTables = parseTables(tokenizer);
      parseTablesStatement(tokenizer);
    } else {
      parseTableName(tokenizer);
      parseTableStatement(tokenizer);
    }
  }

  private void parseAllStatement(SqlTokenizer tokenizer) throws Exception {
    while (true) {
      SqlToken next = tokenizer.LookaheadToken2();
      if (next.equals("EXCLUDE")) {
        tokenizer.NextToken();
        next = tokenizer.LookaheadToken2();
        if (next.equals("(")) {
          excludedTables = parseTables(tokenizer);
        } else if (next.equals("TABLES")) {
          tokenizer.NextToken();
          excludedTables = parseTables(tokenizer);
        } else if (next.equals("COLUMNS")) {
          tokenizer.NextToken();
          excludedColumns = parseColumns(tokenizer);
        }
      } else if (next.equals(SqlToken.None) || tokenizer.EOF()) {
        return;
      } else {
        if (isReplicateAllQuery() || (includedTables != null && includedTables.size() > 0))  {
          throw tokenizer.MalformedSql("The cache statement should be empty when using REPLICATE ALL statement.");
        }
      }
    }
  }

  private void parseTablesStatement(SqlTokenizer tokenizer) throws Exception {
    while (true) {
      SqlToken next = tokenizer.LookaheadToken2();
      if (next.equals("EXCLUDE")) {
        tokenizer.NextToken();
        next = tokenizer.LookaheadToken2();
        if (next.equals("COLUMNS")) {
          tokenizer.NextToken();
          excludedColumns = parseColumns(tokenizer);
        }
      } else if (next.equals(SqlToken.None) || tokenizer.EOF()) {
        return;
      } else {
        if (isReplicateAllQuery() || (includedTables != null && includedTables.size() > 0))  {
          throw tokenizer.MalformedSql("The cache statement should be empty when using REPLICATE TABLES statement.");
        }
      }
    }
  }

  private void parseTableStatement(SqlTokenizer tokenizer) throws Exception {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals("EXCLUDE")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.equals("COLUMNS")) {
        tokenizer.NextToken();
        excludedColumns = parseColumns(tokenizer);
      } else {
        tokenizer.MalformedSql("Invalid token EXCLUDE used in Replication Statement.");
      }
    }
    parseCacheOptions(tokenizer);
    boolean parseQuery = !isTransactionStatement();
    if (parseQuery) {
      parseQuery(tokenizer, dialectProcessor);
    }
    if (isTruncate()) { throw tokenizer.MalformedSql("Invalid token WITH TRUNCATE used in Replication Statement."); }
    if (isDropExisting()) { throw tokenizer.MalformedSql("Invalid token DROP EXISTING used in Replication Statement."); }
    if (isSchemaOnly()) { throw tokenizer.MalformedSql("Invalid token SCHEMA ONLY used in Replication Statement."); }
    if (isAutoCommit()) { throw tokenizer.MalformedSql("Invalid token AUTOCOMMIT used in Replication Statement."); }
  }

  private SqlCollection<SqlColumn> parseColumns(SqlTokenizer tokenizer) throws Exception {
    SqlCollection<SqlColumn> columns;
    tokenizer.EnsureNextToken("(");
    columns = ParserCore.parseColumns(tokenizer, this);
    tokenizer.EnsureNextToken(")");
    return columns;
  }

  private SqlCollection<SqlTable> parseTables(SqlTokenizer tokenizer) throws Exception {
    SqlCollection<SqlTable> tables = new SqlCollection<SqlTable>();
    tokenizer.EnsureNextToken("(");
    SqlToken tryNext;
    do {
      String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
      tables.add(new SqlTable(period[0], period[1], period[2], period[2]));
      tryNext = tokenizer.LookaheadToken2();
      if (!tryNext.equals(",")) {
        break;
      } else {
        tokenizer.NextToken();
      }
    } while (true);
    tokenizer.EnsureNextToken(")");
    return tables;
  }

  public /*#override#*/ String getStatementName() {
    return "REPLICATE";
  }

  @Override
  public Object clone() {
    SqlReplicateStatement obj = new SqlReplicateStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlReplicateStatement o = (SqlReplicateStatement)obj;
    this.replicateAllQuery = o.replicateAllQuery;
    this.includedTables = o.includedTables == null ? null : (SqlCollection<SqlTable>) o.includedTables.clone();
    this.excludedTables = o.excludedTables == null ? null : (SqlCollection<SqlTable>) o.excludedTables.clone();
    this.excludedColumns = o.excludedColumns == null ? null : (SqlCollection<SqlColumn>) o.excludedColumns.clone();
  }
}