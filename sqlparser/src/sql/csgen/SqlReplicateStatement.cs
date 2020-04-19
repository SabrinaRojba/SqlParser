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
 *         REPLICATE [Table] [RENAME TO NEW_TABLE_NAME] [AUTOCOMMIT] [SCHEMA ONLY] QUERY
 */
public class SqlReplicateStatement : SqlCacheStatement {
  private SqlCollection<SqlTable> includedTables = null;
  private SqlCollection<SqlTable> excludedTables = null;
  private SqlCollection<SqlColumn> excludedColumns = null;
  private bool replicateAllQuery    = false;

  public SqlCollection<SqlColumn> GetExcludedColumns() {
    if (this.excludedColumns == null)
      return new SqlCollection<SqlColumn>();
    return this.excludedColumns;
  }

  public void SetExcludedColumns(SqlCollection<SqlColumn> columns) {
    this.excludedColumns = columns;
  }

  public SqlCollection<SqlTable> GetExcludedTables() {
    if (this.excludedTables == null)
      return new SqlCollection<SqlTable>();
    return this.excludedTables;
  }

  public void SetExcludedTables(SqlCollection<SqlTable> tables) {
    this.excludedTables = tables;
  }

  public SqlCollection<SqlTable> GetIncludedTables() {
    if (this.includedTables == null)
      return new SqlCollection<SqlTable>();
    return this.includedTables;
  }

  public void SetIncludedTables(SqlCollection<SqlTable> tables) {
    this.includedTables = tables;
  }

  public bool IsReplicateAllQuery() {
    return this.replicateAllQuery;
  }

  public void SetReplicateAllQuery(bool replicateAllQuery) {
    this.replicateAllQuery = replicateAllQuery;
  }

  public SqlReplicateStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlReplicateStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.NextToken();
    SqlToken tk = tokenizer.LookaheadToken2();
    if (tk.Equals("ALL") ) {
      replicateAllQuery = true;
      tokenizer.NextToken();
      ParseAllStatement(tokenizer);
    } else if (tk.Equals("TABLES")) {
      tokenizer.NextToken();
      includedTables = ParseTables(tokenizer);
      ParseTablesStatement(tokenizer);
    } else {
      ParseTableName(tokenizer);
      ParseTableStatement(tokenizer);
    }
  }

  private void ParseAllStatement(SqlTokenizer tokenizer) {
    while (true) {
      SqlToken next = tokenizer.LookaheadToken2();
      if (next.Equals("EXCLUDE")) {
        tokenizer.NextToken();
        next = tokenizer.LookaheadToken2();
        if (next.Equals("(")) {
          excludedTables = ParseTables(tokenizer);
        } else if (next.Equals("TABLES")) {
          tokenizer.NextToken();
          excludedTables = ParseTables(tokenizer);
        } else if (next.Equals("COLUMNS")) {
          tokenizer.NextToken();
          excludedColumns = ParseColumns(tokenizer);
        }
      } else if (next.Equals(SqlToken.None) || tokenizer.EOF()) {
        return;
      } else {
        if (IsReplicateAllQuery() || (includedTables != null && includedTables.Size() > 0))  {
          throw tokenizer.MalformedSql("The cache statement should be empty when using REPLICATE ALL statement.");
        }
      }
    }
  }

  private void ParseTablesStatement(SqlTokenizer tokenizer) {
    while (true) {
      SqlToken next = tokenizer.LookaheadToken2();
      if (next.Equals("EXCLUDE")) {
        tokenizer.NextToken();
        next = tokenizer.LookaheadToken2();
        if (next.Equals("COLUMNS")) {
          tokenizer.NextToken();
          excludedColumns = ParseColumns(tokenizer);
        }
      } else if (next.Equals(SqlToken.None) || tokenizer.EOF()) {
        return;
      } else {
        if (IsReplicateAllQuery() || (includedTables != null && includedTables.Size() > 0))  {
          throw tokenizer.MalformedSql("The cache statement should be empty when using REPLICATE TABLES statement.");
        }
      }
    }
  }

  private void ParseTableStatement(SqlTokenizer tokenizer) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals("EXCLUDE")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.Equals("COLUMNS")) {
        tokenizer.NextToken();
        excludedColumns = ParseColumns(tokenizer);
      } else {
        tokenizer.MalformedSql("Invalid token EXCLUDE used in Replication Statement.");
      }
    }
    ParseCacheOptions(tokenizer);
    bool parseQuery = !IsTransactionStatement();
    if (parseQuery) {
      ParseQuery(tokenizer, dialectProcessor);
    }
    if (IsTruncate()) { throw tokenizer.MalformedSql("Invalid token WITH TRUNCATE used in Replication Statement."); }
    if (IsDropExisting()) { throw tokenizer.MalformedSql("Invalid token DROP EXISTING used in Replication Statement."); }
    if (IsSchemaOnly()) { throw tokenizer.MalformedSql("Invalid token SCHEMA ONLY used in Replication Statement."); }
    if (IsAutoCommit()) { throw tokenizer.MalformedSql("Invalid token AUTOCOMMIT used in Replication Statement."); }
  }

  private SqlCollection<SqlColumn> ParseColumns(SqlTokenizer tokenizer) {
    SqlCollection<SqlColumn> columns;
    tokenizer.EnsureNextToken("(");
    columns = ParserCore.ParseColumns(tokenizer, this);
    tokenizer.EnsureNextToken(")");
    return columns;
  }

  private SqlCollection<SqlTable> ParseTables(SqlTokenizer tokenizer) {
    SqlCollection<SqlTable> tables = new SqlCollection<SqlTable>();
    tokenizer.EnsureNextToken("(");
    SqlToken tryNext;
    do {
      string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
      tables.Add(new SqlTable(period[0], period[1], period[2], period[2]));
      tryNext = tokenizer.LookaheadToken2();
      if (!tryNext.Equals(",")) {
        break;
      } else {
        tokenizer.NextToken();
      }
    } while (true);
    tokenizer.EnsureNextToken(")");
    return tables;
  }

  public override string GetStatementName() {
    return "REPLICATE";
  }

  public override Object Clone() {
    SqlReplicateStatement obj = new SqlReplicateStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlReplicateStatement o = (SqlReplicateStatement)obj;
    this.replicateAllQuery = o.replicateAllQuery;
    this.includedTables = o.includedTables == null ? null : (SqlCollection<SqlTable>) o.includedTables.Clone();
    this.excludedTables = o.excludedTables == null ? null : (SqlCollection<SqlTable>) o.excludedTables.Clone();
    this.excludedColumns = o.excludedColumns == null ? null : (SqlCollection<SqlColumn>) o.excludedColumns.Clone();
  }
}
}

