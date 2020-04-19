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
 *         Cache [Table] [RENAME TO NEW_TABLE_NAME] [AUTOCOMMIT] [WITH TRUNCATE] [SCHEMA ONLY] [DROP EXISTING] [KEEP SCHEMA] QUERY
 */
public class SqlCacheStatement : SqlStatement {
  private bool truncate = false;
  private bool dropExisting = false;
  private bool schemaOnly = false;
  private bool alterSchema = false;
  private bool keepSchema = false;
  private bool autoCommit = false;
  private bool continueOnError = false;
  private bool skipDeleted = false;
  private bool openTransaction = false;
  private bool commitTransaction = false;
  private bool rollbackTransaction = false;
  private string transactionId = null;
  private SqlStatement cacheStatement;
  private string newTableName = "";
  private bool useTempTable = false;

  public SqlCacheStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlCacheStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    ParseStatement(tokenizer);
    if (this.skipDeleted)
      tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.INVALID_CACHE_OPTION, "SKIP DELETED"));
  }

  public bool IsTruncate() {
    return truncate;
  }

  public void SetTruncate(bool truncate) {
    this.truncate = truncate;
  }

  public bool IsDropExisting() {
    return dropExisting;
  }

  public void SetDropExisting(bool dropExisting) {
    this.dropExisting = dropExisting;
  }

  public bool IsSchemaOnly() {
    return schemaOnly;
  }

  public void SetSchemaOnly(bool schemaOnly) {
    this.schemaOnly = schemaOnly;
  }

  public bool IsAlterSchema() {
    return alterSchema;
  }

  public void SetAlterSchema(bool alterSchema) {
    this.alterSchema = alterSchema;
  }

  public bool IsKeepSchema() {
    return keepSchema;
  }

  public void SetKeepSchema(bool keepSchema) {
    this.keepSchema = keepSchema;
  }

  public bool IsAutoCommit() {
    return autoCommit;
  }

  public void SetAutoCommit(bool autoCommit) {
    this.autoCommit = autoCommit;
  }

  public bool IsContinueOnError() {
    return continueOnError;
  }

  public void SetContinueOnError(bool continueOnError) {
    this.continueOnError = continueOnError;
  }

  public bool IsSkipDeleted() {
    return skipDeleted;
  }

  public void SetSkipDeleted(bool skipDeleted) {
    this.skipDeleted = skipDeleted;
  }

  public bool IsOpenTransaction() {
    return openTransaction;
  }

  public void SetOpenTransaction(bool openTransaction) {
    this.openTransaction = openTransaction;
  }

  public bool IsCommitTransaction() {
    return commitTransaction;
  }

  public void SetCommitTransaction(bool commitTransaction) {
    this.commitTransaction = commitTransaction;
  }

  public bool IsRollbackTransaction() {
    return rollbackTransaction;
  }

  public void SetRollbackTransaction(bool rollbackTransaction) {
    this.rollbackTransaction = rollbackTransaction;
  }

  public string GetTransactionId() {
    return this.transactionId;
  }

  public void SetTransactionId(string transactionId) {
    this.transactionId = transactionId;
  }

  public SqlStatement GetCacheStatement() {
    return this.cacheStatement;
  }

  public void SetCacheStatement(SqlStatement cacheStatement) {
    this.cacheStatement = cacheStatement;
  }

  public string GetNewTableName() {
    return newTableName;
  }

  public void SetNewTableName(string newTableName) {
    this.newTableName = newTableName;
  }

  public bool IsUseTempTable() {
    return useTempTable;
  }

  public void SetUseTempTable(bool useTempTable) {
    this.useTempTable = useTempTable;
  }

  public override void Accept(ISqlQueryVisitor visitor) {

  }

  public bool IsRenameStatement() {
    return (newTableName != null && newTableName.Length > 0);
  }

  public bool IsTransactionStatement() {
    return (openTransaction || commitTransaction || rollbackTransaction);
  }

  public override Object Clone() {
    SqlCacheStatement obj = new SqlCacheStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlCacheStatement o = (SqlCacheStatement)obj;
    this.truncate = o.truncate;
    this.dropExisting = o.dropExisting;
    this.schemaOnly = o.schemaOnly;
    this.keepSchema = o.keepSchema;
    this.alterSchema = o.alterSchema;
    this.autoCommit = o.autoCommit;
    this.continueOnError = o.continueOnError;
    this.skipDeleted = o.skipDeleted;
    this.openTransaction = o.openTransaction;
    this.commitTransaction = o.commitTransaction;
    this.rollbackTransaction = o.rollbackTransaction;
    this.transactionId = o.transactionId;
    this.cacheStatement = o.cacheStatement == null ? null : (SqlStatement)o.cacheStatement.Clone();
    this.newTableName = o.newTableName;
    this.useTempTable = o.useTempTable;
  }

  private void ParseStatement(SqlTokenizer tokenizer) {
    tokenizer.NextToken();
    ParseTableName(tokenizer);
    ParseCacheOptions(tokenizer);
    bool parseQuery = !IsTransactionStatement();
    if (parseQuery) {
      ParseQuery(tokenizer, dialectProcessor);
    }
  }

  public virtual string GetStatementName() {
    return "CACHE";
  }

  protected virtual void ParseQuery(SqlTokenizer tokenizer, Dialect dialectProcessor) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals(SqlToken.None) || tokenizer.EOF()) {
      if (IsTableNameStatement()) return;
      if (IsRenameStatement()) return;
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_QUERY));
    } else if (next.Equals("SELECT")) {
      cacheStatement = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
    } else if (next.Equals("GETDELETED")) {
      cacheStatement = new SqlGetDeletedStatement(tokenizer, dialectProcessor);
    } else {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_SELECT_OR_GETDELETED, next.Value));
    }
    if (Utilities.IsNullOrEmpty(this.tableName)) {
      this.tableName = cacheStatement.GetTableName();
      this.table = cacheStatement.GetTable();
    }
  }

  protected virtual void ParseCacheOptions(SqlTokenizer tokenizer) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.IsEmpty()) {
      return;
    }
    while (true) {
      if (next.Equals("WITH")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRUNCATE");
        truncate = true;
      } else if (next.Equals("SCHEMA")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("ONLY");
        schemaOnly = true;
      } else if (next.Equals("DROP")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("EXISTING");
        dropExisting = true;
      } else if (next.Equals("CONTINUEONERROR")) {
        tokenizer.NextToken();
        continueOnError = true;
      } else if (next.Equals("SKIP")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("DELETED");
        skipDeleted = true;
      } else if (next.Equals("KEEP")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("SCHEMA");
        keepSchema = true;
      } else if (next.Equals("ALTER")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("SCHEMA");
        alterSchema = true;
      } else if (next.Equals("RENAME")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TO");
        newTableName = tokenizer.NextToken().Value;
      } else if (next.Equals("AUTOCOMMIT")) {
        tokenizer.NextToken();
        autoCommit = true;
      } else if (next.Equals("OPEN")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRANSACTION");
        transactionId = tokenizer.NextToken().Value;
        openTransaction = true;
      } else if (next.Equals("COMMIT")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRANSACTION");
        transactionId = tokenizer.NextToken().Value;
        commitTransaction = true;
      } else if (next.Equals("ROLLBACK")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRANSACTION");
        transactionId = tokenizer.NextToken().Value;
        rollbackTransaction = true;
      } else if (next.Equals("TRANSACTION")) {
        tokenizer.NextToken();
        transactionId = tokenizer.NextToken().Value;
      } else {
        break;
      }
      next = tokenizer.LookaheadToken2(); // look ahead one more token
    }
  }

  protected void ParseTableName(SqlTokenizer tokenizer) {
    SqlToken tk = tokenizer.LookaheadToken2();
    if (tk.Equals("TO") || tk.Equals("UPDATE") || tk.Equals("TEMP")) {
      if(tk.Equals("TEMP"))
        this.useTempTable = true;
      tokenizer.NextToken(); // optional, CACHE [UPDATE|TO] [TABLE_IN_CACHE] QUERY
      tk = tokenizer.LookaheadToken2();
    }
    if (tk.IsEmpty()) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    if (tk.Equals("SELECT") || tk.Equals("GETDELETED")
            || tk.Equals("WITH")
            || tk.Equals("SCHEMA")
            || tk.Equals("DROP")
            || tk.Equals("KEEP")
            || tk.Equals("RENAME")
            || tk.Equals("AUTOCOMMIT")
            || tk.Equals("OPEN")
            || tk.Equals("COMMIT")
            || tk.Equals("ROLLBACK")
            || tk.Equals("TRANSACTION")) {
      return;
    }
    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private bool IsTableNameStatement() {
    return (tableName != null && tableName.Length > 0);
  }
}
}

