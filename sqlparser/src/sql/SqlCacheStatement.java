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
import rssbus.oputils.common.Utilities;

/**
 * Syntax:
 *         Cache [Table] [RENAME TO NEW_TABLE_NAME] [AUTOCOMMIT] [WITH TRUNCATE] [SCHEMA ONLY] [DROP EXISTING] [KEEP SCHEMA] QUERY
 */
public class SqlCacheStatement extends SqlStatement {
  private boolean truncate = false;
  private boolean dropExisting = false;
  private boolean schemaOnly = false;
  private boolean alterSchema = false;
  private boolean keepSchema = false;
  private boolean autoCommit = false;
  private boolean continueOnError = false;
  private boolean skipDeleted = false;
  private boolean openTransaction = false;
  private boolean commitTransaction = false;
  private boolean rollbackTransaction = false;
  private String transactionId = null;
  private SqlStatement cacheStatement;
  private String newTableName = "";
  private boolean useTempTable = false;

  public SqlCacheStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlCacheStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    parseStatement(tokenizer);
    if (this.skipDeleted)
      tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.INVALID_CACHE_OPTION, "SKIP DELETED"));
  }

  public boolean isTruncate() {
    return truncate;
  }

  public void setTruncate(boolean truncate) {
    this.truncate = truncate;
  }

  public boolean isDropExisting() {
    return dropExisting;
  }

  public void setDropExisting(boolean dropExisting) {
    this.dropExisting = dropExisting;
  }

  public boolean isSchemaOnly() {
    return schemaOnly;
  }

  public void setSchemaOnly(boolean schemaOnly) {
    this.schemaOnly = schemaOnly;
  }

  public boolean isAlterSchema() {
    return alterSchema;
  }

  public void setAlterSchema(boolean alterSchema) {
    this.alterSchema = alterSchema;
  }

  public boolean isKeepSchema() {
    return keepSchema;
  }

  public void setKeepSchema(boolean keepSchema) {
    this.keepSchema = keepSchema;
  }

  public boolean isAutoCommit() {
    return autoCommit;
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  public boolean isContinueOnError() {
    return continueOnError;
  }

  public void setContinueOnError(boolean continueOnError) {
    this.continueOnError = continueOnError;
  }

  public boolean isSkipDeleted() {
    return skipDeleted;
  }

  public void setSkipDeleted(boolean skipDeleted) {
    this.skipDeleted = skipDeleted;
  }

  public boolean isOpenTransaction() {
    return openTransaction;
  }

  public void setOpenTransaction(boolean openTransaction) {
    this.openTransaction = openTransaction;
  }

  public boolean isCommitTransaction() {
    return commitTransaction;
  }

  public void setCommitTransaction(boolean commitTransaction) {
    this.commitTransaction = commitTransaction;
  }

  public boolean isRollbackTransaction() {
    return rollbackTransaction;
  }

  public void setRollbackTransaction(boolean rollbackTransaction) {
    this.rollbackTransaction = rollbackTransaction;
  }

  public String getTransactionId() {
    return this.transactionId;
  }

  public void setTransactionId(String transactionId) {
    this.transactionId = transactionId;
  }

  public SqlStatement getCacheStatement() {
    return this.cacheStatement;
  }

  public void setCacheStatement(SqlStatement cacheStatement) {
    this.cacheStatement = cacheStatement;
  }

  public String getNewTableName() {
    return newTableName;
  }

  public void setNewTableName(String newTableName) {
    this.newTableName = newTableName;
  }

  public boolean isUseTempTable() {
    return useTempTable;
  }

  public void setUseTempTable(boolean useTempTable) {
    this.useTempTable = useTempTable;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public boolean isRenameStatement() {
    return (newTableName != null && newTableName.length() > 0);
  }

  public boolean isTransactionStatement() {
    return (openTransaction || commitTransaction || rollbackTransaction);
  }

  @Override
  public Object clone() {
    SqlCacheStatement obj = new SqlCacheStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
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
    this.cacheStatement = o.cacheStatement == null ? null : (SqlStatement)o.cacheStatement.clone();
    this.newTableName = o.newTableName;
    this.useTempTable = o.useTempTable;
  }

  private void parseStatement(SqlTokenizer tokenizer) throws Exception {
    tokenizer.NextToken();
    parseTableName(tokenizer);
    parseCacheOptions(tokenizer);
    boolean parseQuery = !isTransactionStatement();
    if (parseQuery) {
      parseQuery(tokenizer, dialectProcessor);
    }
  }

  public /*#virtual#*/ String getStatementName() {
    return "CACHE";
  }

  protected /*#virtual#*/ void parseQuery(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals(SqlToken.None) || tokenizer.EOF()) {
      if (isTableNameStatement()) return;
      if (isRenameStatement()) return;
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_QUERY));
    } else if (next.equals("SELECT")) {
      cacheStatement = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
    } else if (next.equals("GETDELETED")) {
      cacheStatement = new SqlGetDeletedStatement(tokenizer, dialectProcessor);
    } else {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_SELECT_OR_GETDELETED, next.Value));
    }
    if (Utilities.isNullOrEmpty(this.tableName)) {
      this.tableName = cacheStatement.getTableName();
      this.table = cacheStatement.getTable();
    }
  }

  protected /*#virtual#*/ void parseCacheOptions(SqlTokenizer tokenizer) throws Exception {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.IsEmpty()) {
      return;
    }
    while (true) {
      if (next.equals("WITH")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRUNCATE");
        truncate = true;
      } else if (next.equals("SCHEMA")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("ONLY");
        schemaOnly = true;
      } else if (next.equals("DROP")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("EXISTING");
        dropExisting = true;
      } else if (next.equals("CONTINUEONERROR")) {
        tokenizer.NextToken();
        continueOnError = true;
      } else if (next.equals("SKIP")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("DELETED");
        skipDeleted = true;
      } else if (next.equals("KEEP")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("SCHEMA");
        keepSchema = true;
      } else if (next.equals("ALTER")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("SCHEMA");
        alterSchema = true;
      } else if (next.equals("RENAME")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TO");
        newTableName = tokenizer.NextToken().Value;
      } else if (next.equals("AUTOCOMMIT")) {
        tokenizer.NextToken();
        autoCommit = true;
      } else if (next.equals("OPEN")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRANSACTION");
        transactionId = tokenizer.NextToken().Value;
        openTransaction = true;
      } else if (next.equals("COMMIT")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRANSACTION");
        transactionId = tokenizer.NextToken().Value;
        commitTransaction = true;
      } else if (next.equals("ROLLBACK")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextIdentifier("TRANSACTION");
        transactionId = tokenizer.NextToken().Value;
        rollbackTransaction = true;
      } else if (next.equals("TRANSACTION")) {
        tokenizer.NextToken();
        transactionId = tokenizer.NextToken().Value;
      } else {
        break;
      }
      next = tokenizer.LookaheadToken2(); // look ahead one more token
    }
  }

  protected void parseTableName(SqlTokenizer tokenizer) throws Exception {
    SqlToken tk = tokenizer.LookaheadToken2();
    if (tk.equals("TO") || tk.equals("UPDATE") || tk.equals("TEMP")) {
      if(tk.equals("TEMP"))
        this.useTempTable = true;
      tokenizer.NextToken(); // optional, CACHE [UPDATE|TO] [TABLE_IN_CACHE] QUERY
      tk = tokenizer.LookaheadToken2();
    }
    if (tk.IsEmpty()) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    if (tk.equals("SELECT") || tk.equals("GETDELETED")
            || tk.equals("WITH")
            || tk.equals("SCHEMA")
            || tk.equals("DROP")
            || tk.equals("KEEP")
            || tk.equals("RENAME")
            || tk.equals("AUTOCOMMIT")
            || tk.equals("OPEN")
            || tk.equals("COMMIT")
            || tk.equals("ROLLBACK")
            || tk.equals("TRANSACTION")) {
      return;
    }
    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private boolean isTableNameStatement() {
    return (tableName != null && tableName.length() > 0);
  }
}

