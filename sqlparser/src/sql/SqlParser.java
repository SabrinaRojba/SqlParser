//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.*;
import rssbus.oputils.common.Utilities;

import java.util.ArrayList;

public final class SqlParser implements ISqlCloneable {
  private static Dialect DialectProcessor;

  public Exception failedParseException = null;
  private String originalSQLText;
  private SqlStatement actualStatement;
  private StatementType statementType = StatementType.UNPARSED;

  public SqlParser(String original, SqlStatement actual, StatementType type) {
    actualStatement = actual;
    originalSQLText = original;
    statementType = type;
  }

  public SqlParser(SqlStatement actual) {
    actualStatement = actual;
    originalSQLText = null;
    if (actual instanceof SqlSelectIntoStatement) {
      statementType = StatementType.SELECTINTO;
    } else if (actual instanceof SqlQueryStatement) {
      statementType = StatementType.SELECT;
    } else if (actual instanceof SqlDeleteStatement) {
      statementType = StatementType.DELETE;
    } else if (actual instanceof SqlInsertStatement) {
      statementType = StatementType.INSERT;
    } else if (actual instanceof SqlUpsertStatement) {
      statementType = StatementType.UPSERT;
    } else if (actual instanceof SqlUpdateStatement) {
      statementType = StatementType.UPDATE;
    } else if (actual instanceof SqlMergeStatement) {
      statementType = StatementType.MERGE;
    } else if (actual instanceof SqlCacheStatement) {
      statementType = StatementType.CACHE;
    } else if (actual instanceof SqlCheckCacheStatement) {
      statementType = StatementType.CHECKCACHE;
    } else if (actual instanceof SqlReplicateStatement) {
      statementType = StatementType.REPLICATE;
    } else if (actual instanceof SqlKillStatement) {
      statementType = StatementType.KILL;
    } else if (actual instanceof SqlGetDeletedStatement) {
      statementType = StatementType.GETDELETED;
    } else if (actual instanceof SqlMetaQueryStatement) {
      statementType = StatementType.MEMORYQUERY;
    } else if (actual instanceof SqlLoadMemoryQueryStatement) {
      statementType = StatementType.LOADMEMORYQUERY;
    } else if (actual instanceof SqlQueryCacheStatement) {
      statementType = StatementType.QUERYCACHE;
    } else if (actual instanceof SqlResetStatement) {
      statementType = StatementType.RESET;
    } else if (actual instanceof SqlExecStatement) {
      statementType = StatementType.EXEC;
    } else if (actual instanceof SqlCallSPStatement) {
      statementType = StatementType.CALL;
    } else if (actual instanceof SqlCreateTableStatement) {
      statementType = StatementType.CREATE;
    } else if (actual instanceof SqlDropStatement) {
      statementType = StatementType.DROP;
    } else if (actual instanceof SqlCommitStatement) {
      statementType = StatementType.COMMIT;
    } else if (actual instanceof SqlXmlStatement) {
      statementType = StatementType.XML;
    } else if (actual instanceof SqlAlterTableStatement) {
      statementType = StatementType.ALTERTABLE;
    } else {
      statementType = StatementType.UNKNOWN;
    }
  }

  public static void registerDialectProcessor(Dialect obj) {
    DialectProcessor = obj;
  }

  public static Dialect getDialectProcessor() {
    return DialectProcessor;
  }

  public SqlStatement getStatement() {
    return this.actualStatement;
  }

  public String getOriginal() {
    return originalSQLText;
  }

  public StatementType getStatementType() {
    return statementType;
  }

  public Boolean IsStoredProcedure() {
    return statementType == StatementType.CALL || statementType == StatementType.EXEC || statementType == StatementType.SP;
  }

  public Exception getFailedParseException() {
    return failedParseException;
  }

  public SqlQueryStatement getSelect() {
    return /*@*/as(actualStatement, SqlQueryStatement.class);/*@*//*#actualStatement as SqlQueryStatement;#*/
  }

  public SqlSelectIntoStatement getSelectInto() {
    return /*@*/as(actualStatement, SqlSelectIntoStatement.class);/*@*//*#actualStatement as SqlSelectIntoStatement;#*/
  }

  public SqlCreateTableStatement getCreateTable() {
    return /*@*/as(actualStatement, SqlCreateTableStatement.class);/*@*//*#actualStatement as SqlCreateTableStatement;#*/
  }

  public SqlCommitStatement getCommit() {
    return /*@*/as(actualStatement, SqlCommitStatement.class);/*@*//*#actualStatement as SqlCommitStatement;#*/
  }

  public SqlDropStatement getDrop() {
    return /*@*/as(actualStatement, SqlDropStatement.class);/*@*//*#actualStatement as SqlDropStatement;#*/
  }

  public SqlDeleteStatement getDelete() {
    return /*@*/as(actualStatement, SqlDeleteStatement.class);/*@*//*#actualStatement as SqlDeleteStatement;#*/
  }

  public SqlInsertStatement getInsert() {
    return /*@*/as(actualStatement, SqlInsertStatement.class);/*@*//*#actualStatement as SqlInsertStatement;#*/
  }

  public SqlUpsertStatement getUpsert() {
    return /*@*/as(actualStatement, SqlUpsertStatement.class);/*@*//*#actualStatement as SqlUpsertStatement;#*/
  }

  public SqlUpdateStatement getUpdate() {
    return /*@*/as(actualStatement, SqlUpdateStatement.class);/*@*//*#actualStatement as SqlUpdateStatement;#*/
  }

  public SqlMergeStatement getMerge() {
    return /*@*/as(actualStatement, SqlMergeStatement.class);/*@*//*#actualStatement as SqlMergeStatement;#*/
  }

  public SqlCacheStatement getCache() {
    return /*@*/as(actualStatement, SqlCacheStatement.class);/*@*//*#actualStatement as SqlCacheStatement;#*/
  }

  public SqlReplicateStatement getReplicate() {
    return /*@*/as(actualStatement, SqlReplicateStatement.class);/*@*//*#actualStatement as SqlReplicateStatement;#*/
  }

  public SqlKillStatement getKill() {
    return /*@*/as(actualStatement, SqlKillStatement.class);/*@*//*#actualStatement as SqlKillStatement;#*/
  }

  public SqlGetDeletedStatement getGetDeleted() {
    return /*@*/as(actualStatement, SqlGetDeletedStatement.class);/*@*//*#actualStatement as SqlGetDeletedStatement;#*/
  }

  public SqlResetStatement getReset() {
    return /*@*/as(actualStatement, SqlResetStatement.class);/*@*//*#actualStatement as SqlResetStatement;#*/
  }

  public SqlExecStatement getExec() {
    return /*@*/as(actualStatement, SqlExecStatement.class);/*@*//*#actualStatement as SqlExecStatement;#*/
  }

  public SqlCallSPStatement getCallable() {
    return /*@*/as(actualStatement, SqlCallSPStatement.class);/*@*//*#actualStatement as SqlCallSPStatement;#*/
  }

  public SqlMetaQueryStatement getMetaQuery() {
    return /*@*/as(actualStatement, SqlMetaQueryStatement.class);/*@*//*#actualStatement as SqlMetaQueryStatement;#*/
  }

  public SqlMemoryQueryStatement getMemoryQuery() {
    return /*@*/as(actualStatement, SqlMemoryQueryStatement.class);/*@*//*#actualStatement as SqlMemoryQueryStatement;#*/
  }

  public SqlLoadMemoryQueryStatement getLoadMemoryQuery() {
    return /*@*/as(actualStatement, SqlLoadMemoryQueryStatement.class);/*@*//*#actualStatement as SqlLoadMemoryQueryStatement;#*/
  }

  public SqlQueryCacheStatement getQueryCacheQuery() {
    return /*@*/as(actualStatement, SqlQueryCacheStatement.class);/*@*//*#actualStatement as SqlQueryCacheStatement;#*/
  }

  public SqlXmlStatement getXml() {
    return /*@*/as(actualStatement, SqlXmlStatement.class);/*@*//*#actualStatement as SqlXmlStatement;#*/
  }

  public SqlAlterTableStatement getAlterTable() {
    return /*@*/as(actualStatement, SqlAlterTableStatement.class);/*@*//*#actualStatement as SqlAlterTableStatement;#*/
  }

  public SqlCheckCacheStatement getCheckCache() {
    return /*@*/as(actualStatement, SqlCheckCacheStatement.class);/*@*//*#actualStatement as SqlCheckCacheStatement;#*/
  }

  public Object clone() {
    SqlParser obj = new SqlParser(this.originalSQLText, this.actualStatement == null ? null : (SqlStatement) this.actualStatement.clone(), this.statementType);
    obj.failedParseException = this.failedParseException;
    return obj;
  }

  //@
  // A helper method to simulate the "as" keyword in CSharp
  public static <T> T as(Object o, Class<T> tClass) {
    return tClass.isInstance(o) ? (T) o : null;
  }

  //@
  // We need detect SELECT queries even if we cant parse them to decide if we should return a resultset for them
  public static boolean isSelect(String text) {
    // TODO: improve 
    String sqn = text.trim().toLowerCase();
    return sqn.startsWith("select");
  }

  public static void normalize(SqlParser parser, IDataConnection dataConnection) {
    String[] normalizationOptions = (String[])dataConnection.getDbOption(DbOptions.NormalizationOptions);
    NormalizationOption options = NormalizationOption.buildNormalizationOption(normalizationOptions);
    try {
      parser.normalize(options, new ColumnMetadataHolder(dataConnection));
    } catch (Exception ex) {
      ILogger logger = dataConnection.getLogger();
      if (logger != null) {
        logger.log(2, "", SqlExceptions.localizedMessage(SqlExceptions.WARN_STRING, Utilities.getExceptionMessage(ex)));
      }
    }
  }

  public void normalize(NormalizationOption option, IDataMetadata dataMetadata) throws Exception {
    SqlNormalization normalization = new SqlNormalization();
    this.actualStatement = normalization.normalizeStatement(option, this.actualStatement, dataMetadata);
    if (normalization.getException() != null) {
      throw normalization.getException();
    }
  }

  public static SqlParser Parse(String text) {
    return Parse(text, DialectProcessor);
  }

  public static SqlParser Parse(String text, Dialect dialectProcessor) {
    SqlParser res = ParserCore.tryParseXml(text, dialectProcessor);
    if(res != null) {
      return res;
    }
    SqlTokenizer tokenizer = null;
    if (dialectProcessor != null) {
      tokenizer = new SqlTokenizer(text, dialectProcessor.getEscapeChar());
    } else {
      tokenizer = new SqlTokenizer(text);
    }
    res = ParserCore.tryParse(tokenizer, dialectProcessor);
    return res;
  }

  public static SqlParser[] ParseAll(String text) throws Exception {
    return ParseAll(text, DialectProcessor);
  }

  public static SqlParser[] ParseAll(String text, Dialect dialectProcessor) throws Exception {
    ArrayList<SqlParser> list = new ArrayList<SqlParser>();
    SqlParser parser = ParserCore.tryParseXml(text, dialectProcessor);
    if (parser != null) {
      list.add(parser);
    } else {
      SqlTokenizer tokenizer = null;
      if (dialectProcessor != null) {
        tokenizer = new SqlTokenizer(text, dialectProcessor.getEscapeChar());
      } else {
        tokenizer = new SqlTokenizer(text);
      }
      while (!tokenizer.EOF()) {
        SqlToken tryNext = tokenizer.LookaheadToken2();
        if (tryNext.IsEmpty()) {
          if (tokenizer.EOF()) {
            break;
          } else {
            tokenizer.NextToken();
            continue;
          }
        }
        parser = ParserCore.tryParse(tokenizer, dialectProcessor);
        if (parser == null) {
          continue;
        }
        list.add(parser);
        if (parser.getFailedParseException() != null) break;
      }
      if (list.size() == 0) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EMPTY_STATEMENT));
      }
    }
    return (SqlParser[]) list.toArray(/*@*/new SqlParser[0]/*@*//*#typeof(SqlParser)#*/);
  }
}

