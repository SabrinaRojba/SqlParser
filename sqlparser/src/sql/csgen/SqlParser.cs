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


public sealed class SqlParser : ISqlCloneable {
  private static Dialect DialectProcessor;

  public Exception failedParseException = null;
  private string originalSQLText;
  private SqlStatement actualStatement;
  private StatementType statementType = StatementType.UNPARSED;

  public SqlParser(string original, SqlStatement actual, StatementType type) {
    actualStatement = actual;
    originalSQLText = original;
    statementType = type;
  }

  public SqlParser(SqlStatement actual) {
    actualStatement = actual;
    originalSQLText = null;
    if (actual is SqlSelectIntoStatement) {
      statementType = StatementType.SELECTINTO;
    } else if (actual is SqlQueryStatement) {
      statementType = StatementType.SELECT;
    } else if (actual is SqlDeleteStatement) {
      statementType = StatementType.DELETE;
    } else if (actual is SqlInsertStatement) {
      statementType = StatementType.INSERT;
    } else if (actual is SqlUpsertStatement) {
      statementType = StatementType.UPSERT;
    } else if (actual is SqlUpdateStatement) {
      statementType = StatementType.UPDATE;
    } else if (actual is SqlMergeStatement) {
      statementType = StatementType.MERGE;
    } else if (actual is SqlCacheStatement) {
      statementType = StatementType.CACHE;
    } else if (actual is SqlCheckCacheStatement) {
      statementType = StatementType.CHECKCACHE;
    } else if (actual is SqlReplicateStatement) {
      statementType = StatementType.REPLICATE;
    } else if (actual is SqlKillStatement) {
      statementType = StatementType.KILL;
    } else if (actual is SqlGetDeletedStatement) {
      statementType = StatementType.GETDELETED;
    } else if (actual is SqlMetaQueryStatement) {
      statementType = StatementType.MEMORYQUERY;
    } else if (actual is SqlLoadMemoryQueryStatement) {
      statementType = StatementType.LOADMEMORYQUERY;
    } else if (actual is SqlQueryCacheStatement) {
      statementType = StatementType.QUERYCACHE;
    } else if (actual is SqlResetStatement) {
      statementType = StatementType.RESET;
    } else if (actual is SqlExecStatement) {
      statementType = StatementType.EXEC;
    } else if (actual is SqlCallSPStatement) {
      statementType = StatementType.CALL;
    } else if (actual is SqlCreateTableStatement) {
      statementType = StatementType.CREATE;
    } else if (actual is SqlDropStatement) {
      statementType = StatementType.DROP;
    } else if (actual is SqlCommitStatement) {
      statementType = StatementType.COMMIT;
    } else if (actual is SqlXmlStatement) {
      statementType = StatementType.XML;
    } else if (actual is SqlAlterTableStatement) {
      statementType = StatementType.ALTERTABLE;
    } else {
      statementType = StatementType.UNKNOWN;
    }
  }

  public static void RegisterDialectProcessor(Dialect obj) {
    DialectProcessor = obj;
  }

  public static Dialect GetDialectProcessor() {
    return DialectProcessor;
  }

  public SqlStatement GetStatement() {
    return this.actualStatement;
  }

  public string GetOriginal() {
    return originalSQLText;
  }

  public StatementType GetStatementType() {
    return statementType;
  }

  public Boolean IsStoredProcedure() {
    return statementType == StatementType.CALL || statementType == StatementType.EXEC || statementType == StatementType.SP;
  }

  public Exception GetFailedParseException() {
    return failedParseException;
  }

  public SqlQueryStatement GetSelect() {
    return actualStatement as SqlQueryStatement;
  }

  public SqlSelectIntoStatement GetSelectInto() {
    return actualStatement as SqlSelectIntoStatement;
  }

  public SqlCreateTableStatement GetCreateTable() {
    return actualStatement as SqlCreateTableStatement;
  }

  public SqlCommitStatement GetCommit() {
    return actualStatement as SqlCommitStatement;
  }

  public SqlDropStatement GetDrop() {
    return actualStatement as SqlDropStatement;
  }

  public SqlDeleteStatement GetDelete() {
    return actualStatement as SqlDeleteStatement;
  }

  public SqlInsertStatement GetInsert() {
    return actualStatement as SqlInsertStatement;
  }

  public SqlUpsertStatement GetUpsert() {
    return actualStatement as SqlUpsertStatement;
  }

  public SqlUpdateStatement GetUpdate() {
    return actualStatement as SqlUpdateStatement;
  }

  public SqlMergeStatement GetMerge() {
    return actualStatement as SqlMergeStatement;
  }

  public SqlCacheStatement GetCache() {
    return actualStatement as SqlCacheStatement;
  }

  public SqlReplicateStatement GetReplicate() {
    return actualStatement as SqlReplicateStatement;
  }

  public SqlKillStatement GetKill() {
    return actualStatement as SqlKillStatement;
  }

  public SqlGetDeletedStatement GetGetDeleted() {
    return actualStatement as SqlGetDeletedStatement;
  }

  public SqlResetStatement GetReset() {
    return actualStatement as SqlResetStatement;
  }

  public SqlExecStatement GetExec() {
    return actualStatement as SqlExecStatement;
  }

  public SqlCallSPStatement GetCallable() {
    return actualStatement as SqlCallSPStatement;
  }

  public SqlMetaQueryStatement GetMetaQuery() {
    return actualStatement as SqlMetaQueryStatement;
  }

  public SqlMemoryQueryStatement GetMemoryQuery() {
    return actualStatement as SqlMemoryQueryStatement;
  }

  public SqlLoadMemoryQueryStatement GetLoadMemoryQuery() {
    return actualStatement as SqlLoadMemoryQueryStatement;
  }

  public SqlQueryCacheStatement GetQueryCacheQuery() {
    return actualStatement as SqlQueryCacheStatement;
  }

  public SqlXmlStatement GetXml() {
    return actualStatement as SqlXmlStatement;
  }

  public SqlAlterTableStatement GetAlterTable() {
    return actualStatement as SqlAlterTableStatement;
  }

  public SqlCheckCacheStatement GetCheckCache() {
    return actualStatement as SqlCheckCacheStatement;
  }

  public Object Clone() {
    SqlParser obj = new SqlParser(this.originalSQLText, this.actualStatement == null ? null : (SqlStatement) this.actualStatement.Clone(), this.statementType);
    obj.failedParseException = this.failedParseException;
    return obj;
  }

  
  // We need detect SELECT queries even if we cant parse them to decide if we should return a resultset for them
  public static bool IsSelect(string text) {
    // TODO: improve 
    string sqn = text.Trim().ToLower();
    return sqn.StartsWith("select");
  }

  public static void Normalize(SqlParser parser, IDataConnection dataConnection) {
    string[] normalizationOptions = (string[])dataConnection.GetDbOption(DbOptions.NormalizationOptions);
    NormalizationOption options = NormalizationOption.BuildNormalizationOption(normalizationOptions);
    try {
      parser.Normalize(options, new ColumnMetadataHolder(dataConnection));
    } catch (Exception ex) {
      ILogger logger = dataConnection.GetLogger();
      if (logger != null) {
        logger.Log(2, "", SqlExceptions.LocalizedMessage(SqlExceptions.WARN_STRING, Utilities.GetExceptionMessage(ex)));
      }
    }
  }

  public void Normalize(NormalizationOption option, IDataMetadata dataMetadata) {
    SqlNormalization normalization = new SqlNormalization();
    this.actualStatement = normalization.NormalizeStatement(option, this.actualStatement, dataMetadata);
    if (normalization.GetException() != null) {
      throw normalization.GetException();
    }
  }

  public static SqlParser Parse(string text) {
    return Parse(text, DialectProcessor);
  }

  public static SqlParser Parse(string text, Dialect dialectProcessor) {
    SqlParser res = ParserCore.TryParseXml(text, dialectProcessor);
    if(res != null) {
      return res;
    }
    SqlTokenizer tokenizer = null;
    if (dialectProcessor != null) {
      tokenizer = new SqlTokenizer(text, dialectProcessor.GetEscapeChar());
    } else {
      tokenizer = new SqlTokenizer(text);
    }
    res = ParserCore.TryParse(tokenizer, dialectProcessor);
    return res;
  }

  public static SqlParser[] ParseAll(string text) {
    return ParseAll(text, DialectProcessor);
  }

  public static SqlParser[] ParseAll(string text, Dialect dialectProcessor) {
    JavaArrayList<SqlParser> list = new JavaArrayList<SqlParser>();
    SqlParser parser = ParserCore.TryParseXml(text, dialectProcessor);
    if (parser != null) {
      list.Add(parser);
    } else {
      SqlTokenizer tokenizer = null;
      if (dialectProcessor != null) {
        tokenizer = new SqlTokenizer(text, dialectProcessor.GetEscapeChar());
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
        parser = ParserCore.TryParse(tokenizer, dialectProcessor);
        if (parser == null) {
          continue;
        }
        list.Add(parser);
        if (parser.GetFailedParseException() != null) break;
      }
      if (list.Size() == 0) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EMPTY_STATEMENT));
      }
    }
    return (SqlParser[]) list.ToArray(typeof(SqlParser));
  }
}
}

