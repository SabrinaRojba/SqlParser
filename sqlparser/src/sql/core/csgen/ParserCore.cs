using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;

namespace RSSBus.core {

using RSSBus.core;
using RSSBus;
using CData.Sql;


public sealed class ParserCore {
  public const string DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX = "NESTED_QUERY";
  public const string DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX = "NESTED_JOIN";
  public const string DERIVED_VALUES_TABLE_NAME_PREFIX = "VALUES_DERIVED_TABLE";

  public static SqlParser TryParse(SqlTokenizer tokenizer, Dialect dialectProcessor) {
    StatementType type = StatementType.UNPARSED;
    SqlStatement stmt = null;
    try {
      tokenizer.MarkStart();
      SqlToken token = tokenizer.LookaheadToken2();
      while(true) {
        if (token.Kind == TokenKind.Comment) {
          tokenizer.NextToken();
          token = tokenizer.LookaheadToken2();
        } else {
          break;
        }
      }

      if (token.IsEmpty()) {
        SqlParser parser = new SqlParser(tokenizer.GetInputText(), null, type);
        parser.failedParseException = tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EMPTY_STATEMENT));
        return parser;
      }

      if (null != dialectProcessor) {
        stmt = dialectProcessor.Parse(tokenizer);
        if (stmt != null) {
          type = new SqlParser(stmt).GetStatementType();
          return new SqlParser(tokenizer.GetStatementText(), stmt, type);
        }
      }

      if (token.Equals("SELECT") || IsQueryClause(tokenizer)) {
        type = StatementType.SELECT;
        stmt = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
        if (stmt is SqlSelectIntoStatement) {
          type = StatementType.SELECTINTO;
        }
      } else if (token.Equals("DELETE")) {
        type = StatementType.DELETE;
        stmt = new SqlDeleteStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("INSERT")) {
        type = StatementType.INSERT;
        stmt = new SqlInsertStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("UPSERT")) {
        type = StatementType.UPSERT;
        stmt = new SqlUpsertStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("UPDATE")) {
        type = StatementType.UPDATE;
        stmt = new SqlUpdateStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("MERGE")) {
        type = StatementType.MERGE;
        stmt = new SqlMergeStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("CACHE")) {
        type = StatementType.CACHE;
        stmt = new SqlCacheStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("CHECKCACHE")) {
        type = StatementType.CHECKCACHE;
        stmt = new SqlCheckCacheStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("REPLICATE")) {
        type = StatementType.REPLICATE;
        stmt = new SqlReplicateStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("KILL")) {
        type = StatementType.KILL;
        stmt = new SqlKillStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("GETDELETED")) {
        type = StatementType.GETDELETED;
        stmt = new SqlGetDeletedStatement(tokenizer,dialectProcessor);

      } else if (token.Equals("METAQUERY")) {
        type = StatementType.METAQUERY;
        stmt = new SqlMetaQueryStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("MEMORYQUERY")) {
        type = StatementType.MEMORYQUERY;
        stmt = new SqlMemoryQueryStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("LOADMEMORYQUERY")) {
        type = StatementType.LOADMEMORYQUERY;
        stmt = new SqlLoadMemoryQueryStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("QUERYCACHE")) {
        type = StatementType.QUERYCACHE;
        stmt = new SqlQueryCacheStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("RESET")) {
        type = StatementType.RESET;
        stmt = new SqlResetStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("EXEC") || token.Equals("EXECUTE")) {
        type = StatementType.EXEC;
        stmt = new SqlExecStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("CALL")) {
        type = StatementType.CALL;
        stmt = new SqlCallSPStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("CREATE")) {
        type = StatementType.CREATE;
        stmt = new SqlCreateTableStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("DROP")) {
        type = StatementType.DROP;
        stmt = new SqlDropStatement(tokenizer, dialectProcessor);

      } else if (token.Equals("COMMIT")) {
        type = StatementType.COMMIT;
        stmt = new SqlCommitStatement(tokenizer, dialectProcessor);
      } else if (token.Equals("ALTER")) {
        tokenizer.NextToken();
        token = tokenizer.LookaheadToken2();
        if (token.Equals("TABLE")) {
          type = StatementType.ALTERTABLE;
          stmt = new SqlAlterTableStatement(tokenizer, dialectProcessor);
        } else {
          type = StatementType.UNKNOWN;
          throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNRECOGNIZED_KEYWORD, token.Value));
        }
      } else {
        type = StatementType.UNKNOWN;
        if (null != dialectProcessor) {
          stmt = dialectProcessor.Parse(tokenizer);
          if (stmt != null) return new SqlParser(tokenizer.GetStatementText(), stmt, type);
        } else {
          throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNRECOGNIZED_KEYWORD, token.Value));
        }
      }
      token = tokenizer.LookaheadToken2();
      if (token.Equals(SqlToken.None)) {
        tokenizer.NextToken();
      } else if (!token.IsEmpty()) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNEXPECTED_TOKEN, token.Value));
      }
    } catch (Exception ex) {
      SqlParser parser = new SqlParser(tokenizer.GetInputText(), null, type);
      parser.failedParseException = ex;
      return parser;
    }
    return new SqlParser(tokenizer.GetStatementText(), stmt, type);
  }

  public static SqlParser TryParseXml(string text, Dialect dialectProcessor) {
    int start = 0;
    while (start < text.Length && char.IsWhiteSpace(text[start])) {
      start++;
    }
    if (start < text.Length && text[start] == '<') { // A simple test for Updategrams should be good enough for now
      SqlXmlStatement xmlSql = new SqlXmlStatement(text, dialectProcessor);
      return new SqlParser(text, xmlSql, StatementType.XML);
    }
    return null;
  }

  public static bool ContainWildColumn(SqlCollection<SqlColumn> columns) {
    bool ret = false;
    if (columns != null) {
      foreach(SqlColumn col in columns) {
        if (col is SqlWildcardColumn) {
          ret = true;
          break;
        }
      }
    }
    return ret;
  }

  public static bool IsParameterExpression(SqlExpression expr) {
    if (expr is SqlValueExpression) {
      SqlValueExpression valExpr = (SqlValueExpression) expr;
      return valExpr.IsParameter();
    } else {
      return false;
    }
  }

  public static bool IsJoinStatement(SqlSelectStatement selectStatement) {
    bool isJoin = false;
    if (selectStatement.GetTables().Size() > 1) {
      isJoin = true;
    }

    if (selectStatement.GetJoins().Size() > 0) {
      isJoin = true;
    }
    return isJoin;
  }

  public static void ParseComment(SqlStatement stmt, SqlTokenizer tokenizer) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.Kind == TokenKind.Comment) {
      stmt.GetComments().Add(tryNext.Value);
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
    }
  }

  public static SqlCollection<SqlColumn> ParseColumns(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlCollection<SqlColumn> columns = new SqlCollection<SqlColumn>();
    JavaHashtable<string, string> alias2Name = new JavaHashtable<string, string>();
    SqlToken tryNext;
    do {
      SqlExpression column;
      if (tokenizer.LookaheadToken2().Equals("*")) {
        tokenizer.NextToken();
        column = new SqlWildcardColumn();
      } else {
        column = ReadExpression(tokenizer, stmt);
        tryNext = tokenizer.LookaheadToken2();
        string aliasName = null;
        bool hasAlias = false;
        if (tryNext.Equals("AS")) {
          hasAlias = true;
          tokenizer.NextToken();
        }

        if (TokenKind.Str == tryNext.Kind
            || TokenKind.Identifier == tryNext.Kind) {
          hasAlias = true;
        }

        if (hasAlias) {
          aliasName = tokenizer.NextToken().Value;
          if (column is SqlColumn) {
            if (((SqlColumn) column).GetTableName() != null) {
              string fullName = ((SqlColumn) column).GetFullName();
              alias2Name.Put(aliasName, fullName);
            } else {
              alias2Name.Put(aliasName, ((SqlColumn) column).GetColumnName());
            }

          }
        } else {
          if (column is SqlColumn) {
            string columnName = ((SqlColumn)column).GetAlias();
            int match = 0;
            if (columnName != null) {
              if (((SqlColumn) column).GetTableName() != null) {
                columnName = ((SqlColumn) column).GetFullName();
              }
              bool isExistAlias = alias2Name.ContainsKey(columnName);
              if (isExistAlias) {
                for (JavaEnumeration e = alias2Name.Keys(); e.HasMoreElements();) {
                  string existedAlias =  (string)e.NextElement();
                  string fullName = alias2Name.Get(existedAlias);
                  if (fullName.Equals(columnName)) {
                    match++;
                  }
                }
              }
            }
            if (match > 0) {
              aliasName = columnName + match;
              alias2Name.Put(aliasName, columnName);
            } else {
              alias2Name.Put(columnName, columnName);
            }
          }
        }

        if (column is SqlOperationExpression) {
          SqlOperationExpression opExpr = (SqlOperationExpression)column;
          if (!Utilities.IsNullOrEmpty(aliasName)) {
            column = new SqlOperationColumn(aliasName, opExpr);
          } else {
            column = new SqlOperationColumn(opExpr);
          }
        } else if (column is SqlValueExpression) {
          if (!Utilities.IsNullOrEmpty(aliasName)) {
            column = new SqlConstantColumn(aliasName, (SqlValueExpression)column);
          } else {
            column = new SqlConstantColumn((SqlValueExpression)column);
          }
        }  else if (column is SqlSubQueryExpression) {
          if (!Utilities.IsNullOrEmpty(aliasName)) {
            column = new SqlSubQueryColumn(aliasName, (SqlSubQueryExpression)column);
          } else {
            column = new SqlSubQueryColumn((SqlSubQueryExpression)column);
          }
        } else if (column is SqlFormulaColumn) {
          if (!Utilities.IsNullOrEmpty(aliasName)) {
            SqlFormulaColumn fcol = (SqlFormulaColumn) column;
            column = new SqlFormulaColumn(fcol.GetColumnName(),
                    aliasName,
                    fcol.GetParameters(),
                    fcol.GetOverClause());
          }
        } else if (column is SqlGeneralColumn && !(column is SqlWildcardColumn)) {
          if (!Utilities.IsNullOrEmpty(aliasName)) {
            SqlGeneralColumn col = (SqlGeneralColumn) column;
            column = new SqlGeneralColumn(col.GetTable(), col.GetColumnName(), aliasName, col.GetValueExpr());
          }
        }
      }
      if (column != null) {
        columns.Add((SqlColumn)column);
      }
      tryNext = tokenizer.LookaheadToken2();
      if (!tryNext.Equals(",")) {
        break;
      } else {
        tokenizer.NextToken();
      }
    } while (true);
    return columns;
  }

  public static bool IsQueryClause(SqlTokenizer tokenizer) {
    int start = tokenizer.CurrentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.Equals(SqlToken.Open.Value)) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
    }
    bool isQuery = tryNext.Equals("SELECT") || tryNext.Equals("FROM") || tryNext.Equals("WITH");
    tokenizer.Backtrack(start);
    return isQuery;
  }

  public static SqlExpression ReadExpression(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression expr = ReadAndExpr(tokenizer, stmt);
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.Equals("OR")) {
      tokenizer.NextToken();
      expr = new SqlCondition(expr, SqlLogicalOperator.Or, ReadAndExpr(tokenizer, stmt));
      tryNext = tokenizer.LookaheadToken2();
    }
    return expr;
  }

  public static SqlOutputClause ReadOutputClause(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlToken tN = tokenizer.LookaheadToken2();
    if (!tN.Equals("OUTPUT")) {
      return null;
    }
    tokenizer.NextToken();
    SqlCollection<SqlColumn> dml_select_list = ParseColumns(tokenizer, stmt);
    tN = tokenizer.LookaheadToken2();
    if (tN.Equals("INTO")) {
      tokenizer.NextToken();
      return new SqlOutputClause(dml_select_list, ReadExpression(tokenizer, stmt));
    } else {
      return new SqlOutputClause(dml_select_list);
    }
  }

  public static SqlOverClause ReadOverClause(SqlTokenizer tokenizer, SqlStatement stmt) {
    tokenizer.EnsureNextToken("OVER");
    tokenizer.EnsureNextToken(SqlToken.Open.Value);
    SqlOverClause overClause;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    JavaArrayList<SqlExpression> partition = new JavaArrayList<SqlExpression>();
    if (tryNext.Equals("PARTITION")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("BY");

      do {
        SqlExpression p = ReadExpression(tokenizer, stmt);
        partition.Add(p);
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
    }

    tryNext = tokenizer.LookaheadToken2();
    SqlCollection<SqlOrderSpec> orderBy = new SqlCollection<SqlOrderSpec>();
    if (tryNext.Equals("ORDER")) {
      SqlSelectStatement select = new SqlSelectStatement(null);
      ParseOrderBy(select, tokenizer);
      orderBy = select.GetOrderBy();
    }

    tryNext = tokenizer.LookaheadToken2();
    SqlFrameClause frameClause = null;
    if (tryNext.Equals("ROWS") || tryNext.Equals("RANGE")) {
      frameClause = ReadFrameClause(tokenizer, stmt);
    }

    overClause = new SqlOverClause(partition.ToArray(typeof(SqlExpression)),
            orderBy.ToArray(typeof(SqlOrderSpec)),
            frameClause);
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    return overClause;
  }

  private static SqlFrameClause ReadFrameClause(SqlTokenizer  tokenizer, SqlStatement stmt) {
    SqlFrameClause frame;
    WinFrameType frameType;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("ROWS")) {
      tokenizer.NextToken();
      frameType = WinFrameType.ROWS;
    } else {
      tokenizer.EnsureNextToken("RANGE");
      frameType = WinFrameType.RANGE;
    }
    tryNext = tokenizer.LookaheadToken2();
    WinFrameOption start;
    WinFrameOption end;
    if (tryNext.Equals("BETWEEN")) {
      tokenizer.NextToken();
      start = ReadFrameOptions(tokenizer, stmt);
      tokenizer.EnsureNextToken("AND");
      end = ReadFrameOptions(tokenizer, stmt);
    } else {
      start = ReadFrameOptions(tokenizer, stmt);
      end = null;
    }
    return new SqlFrameClause(frameType,
            start,
            end);
  }

  private static WinFrameOption ReadFrameOptions(SqlTokenizer tokenizer, SqlStatement stmt) {
    WinFrameOptionType type;
    SqlExpression expr = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("UNBOUNDED")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("PRECEDING")) {
        tokenizer.NextToken();
        type = WinFrameOptionType.UNBOUNDED_PRECEDING;
      } else {
        tokenizer.EnsureNextToken("FOLLOWING");
        type = WinFrameOptionType.UNBOUNDED_FOLLOWING;
      }
    } else if (tryNext.Equals("CURRENT")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("ROW");
      type = WinFrameOptionType.CURRENT_ROW;
    } else {
      expr = ReadExpression(tokenizer, stmt);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("PRECEDING")) {
        tokenizer.NextToken();
        type = WinFrameOptionType.PRECEDING;
      } else {
        tokenizer.EnsureNextToken("FOLLOWING");
        type = WinFrameOptionType.FOLLOWING;
      }
    }
    return new WinFrameOption(type, expr);
  }

  public static SqlConditionNode ParseWhere(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    SqlExpression cond = null;
    if (tryNext.Equals("WHERE")) {
      tokenizer.NextToken();
      cond = ReadExpression(tokenizer, stmt);
      if (cond is SqlValueExpression && cond.Evaluate().GetValueType() == SqlValueType.BOOLEAN){
        if (!cond.Evaluate().GetValueAsBool(true)) {
          cond = new SqlCriteria(null, ComparisonType.FALSE, null);
        }
      } else if (!(cond is SqlConditionNode)) {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        cond = new SqlCriteria(cond, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
      }
    }
    return (SqlConditionNode)cond;
  }

  public static string[] ParsePeriodTableName(SqlTokenizer tokenizer) {
    return ParsePeriodTableName(tokenizer, null);
  }

  public static string[] ParsePeriodTableName(SqlTokenizer tokenizer, Dialect dialect) {
    SqlCollection<string> periodNames = new SqlCollection<string>();
    SqlToken nextToken = tokenizer.NextToken();
    do {
      string v = null;
      if (dialect != null) {
        v = dialect.ParseIdentifierName(nextToken);
      }
      if (null == v) {
        v = nextToken.Value;
      }
      periodNames.Add(v);
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Dot) {
        tokenizer.NextToken();
        nextToken = tokenizer.NextToken();
      } else {
        break;
      }
    } while (true);


    string [] period;//catalog, schema, table
    if (periodNames.Size() > 3) {
      period = new string [periodNames.Size()];
    } else {
      period = new string [3];
    }

    if (1 == periodNames.Size()) {
      period[2] = periodNames.Get(0);
    } else if (2 == periodNames.Size()) {
      period[1] = periodNames.Get(0);
      period[2] = periodNames.Get(1);
    } else if (3 == periodNames.Size()) {
      period[0] = periodNames.Get(0);
      period[1] = periodNames.Get(1);
      period[2] = periodNames.Get(2);
    } else {
      period[2] = periodNames.Get(periodNames.Size() - 1);
      period[1] = periodNames.Get(periodNames.Size() - 2);
      period[0] = periodNames.Get(periodNames.Size() - 3);
      for (int i = 3 ; i < periodNames.Size(); ++i) {
        period[i] = periodNames.Get(i - 3);
      }
    }

    return period;
  }

  public static bool IsValidTableName(string name) {
    if (name == null || name.Equals("")) {
      return false;
    } else {
      if (name.StartsWith(DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX)
              || name.StartsWith(DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX)
              || name.StartsWith(DERIVED_VALUES_TABLE_NAME_PREFIX)) {
        return false;
      } else {
        return true;
      }
    }
  }

  public static void FlattenJoins(SqlCollection<SqlJoin> joins, SqlTable tableExpr) {
    if (tableExpr != null) {
      if (tableExpr.GetNestedJoin() != null) {
        SqlTable nestJoin = tableExpr.GetNestedJoin();
        FlattenJoins(joins, nestJoin);
      }
      if (tableExpr.GetJoin() != null) {
        joins.Add(tableExpr.GetJoin());
        FlattenJoins(joins, tableExpr.GetJoin().GetTable());
      }
    }
  }

  public static SqlQueryStatement ParseSelectUnion(SqlTokenizer tokenizer, Dialect dialectProcessor) {
    return ParseSelectUnion(tokenizer, dialectProcessor, 0);
  }

  public static SqlQueryStatement ParseSelectUnion(SqlTokenizer tokenizer, Dialect dialectProcessor, int parentParasNumber) {
    SqlQueryStatement query = ParserCore.ParseSelectSub(tokenizer, dialectProcessor, parentParasNumber);
    return ParseSelectUnionExtension(query, tokenizer, dialectProcessor);
  }

  public static void ParseGroupBy(SqlSelectStatement select, SqlTokenizer tokenizer) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("GROUP")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      bool eachGroupBy = false;
      if (tryNext.Equals("EACH")) {
        eachGroupBy = true;
        tokenizer.NextToken();
      }
      tokenizer.EnsureNextToken("BY");
      SqlCollection<SqlExpression> groupByList = new SqlCollection<SqlExpression>();
      do {
        SqlExpression expr = ParserCore.ReadExpression(tokenizer, select);
        if (expr is  SqlValueExpression && expr.Evaluate().GetValueType() == SqlValueType.NUMBER) {
          if (!ContainWildColumn(select.GetColumns())) {
            int position = int.Parse(expr.Evaluate().GetValueAsString(null));
            if (position < 0) {
              position = -position;
            }
            SqlColumn column = select.GetColumns().Get(position - 1);
            if (column is  SqlConstantColumn) {
              expr = column;
            } else  if (column.HasAlias()) {
              expr = new SqlGeneralColumn(column.GetAlias());
            } else {
              expr = column;
            }
          }
        }
        tryNext = tokenizer.LookaheadToken2();
        groupByList.Add(expr);
        if (tryNext.Equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
      select.SetGroupByClause(groupByList, eachGroupBy);
    }
  }

  public static void ParseOrderBy(SqlQueryStatement select, SqlTokenizer tokenizer) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("ORDER")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("BY");
      SqlCollection<SqlOrderSpec> orderList = new SqlCollection<SqlOrderSpec>();
      do {
        SqlExpression expr = ParserCore.ReadExpression(tokenizer, select);
        SortOrder type = SortOrder.Asc;
        if (expr is SqlValueExpression && expr.Evaluate().GetValueType() == SqlValueType.NUMBER) {
          if (!ContainWildColumn(select.GetColumns())) {
            int position = int.Parse(expr.Evaluate().GetValueAsString(null));
            if (position < 0) {
              type = SortOrder.Desc;
              position = -position;
            }
            SqlColumn column = select.GetColumns().Get(position - 1);
            if (column is SqlConstantColumn) {
              expr = column;
            } else if (column.HasAlias()) {
              expr = new SqlGeneralColumn(column.GetAlias());
            } else {
              expr = column;
            }
          }
        }
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("DESC")) {
          type = SortOrder.Desc;
          tokenizer.NextToken();
        } else if (tryNext.Equals("ASC")) {
          tokenizer.NextToken();
        }
        tryNext = tokenizer.LookaheadToken2();
        bool isNullsFirst = false;
        bool hasNulls = false;
        if (tryNext.Equals("NULLS")) {
          hasNulls = true;
          tokenizer.NextToken();
          tryNext = tokenizer.LookaheadToken2();
          if (tryNext.Equals("FIRST")) {
            tokenizer.NextToken();
            isNullsFirst = true;
          } else if (tryNext.Equals("LAST")) {
            tokenizer.NextToken();
            isNullsFirst = false;
          } else {
            throw SqlExceptions.Exception("parseOrderBy", SqlExceptions.EXPECTED_FIRST_LAST_AFTER_NULLS);
          }
          tryNext = tokenizer.LookaheadToken2();
        } else {
          if (type == SortOrder.Asc) {
            isNullsFirst = true;
          } else {
            isNullsFirst = false;
          }
        }
        SqlOrderSpec order = new SqlOrderSpec(expr, type, isNullsFirst, hasNulls);
        orderList.Add(order);
        if (tryNext.Equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
      select.SetOrderBy(orderList);
    }
  }

  public static void ParseColumnDefinitions(SqlTokenizer tokenizer,
                                            SqlCollection<SqlColumnDefinition> ColumnDefinitions,
                                            Dialect dialect) {
    tokenizer.EnsureNextToken(SqlToken.Open.Value);
    SqlToken nextToken = tokenizer.LookaheadToken2();
    while (true) {
      if (dialect != null) {
        SqlColumnDefinition COLUMN_DEF = dialect.ParseColumnDefinition(tokenizer);
        if (COLUMN_DEF != null) {
          ColumnDefinitions.Add(COLUMN_DEF);
          nextToken = tokenizer.LookaheadToken2();
          if(nextToken.Equals(",")){
            tokenizer.NextToken();
            nextToken = tokenizer.LookaheadToken2();
            continue;
          } else{
            break;
          }
        }
      }

      if(nextToken.Equals("PRIMARY")) {
        ParsePrimaryKey(tokenizer, ColumnDefinitions);
      } else if(nextToken.Equals("CONSTRAINT")) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNSUPPORTED_CONSTRAINT));
      } else{
        SqlColumnDefinition columnDefinition = ParseOneColumnDefinition(tokenizer, dialect);
        ColumnDefinitions.Add(columnDefinition);
      }
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.Equals(",")){
        tokenizer.NextToken();
        nextToken = tokenizer.LookaheadToken2();
      } else{
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    if(!tokenizer.NextToken().IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SYNTAX, tokenizer.NextToken().Value));
    }

    if(ColumnDefinitions.Size() <= 0){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_COL_DEF));
    }
  }

  public static SqlColumnDefinition ParseOneColumnDefinition(SqlTokenizer tokenizer, Dialect dialect) {
    SqlColumnDefinition newColumn = new SqlColumnDefinition();
    string columnName = null;
    SqlToken tk = tokenizer.NextToken();
    if (dialect != null) {
      columnName = dialect.ParseIdentifierName(tk);
    }

    if (null == columnName) {
      columnName = tk.Value;
    }

    string dataType = tokenizer.NextToken().Value;
    if(Utilities.IsNullOrEmpty(columnName)){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_COLNAME));
    }
    if(Utilities.IsNullOrEmpty(dataType)){
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_DATATYPE));
    }
    newColumn.ColumnName = columnName;
    newColumn.DataType = dataType;
    SqlToken nextToken = tokenizer.LookaheadToken2();
    if(nextToken.Equals("(")){
      tokenizer.NextToken();
      nextToken = tokenizer.NextToken();
      if(nextToken.IsEmpty()){
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_COLSIZE));
      }
      newColumn.ColumnSize = nextToken.Value;
      nextToken = tokenizer.LookaheadToken2();
      if (nextToken.Equals(",")) {
        tokenizer.NextToken();
        nextToken = tokenizer.NextToken();
        newColumn.Scale = nextToken.Value;
      }

      tokenizer.EnsureNextToken(")");
    }

    while(true){
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.Equals("NOT")){
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("NULL");
        newColumn.IsNullable = false;
      } else if(nextToken.Equals("PRIMARY")){
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("KEY");
        newColumn.IsKey = true;
      } else if(nextToken.Equals("AUTO_INCREMENT") || nextToken.Equals("AUTOINCREMENT")){
        newColumn.AutoIncrement = nextToken.Value;
        tokenizer.NextToken();
      } else if(nextToken.Equals("DEFAULT")) {
        tokenizer.NextToken();
        SqlToken token = tokenizer.NextToken();
        string DefaultValue;
        if (token.WasQuoted) {
          DefaultValue = "'" + token.Value + "'";
        } else {
          DefaultValue = token.Value;
        }
        newColumn.DefaultValue = DefaultValue;
      } else if(nextToken.Equals("UNIQUE")) {
        tokenizer.NextToken();
        newColumn.IsUnique = true;
      } else{
        break;
      }
    }
    return newColumn;
  }

  public static bool ParseIFEXISTS(SqlTokenizer tokenizer) {
    bool ifExists = false;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if(tryNext.Equals("IF")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("EXISTS");
      ifExists = true;
    }
    return ifExists;
  }

  private static void ParsePrimaryKey(SqlTokenizer tokenizer, SqlCollection<SqlColumnDefinition> ColumnDefinitions) {
    tokenizer.NextToken();
    tokenizer.EnsureNextToken("KEY");
    tokenizer.EnsureNextToken("(");
    while(true){
      SqlToken nextToken = tokenizer.NextToken();
      if(nextToken.IsEmpty()){
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_PRIMARY_KEY));
      }
      bool found = false;
      foreach(SqlColumnDefinition scd in ColumnDefinitions){
        if(scd.ColumnName.ToLower().Equals(nextToken.Value.ToLower())){
          scd.IsKey = true;
          found = true;
          break;
        }
      }
      if(!found){
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.NO_COL_DEF, nextToken.Value));
      }
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.Equals(",")){
        tokenizer.NextToken();
      } else if(nextToken.Equals(")")){
        tokenizer.NextToken();
        break;
      } else{
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.SYNTAX, nextToken.Value));
      }
    }
  }

  private static SqlQueryStatement ParseSelectSub(SqlTokenizer tokenizer, Dialect dialectProcessor, int parentParasNumber) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals("(")) {
      tokenizer.NextToken();
      SqlQueryStatement query = ParseSelectUnion(tokenizer, dialectProcessor, parentParasNumber);
      tokenizer.EnsureNextToken(")");
      return query;
    } else if(next.Equals("WITH")) {
      SqlQueryStatement query = ParseWithPart(dialectProcessor, tokenizer);
      return query;
    }
    SqlSelectStatement select = ParseSelectSimple(tokenizer, dialectProcessor, parentParasNumber);
    return select;
  }

  private static bool IsTableValuedFunction(SqlTokenizer tokenizer, Dialect dialectProcessor) {
    int start = tokenizer.CurrentPosition();
    SqlExpression expr = ReadExpression(tokenizer, new SqlSelectStatement(dialectProcessor));
    tokenizer.Backtrack(start);
    return expr is SqlFormulaColumn;
  }

  private static SqlExpression ReadTerm(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression expr = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    Dialect dialect = stmt.GetDialectProcessor();
    if (dialect != null) {
      SqlExpression term = dialect.ReadTerm(tokenizer);
      if (term != null) {
        if (IsParameterExpression(term)) {
          stmt.GetParameterList().Add((SqlValueExpression) term);
        }
        return term;
      }
    }
    if (tryNext.Kind == TokenKind.Open) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals(")")) {
        expr = new SqlExpressionList(new SqlExpression[0]);
      } else {
        SqlCollection<SqlExpression> list = new SqlCollection<SqlExpression>();
        do {
          expr = ReadExpression(tokenizer, stmt);
          list.Add(expr);
          tryNext = tokenizer.LookaheadToken2();
          if (tryNext.Equals(",")) {
            tokenizer.NextToken();
          } else {
            break;
          }
        } while (true);
        if (list.Size() > 1) {
          expr = new SqlExpressionList(list.ToArray(typeof(SqlExpression)));
        }
      }
      tokenizer.EnsureNextToken(")");
    } else if (tryNext.Kind == TokenKind.ESCInitiator) {
      if (IsFunctionEscape(tokenizer) || IsDateValueEscape(tokenizer)) {
        tokenizer.EnsureNextToken(SqlToken.ESCInitiator.Value);
        tokenizer.NextToken();
        expr = ReadExpression(tokenizer, stmt);
        if (expr is SqlFormulaColumn) {
          SqlFormulaColumn sourceFunction = (SqlFormulaColumn) expr;
          expr = new SqlFormulaColumn(SqlFormulaColumn.SCALAR_FUNCTION_NAME_PREFIX + sourceFunction.GetColumnName(),
                  sourceFunction.GetParameters(),
                  sourceFunction.GetOverClause());
        }
        tokenizer.EnsureNextToken(SqlToken.ESCTerminator.Value);
      }
    } else if (tryNext.Kind == TokenKind.Parameter) {
      tokenizer.NextToken();
      if (tryNext.Equals(SqlToken.AnonParam)) {
        expr = new SqlValueExpression("@" + (stmt.GetParentParasNumber() + stmt.GetParameterList().Size() + 1));
        stmt.GetParameterList().Add((SqlValueExpression) expr);
      } else {
        expr = new SqlValueExpression(tryNext.Value);
        if (!tryNext.Value.StartsWith("@@")) {
          stmt.GetParameterList().Add((SqlValueExpression) expr);
        }
      }
    } else if (tryNext.Kind == TokenKind.Keyword) {
      if (tryNext.Equals("SELECT")) {
        SqlQueryStatement query = ParseSelectUnion(tokenizer, stmt.GetDialectProcessor(), stmt.GetParameterList().Size());
        AddParas2Parent(stmt.GetParameterList(), query.GetParameterList());
        expr = new SqlSubQueryExpression(query);
      } else if (tryNext.Equals("CURRENT_TIME") && !tryNext.WasQuoted) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Kind == TokenKind.Open) {
          tokenizer.EnsureNextToken(SqlToken.Open.Value);
          SqlCollection<string> pn = new SqlCollection<string>();
          pn.Add("CURRENT_TIME");
          expr = ReadFunctionExpr(tokenizer, pn, stmt);
        } else {
          expr = new SqlFormulaColumn("CURRENT_TIME", new SqlCollection<SqlExpression>());
        }
      } else if (tryNext.Equals("CURRENT_TIMESTAMP") && !tryNext.WasQuoted) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Kind == TokenKind.Open) {
          tokenizer.EnsureNextToken(SqlToken.Open.Value);
          SqlCollection<string> pn = new SqlCollection<string>();
          pn.Add("CURRENT_TIMESTAMP");
          expr = ReadFunctionExpr(tokenizer, pn, stmt);
        } else {
          expr = new SqlFormulaColumn("CURRENT_TIMESTAMP", new SqlCollection<SqlExpression>());
        }
      }
    } else if (tryNext.Kind == TokenKind.Identifier &&
        (tryNext.Equals("TIMESTAMP")
            || tryNext.Equals("TIME")
            || tryNext.Equals("DATE"))) {
      SqlToken priorTK = tryNext;
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Open) {
        tokenizer.EnsureNextToken(SqlToken.Open.Value);
        SqlCollection<string> pn = new SqlCollection<string>();
        pn.Add(priorTK.Value);
        expr = ReadFunctionExpr(tokenizer, pn, stmt);
      } else if (tryNext.Kind == TokenKind.Str) {
        SqlToken v = tokenizer.NextToken();
        int dataType = ColumnInfo.DATA_TYPE_TIMESTAMP;
        if (priorTK.Equals("TIME")) {
          dataType = ColumnInfo.DATA_TYPE_TIME;
        } else if(priorTK.Equals("DATE")) {
          dataType = ColumnInfo.DATA_TYPE_DATE;
        }
        expr = new SqlValueExpression(new SqlValue(dataType, v.Value, SqlValue.DEFAULTDATETIMEFORMAT));
      } else {
        expr = new SqlGeneralColumn(tokenizer.LastToken().Value);
      }
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals(".")) {
        SqlCollection<string> periodNames = new SqlCollection<string>();
        periodNames.Add(tokenizer.LastToken().Value);
        tokenizer.NextToken();
        expr = ReadTermWithDot(tokenizer, periodNames, stmt);
      }
    } else if (tryNext.Kind == TokenKind.Identifier) {
      string name = tryNext.Value;
      SqlToken nameTK = tryNext;
      bool isQuoted = tryNext.WasQuoted;
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      SqlCollection<string> periodNames;
      if (tryNext.Equals(".")) {
        tokenizer.NextToken();
        periodNames = new SqlCollection<string>();
        periodNames.Add(name);
        expr = ReadTermWithDot(tokenizer, periodNames, stmt);
      } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(name, "CASE") && !isQuoted && !tryNext.Equals(SqlToken.Open)) {
        expr = ReadCase(tokenizer, stmt);
      } else if (tryNext.Equals(SqlToken.Open)) {
        tokenizer.NextToken();
        periodNames = new SqlCollection<string>();
        periodNames.Add(name);
        expr = ReadFunctionExpr(tokenizer, periodNames, stmt);
      } else {
        if (name.Equals("*")) {
          expr = new SqlWildcardColumn();
        } else {
          if (dialect != null) {
            string identifier = dialect.ParseIdentifierName(nameTK);
            if (Utilities.IsNullOrEmpty(identifier)) {
              identifier = name;
            }
            expr = new SqlGeneralColumn(identifier);
          } else {
            expr = new SqlGeneralColumn(name);
          }

        }
      }
    } else if (tryNext.Kind == TokenKind.Bool) {
      tokenizer.NextToken();
      expr = new SqlValueExpression(SqlValueType.BOOLEAN, tryNext.Value);
    } else if (tryNext.Kind == TokenKind.Number) {
      tokenizer.NextToken();
      expr = new SqlValueExpression(SqlValueType.NUMBER, tryNext.Value);
    } else if (tryNext.Kind == TokenKind.Str) {
      tokenizer.NextToken();
      expr = new SqlValueExpression(SqlValueType.STRING, tryNext.Value, System.Globalization.CultureInfo.CurrentCulture);
      tryNext = tokenizer.LookaheadToken2();
      if (TokenKind.Keyword == tryNext.Kind && tryNext.Equals("COLLATE")) {
        tokenizer.NextToken();
        tokenizer.NextToken();
      }
    } else if (tryNext.Kind == TokenKind.Null) {
      tokenizer.NextToken();
      expr = new SqlValueExpression(SqlValue.GetNullValueInstance());
    } else if (tryNext.Kind == TokenKind.Operator && tryNext.Equals("-")) {
      tokenizer.NextToken();
      expr = ReadTerm(tokenizer, stmt);
      SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
      paras.Add(expr);
      expr = new SqlFormulaColumn("NEGATE", paras);
    } else {
      tokenizer.NextToken();
    }
    return expr;
  }

  private static SqlExpression ReadTermWithDot(SqlTokenizer tokenizer,
                                               SqlCollection<string> periodNames,
                                               SqlStatement stmt) {
    Dialect dialect = stmt.GetDialectProcessor();
    SqlExpression expr = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.Kind == TokenKind.Identifier) {
      SqlToken token = tokenizer.NextToken();
      string identifier = token.Value;
      if (dialect != null) {
        identifier = dialect.ParseIdentifierName(token);
      }
      if (identifier != null) {
        periodNames.Add(identifier);
      } else {
        periodNames.Add(token.Value);
      }

      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals(SqlToken.Dot)) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
      } else {
        break;
      }
    }
    if (tryNext.Equals(SqlToken.Open)) {
      tokenizer.NextToken();
      expr = ReadFunctionExpr(tokenizer, periodNames, stmt);
    } else {
      string [] period = ParsePeriodColumnName(periodNames);
      if (Utilities.EqualIgnoreCase("*", period[3])) {
        expr = new SqlWildcardColumn(new SqlTable(period[0], period[1], period[2]));
      } else {
        expr = new SqlGeneralColumn(new SqlTable(period[0], period[1], period[2]), period[3]);
      }
    }
    return expr;
  }

  private static string[] ParsePeriodColumnName(SqlCollection<string> periodNames) {
    string [] period = new string [] {null, null, null, null};//catalog, schema, table, column
    if (periodNames != null && periodNames.Size() > 0) {
      if (1 == periodNames.Size()) {
        period[3] = periodNames.Get(0);
      } else if (2 == periodNames.Size()) {
        period[2] = periodNames.Get(0);
        period[3] = periodNames.Get(1);
      } else if (3 == periodNames.Size()) {
        period[1] = periodNames.Get(0);
        period[2] = periodNames.Get(1);
        period[3] = periodNames.Get(2);
      } else if (4 == periodNames.Size()) {
        period[0] = periodNames.Get(0);
        period[1] = periodNames.Get(1);
        period[2] = periodNames.Get(2);
        period[3] = periodNames.Get(3);
      } else{
        period[3] = periodNames.Get(periodNames.Size() - 1);
        period[2] = periodNames.Get(periodNames.Size() - 2);
        period[1] = periodNames.Get(periodNames.Size() - 3);
        period[0] = periodNames.Get(periodNames.Size() - 4);
      }
    }
    return period;
  }

  private static SqlExpression ReadCase(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlFormulaColumn caseWhen = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
    if (tryNext.Equals("WHEN")) {
      tokenizer.NextToken();
      paras.Add(null);
      do {
        paras.Add(ReadExpression(tokenizer, stmt));
        tokenizer.EnsureNextToken("THEN");
        paras.Add(ReadExpression(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("WHEN")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while(true);
    } else {
      paras.Add(ReadExpression(tokenizer, stmt));
      tokenizer.EnsureNextToken("WHEN");
      do {
        paras.Add(ReadExpression(tokenizer, stmt));
        tokenizer.EnsureNextToken("THEN");
        paras.Add(ReadExpression(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("WHEN")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
    }
    if (tryNext.Equals("ELSE")) {
      tokenizer.NextToken();
      paras.Add(ReadExpression(tokenizer, stmt));
    }
    tokenizer.EnsureNextToken("END");
    caseWhen = new SqlFormulaColumn("CASE", paras);
    return caseWhen;
  }

  private static SqlExpression ReadFunctionExpr(SqlTokenizer tokenizer, SqlCollection<string> periodName, SqlStatement stmt) {
    SqlToken nextTry;
    string name = periodName.Get(periodName.Size() - 1);
    SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
    do {
      SqlExpression para;
      if (Utilities.EqualIgnoreCase("CAST", name)) {
        paras.Add(ReadExpression(tokenizer, stmt));
        tokenizer.EnsureNextToken("AS");
        para = ReadExpression(tokenizer, stmt);
        if (para is SqlGeneralColumn) {
          paras.Add(new SqlValueExpression(SqlValueType.STRING, ((SqlGeneralColumn) para).GetFullName()));
        } else if (para is SqlFormulaColumn) {
          SqlFormulaColumn typeDef = (SqlFormulaColumn)para;
          paras.Add(new SqlValueExpression(SqlValueType.STRING, typeDef.GetColumnName()));
          foreach(SqlExpression expr in typeDef.GetParameters()) {
            paras.Add(expr);
          }
        }
      } else if (SqlUtilities.IsKnownAggragation(name)) {
        nextTry = tokenizer.LookaheadToken2();
        if (nextTry.Equals("DISTINCT") && nextTry.Kind == TokenKind.Keyword) {
          tokenizer.NextToken();
          SqlCollection<SqlExpression> distinctPara = new SqlCollection<SqlExpression>();
          distinctPara.Add(ReadExpression(tokenizer, stmt));
          paras.Add(new SqlFormulaColumn("DISTINCT", distinctPara));
        } else {
          nextTry = tokenizer.LookaheadToken2();
          if (nextTry.Equals("*")) {
            tokenizer.NextToken();
            paras.Add(new SqlWildcardColumn());
          } else {
            paras.Add(ReadExpression(tokenizer, stmt));
          }
        }
      } else if (Utilities.EqualIgnoreCase("TOSTRING", name)) {
        if (periodName.Size() > 1) {
          paras.Add(new SqlGeneralColumn(periodName.Get(periodName.Size() - 2)));
        } else {
          paras.Add(ReadExpression(tokenizer, stmt));
        }
      } else if (Utilities.EqualIgnoreCase("CONCAT", name)) {
        if (periodName.Size() > 1) {
          paras.Add(new SqlGeneralColumn(periodName.Get(periodName.Size() - 2)));
          paras.Add(ReadExpression(tokenizer, stmt));
        } else {
          paras.Add(ReadExpression(tokenizer, stmt));
        }
      } else if (Utilities.EqualIgnoreCase("EXTRACT", name)) {
        SqlToken part = tokenizer.NextToken();
        paras.Add(new SqlValueExpression(SqlValueType.STRING, part.Value));
        tokenizer.EnsureNextToken("FROM");

        SqlExpression source = ReadExpression(tokenizer, stmt);

        if (source is SqlValueExpression) {
          SqlValue dtValue = source.Evaluate();
          if (dtValue.GetDataType() == ColumnInfo.DATA_TYPE_TIMESTAMP) {
            paras.Add(new SqlValueExpression(SqlValueType.STRING, "TIMESTAMP"));
          } else if (dtValue.GetDataType() == ColumnInfo.DATA_TYPE_DATE) {
            paras.Add(new SqlValueExpression(SqlValueType.STRING, "DATE"));
          } else if (dtValue.GetDataType() == ColumnInfo.DATA_TYPE_TIME) {
            paras.Add(new SqlValueExpression(SqlValueType.STRING, "TIME"));
          }
          paras.Add(new SqlValueExpression(SqlValueType.DATETIME, "" + dtValue.GetOriginalValue()));
        } else {
          paras.Add(source);
        }

        nextTry = tokenizer.LookaheadToken2();
        if (nextTry.Equals("AT")) {
          tokenizer.NextToken();
          nextTry = tokenizer.LookaheadToken2();
          if (nextTry.Equals("LOCAL")) {
            tokenizer.NextToken();
            paras.Add(new SqlValueExpression(SqlValueType.STRING, "AT LOCAL"));
          } else {
            tokenizer.EnsureNextToken("TIME");
            tokenizer.EnsureNextToken("ZONE");
            SqlToken tz_spec = tokenizer.NextToken();
            paras.Add(new SqlValueExpression(SqlValueType.STRING, "AT TIME ZONE '" + tz_spec.Value + "'"));
          }
        }
      } else {
        nextTry = tokenizer.LookaheadToken2();
        if (!nextTry.Equals(SqlToken.Close)) {
          paras.Add(ReadExpression(tokenizer, stmt));
        }
      }
      nextTry = tokenizer.LookaheadToken2();
      if (nextTry.Equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    } while (true);
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    nextTry = tokenizer.LookaheadToken2();
    SqlFormulaColumn func;
    if (nextTry.Equals("OVER")) {
      SqlOverClause overClause = ReadOverClause(tokenizer, stmt);
      func = new SqlFormulaColumn(name, paras, overClause);
    } else {
      func = new SqlFormulaColumn(name, paras);
    }
    return func;
  }

  private static SqlExpression ReadAndExpr(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression r = ReadConditionExpr(tokenizer, stmt);
    while (tokenizer.LookaheadToken2().Equals("AND")) {
      tokenizer.NextToken();
      r = new SqlCondition(r, SqlLogicalOperator.And, ReadConditionExpr(tokenizer, stmt));
    }
    return r;
  }

  private static SqlExpression ReadConditionExpr(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression expr;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("NOT") && tryNext.Kind == TokenKind.Keyword) {
      tokenizer.NextToken();
      expr = new SqlConditionNot(ReadConditionExpr(tokenizer, stmt));
      return expr;
    } else if (tryNext.Equals("EXISTS") && tryNext.Kind == TokenKind.Keyword) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("(");
      SqlQueryStatement query = ParseSelectUnion(tokenizer, stmt.GetDialectProcessor(), stmt.GetParameterList().Size());
      tokenizer.EnsureNextToken(")");
      AddParas2Parent(stmt.GetParameterList(), query.GetParameterList());
      expr = new SqlConditionExists(query);
      return expr;
    }

    expr = ReadOperandExpr(tokenizer, stmt);
    tryNext = tokenizer.LookaheadToken2();

    while (true) {
      bool hasNot = false;
      if (tryNext.Equals("NOT")) {
        hasNot = true;
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
      }
      if (tryNext.Equals("LIKE")) {
        tokenizer.NextToken();
        SqlExpression right = ReadOperandExpr(tokenizer, stmt);
        tryNext = tokenizer.LookaheadToken2();
        bool isODBCLikeEscape = false;
        if (tryNext.Equals(SqlToken.ESCInitiator)) {
          isODBCLikeEscape = IsLikeEscape(tokenizer);
          tokenizer.NextToken();
          tryNext = tokenizer.LookaheadToken2();
        }
        if (tryNext.Equals("ESCAPE")) {
          tokenizer.NextToken();
          SqlExpression escape = ReadOperandExpr(tokenizer, stmt);
          expr = new SqlCriteria(expr, right, escape);
        } else {
          expr = new SqlCriteria(expr, ComparisonType.LIKE, right);
        }
        if (isODBCLikeEscape) {
          tokenizer.EnsureNextToken(SqlToken.ESCTerminator.Value);
        }
      } else if (tryNext.Equals("IS")) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("NOT")) {
          tokenizer.NextToken();
          SqlExpression right = ReadOperandExpr(tokenizer, stmt);
          expr = new SqlCriteria(expr, ComparisonType.IS_NOT, right);
        } else {
          SqlExpression right = ReadOperandExpr(tokenizer, stmt);
          expr = new SqlCriteria(expr, ComparisonType.IS, right);
        }
      } else if (tryNext.Equals("IN")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("(");
        if (IsQueryClause(tokenizer)) {
          SqlQueryStatement query = ParseSelectUnion(tokenizer, stmt.GetDialectProcessor(), stmt.GetParameterList().Size());
          expr = new SqlConditionInSelect(expr, query, false, ComparisonType.EQUAL);
          AddParas2Parent(stmt.GetParameterList(), query.GetParameterList());
        } else {
          SqlCollection<SqlExpression> values = new SqlCollection<SqlExpression>();
          do {
            values.Add(ReadExpression(tokenizer, stmt));
            tryNext = tokenizer.LookaheadToken2();
            if (tryNext.Equals(",")) {
              tokenizer.NextToken();
            } else {
              break;
            }
          } while (true);

          ComparisonType type = ComparisonType.IN;
          if (hasNot) {
            hasNot = false;
            type = ComparisonType.NOT_IN;
          }
          expr = new SqlCriteria(expr, type, new SqlExpressionList(values.ToArray(typeof(SqlExpression))));
        }
        tokenizer.EnsureNextToken(")");
      } else if (tryNext.Equals("BETWEEN")) {
        tokenizer.NextToken();
        SqlExpression low = ReadOperandExpr(tokenizer, stmt);
        tokenizer.EnsureNextToken("AND");
        SqlExpression high = ReadOperandExpr(tokenizer, stmt);
        SqlCriteria conLow = new SqlCriteria(expr, ComparisonType.BIGGER_EQUAL, low);
        SqlCriteria conHigh = new SqlCriteria(expr, ComparisonType.SMALLER_EQUAL, high);
        expr = new SqlCondition(conLow, SqlLogicalOperator.And, conHigh);
      } else {
        ComparisonType type = SqlCriteria.GetCompareType(tryNext.Value);
        string customOp = null;
        if (type == ComparisonType.NONE) {
          if (stmt.GetDialectProcessor() != null) {
            customOp = stmt.GetDialectProcessor().GetCustomCompareOp(tokenizer);
            if (customOp != null) {
              type = ComparisonType.CUSTOM;
            }
          }
        }
        if (type == ComparisonType.NONE) {
          break;
        } else {
          tokenizer.NextToken();
          tryNext = tokenizer.LookaheadToken2();
          bool isAll = false;
          bool isComparsionSelect = false;
          if (tryNext.IsKeyword("ALL")) {
            isAll = true;
            isComparsionSelect = true;
          } else if (tryNext.IsKeyword("SOME") || tryNext.IsKeyword("ANY")) {
            isAll = false;
            isComparsionSelect = true;
          }
          if (isComparsionSelect) {
            tokenizer.NextToken();
            tokenizer.EnsureNextToken("(");
            SqlQueryStatement query = ParseSelectUnion(tokenizer, stmt.GetDialectProcessor(), stmt.GetParameterList().Size());
            AddParas2Parent(stmt.GetParameterList(), query.GetParameterList());
            expr = new SqlConditionInSelect(expr, query, isAll, type);
            tokenizer.EnsureNextToken(")");
          } else {
            SqlExpression right = ReadOperandExpr(tokenizer, stmt);
            if (type == ComparisonType.CUSTOM) {
              expr = new SqlCriteria(expr, customOp, right);
            } else {
              expr = new SqlCriteria(expr, type, right);
            }
          }
        }
      }
      if (hasNot) {
        expr = new SqlConditionNot(expr);
      }
      tryNext = tokenizer.LookaheadToken2();
    }
    return expr;
  }

  private static SqlExpression ReadOperandExpr(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression sum = ReadSumExpr(tokenizer, stmt);
    SqlToken next = tokenizer.LookaheadToken2();
    while (next.Equals("||")) {
      tokenizer.NextToken();
      SqlExpression sum2 = ReadSumExpr(tokenizer, stmt);
      sum = new SqlOperationExpression(SqlOperationType.CONCAT, sum, sum2);
      next = tokenizer.LookaheadToken2();
    }
    return sum;
  }

  private static SqlExpression ReadSumExpr(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression factor = ReadFactorExpr(tokenizer, stmt);
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (true) {
      if (tryNext.Equals("+")) {
        tokenizer.NextToken();
        factor = new SqlOperationExpression(SqlOperationType.PLUS, factor, ReadFactorExpr(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
      } else if (tryNext.Equals("-")) {
        tokenizer.NextToken();
        factor = new SqlOperationExpression(SqlOperationType.MINUS, factor, ReadFactorExpr(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
      } else if (tryNext.Kind == TokenKind.Number && tryNext.Value.StartsWith("-")) {
        //column-1
        tokenizer.NextToken();
        string signValue = tryNext.Value;
        string v = RSSBus.core.j2cs.Converter.GetSubstring(signValue, 1, tryNext.Value.Length);
        SqlValueExpression vExpr = new SqlValueExpression(SqlValueType.NUMBER, v);
        factor = new SqlOperationExpression(SqlOperationType.MINUS, factor, vExpr);
        tryNext = tokenizer.LookaheadToken2();
      } else {
        return factor;
      }
    }
  }

  private static SqlExpression ReadFactorExpr(SqlTokenizer tokenizer, SqlStatement stmt) {
    SqlExpression r = ReadTerm(tokenizer, stmt);
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (true) {
      if (tryNext.Equals("*")) {
        tokenizer.NextToken();
        r = new SqlOperationExpression(SqlOperationType.MULTIPLY, r, ReadTerm(tokenizer, stmt));
      } else if (tryNext.Equals("/")) {
        tokenizer.NextToken();
        r = new SqlOperationExpression(SqlOperationType.DIVIDE, r, ReadTerm(tokenizer, stmt));
      } else if (tryNext.Equals("%")) {
        tokenizer.NextToken();
        r = new SqlOperationExpression(SqlOperationType.MODULUS, r, ReadTerm(tokenizer, stmt));
      } else {
        return r;
      }
      tryNext = tokenizer.LookaheadToken2();
    }
  }

  private static bool Is_SCOPE_IDENTITY(SqlCollection<SqlColumn> columns) {
    if (1 == columns.Size()) {
      string name = columns.Get(0).GetColumnName();
      if (name != null) {
        if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(name, "LAST_INSERT_ID") ||
           RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(name, "SCOPE_IDENTITY")) {
          return true;
        }
      }
    }
    return false;
  }

  private static bool Is_ALL_CONSTANT_COLUMNS(SqlCollection<SqlColumn> columns) {
    bool result = true;
    foreach(SqlColumn c in columns) {
      if (!(c is SqlConstantColumn)) {
        result = false;
        break;
      }
    }
    return result;
  }

  private static SqlSelectStatement ParseSelectSimple(SqlTokenizer tokenizer, Dialect dialectProcessor, int parentParasNumber) {
    bool fromFirst = false;
    bool withFirst = false;
    bool isSelectInto = false;
    SqlValueExpression externalDatabase = null;
    SqlTable intoTable = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("SELECT")) {
      tokenizer.NextToken();
      fromFirst = false;
    } else if(tryNext.Equals("WITH")) {
      withFirst = true;
    } else {
      tokenizer.EnsureNextToken("FROM");
      fromFirst = true;
    }
    SqlSelectStatement select = new SqlSelectStatement(dialectProcessor, parentParasNumber);
    if (fromFirst) {
      ParseSelectSimpleFromPart(dialectProcessor, select, tokenizer);
      tokenizer.EnsureNextToken("SELECT");
      ParseSelectSimpleSelectPart(select, tokenizer);
    } else if(withFirst) {
      ParseSimpleWithPart(select, tokenizer);
    } else {
      ParseSelectSimpleSelectPart(select, tokenizer);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("FROM")) {
        tokenizer.NextToken();
        ParseSelectSimpleFromPart(dialectProcessor, select, tokenizer);
      } else if (tryNext.Equals("INTO")) {
        isSelectInto = true;
        tokenizer.NextToken();
        string [] period = ParsePeriodTableName(tokenizer, dialectProcessor);
        string intoTableName = period[2];
        intoTable = new SqlTable(period[0], period[1], intoTableName, intoTableName);
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("IN")) {
          tokenizer.NextToken();
          SqlToken token = tokenizer.NextToken();
          externalDatabase = new SqlValueExpression(SqlValueType.STRING, token.Value);
        }
        tokenizer.EnsureNextToken("FROM");
        ParseSelectSimpleFromPart(dialectProcessor, select, tokenizer);
      } else if (!tryNext.IsEmpty()) {
        if (Is_SCOPE_IDENTITY(select.GetColumns())
            || Is_ALL_CONSTANT_COLUMNS(select.GetColumns())) {
          return select;
        } else if (!tryNext.Equals("FROM")){
          return select;
        } else {
          tokenizer.EnsureNextToken("FROM");
        }
      }
    }
    select.AddCondition(ParseWhere(tokenizer, select));
    ParseGroupBy(select, tokenizer);
    ParseHaving(select, tokenizer);
    if (isSelectInto) {
      select = new SqlSelectIntoStatement(intoTable,
              externalDatabase,
              select.GetColumns(),
              select.GetHavingClause(),
              select.GetCriteria(),
              select.GetOrderBy(),
              select.GetGroupBy(),
              select.GetEachGroupBy(),
              select.GetTables(),
              select.GetParameterList(),
              select.GetFromLast(),
              select.GetLimitExpr(),
              select.GetOffsetExpr(),
              select.IsDistinct(),
              select.GetDialectProcessor());
    }
    return select;
  }

  private static void ParseSimpleWithPart(SqlSelectStatement select, SqlTokenizer tokenizer) {
    tokenizer.EnsureNextToken("WITH");
    SqlCollection<SqlTable> withes = new SqlCollection<SqlTable>();
    ParseWithClause(tokenizer, select.GetDialectProcessor(), withes);
    select.SetWithClause(withes);

    SqlToken tryNext = tokenizer.LookaheadToken2();
    if(tryNext.Equals("SELECT")) {
      SqlQueryStatement query = ParseSelectUnion(tokenizer, select.GetDialectProcessor());

      if(query is SqlSelectStatement) {
        SqlSelectStatement sub = (SqlSelectStatement) query;
        SqlCollection<SqlTable> tables = sub.GetTables();

        foreach(SqlTable t in tables) {
          select.AddTable(t);
        }
        select.SetOrderBy(sub.GetOrderBy());
        select.SetColumns(sub.GetColumns());
        select.SetDistinct(sub.IsDistinct());
        select.SetFromLast(sub.GetFromLast());
        select.SetCriteria(sub.GetCriteria());
        select.SetLimitExpr(sub.GetLimitExpr());
        select.SetOffsetExpr(sub.GetOffsetExpr());
        select.SetHavingClause(sub.GetHavingClause());
        select.SetGroupByClause(sub.GetGroupBy(), sub.GetEachGroupBy());
      }
    } else {
      // CTE define must be used.
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNEXPECTED_TOKEN, tryNext.Value));
    }
  }

  private static SqlQueryStatement ParseWithPart(Dialect dialect, SqlTokenizer tokenizer) {
    tokenizer.EnsureNextToken("WITH");
    SqlCollection<SqlTable> withes = new SqlCollection<SqlTable>();
    ParseWithClause(tokenizer, dialect, withes);

    SqlToken tryNext = tokenizer.LookaheadToken2();
    if(tryNext.Equals("SELECT")) {
      SqlQueryStatement query = ParseSelectUnion(tokenizer, dialect);
      query.SetWithClause(withes);
      return query;
    } else {
      // CTE define must be used.
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNEXPECTED_TOKEN, tryNext.Value));
    }
  }

  private static void ParseSelectSimpleFromPart(Dialect dialect, SqlSelectStatement select, SqlTokenizer tokenizer) {
    ParseTableReferences(dialect, select, tokenizer);
  }

  public static void ParseTableReferences(Dialect dialect, SqlSelectStatement select, SqlTokenizer tokenizer) {
    do {
      SqlTable table = ReadTable(select, tokenizer, dialect);
      ParseJoinTable(table, select, tokenizer, dialect);
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if (!tryNext.Equals(",")) {
        break;
      } else {
        tokenizer.NextToken();
      }
    } while (true);
  }

  private static void ParseJoinTable(SqlTable top, SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) {
    top = ReadJoinTable(top, select, tokenizer, dialect);
    if (null == select.GetTable()) {
      select.SetTable(top);
      select.SetTableName(top.GetName());
    } else {
      select.AddTable(top);
    }
  }

  private static SqlTable ReadTable(SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) {
    SqlTable table = null;
    string tableName = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals(SqlToken.Open)) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("SELECT") || tryNext.Equals("WITH")) {
        SqlQueryStatement query = ParseSelectUnion(tokenizer, select.GetDialectProcessor(), select.GetParameterList().Size());
        AddParas2Parent(select.GetParameterList(), query.GetParameterList());
        tableName = DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX + tokenizer.CurrentPosition();
        table = new SqlTable(query, tableName, null);
      } else {
        //nested join SELECT * FROM (TableA A LEFT JOIN TableB B) LEFT JOIN TableC C
        table = ReadTable(select, tokenizer, dialect);
        table = ReadCrossApplyTable(table, select, tokenizer, dialect);
        table = ReadJoinTable(table, select, tokenizer, dialect);
        if (table.GetJoin() != null || table.GetQuery() != null || table.GetNestedJoin() != null) {
          table = new SqlTable(null, null, DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX + tokenizer.CurrentPosition(), null, table);
        }
      }
      tableName = table.GetName();
      tokenizer.EnsureNextToken(SqlToken.Close.Value);
    } else if (tryNext.Equals(SqlToken.ESCInitiator)) {
      //support parsing odbc join style {oj ...}
      //http://msdn.microsoft.com/en-us/library/ms714641%28v=vs.85%29.aspx
      tokenizer.NextToken();
      tokenizer.EnsureNextIdentifier("oj");
      table = ReadTable(select, tokenizer, dialect);
      table = ReadCrossApplyTable(table, select, tokenizer, dialect);
      table = ReadJoinTable(table, select, tokenizer, dialect);
      tokenizer.EnsureNextToken(SqlToken.ESCTerminator.Value);
      return table;
    } else if (tryNext.Equals("VALUES")) {
      //SELECT * FROM (VALUES(1,1),(2,2),(3,3)) AS MyTable(a, b);
      tokenizer.NextToken();
      SqlCollection<SqlExpression> list = new SqlCollection<SqlExpression>();
      do {
        SqlExpression expr = ReadExpression(tokenizer, select);
        list.Add(expr);
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
      tableName = DERIVED_VALUES_TABLE_NAME_PREFIX;
      table = new SqlTable(tableName, tableName);
    } else if (IsTableValuedFunction(tokenizer, select.GetDialectProcessor())) {
      SqlFormulaColumn tableValueFunc = (SqlFormulaColumn)ReadExpression(tokenizer, select);
      tableName = tableValueFunc.GetColumnName();
      table = new SqlTable(tableValueFunc);
    } else {
      if (tryNext == null || tryNext.OpenQuote == "\'" || tryNext.CloseQuote == "\'") {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_TABLENAME_BEFORE_WHERE));
      }
      string [] period = ParsePeriodTableName(tokenizer, select.GetDialectProcessor());
      tableName = period[2];
      if (period.Length > 3) {
        ByteBuffer sb = new ByteBuffer();
        for (int i = 3 ; i < period.Length; ++i) {
          sb.Append(period[i]).Append(SqlToken.Dot.Value);
        }
        sb.Append(period[0]);
        table = new SqlTable(sb.ToString(), period[1], tableName, tableName);
      } else {
        table = new SqlTable(period[0], period[1], tableName, tableName);
      }

    }
    string tableAlias = ReadFromAlias(tokenizer, tableName, select);
    table = new SqlTable(table.GetCatalog(), table.GetSchema(), table.GetName(), tableAlias, table.GetJoin(), table.GetNestedJoin(), table.GetQuery(), table.GetTableValueFunction(), table.GetCrossApply());
    table = ReadCrossApplyTable(table, select, tokenizer, dialect);
    return table;
  }

  private static string ReadFromAlias(SqlTokenizer tokenizer, string alias, SqlStatement stmt) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("AS")) {
      tokenizer.NextToken();
      int pos = tokenizer.CurrentPosition();
      SqlExpression expr = ReadExpression(tokenizer, stmt);
      if (expr is SqlFormulaColumn) {
        alias = ((SqlFormulaColumn) expr).GetColumnName();
      } else {
        tokenizer.Backtrack(pos);
        alias = tokenizer.NextToken().Value;
      }
    } else if (tryNext.Kind == TokenKind.Identifier){
      if (!tryNext.IsEmpty() && !tryNext.Equals("LEFT") && !tryNext.Equals("RIGHT") && !tryNext.Equals("FULL")) {
        SqlExpression expr = ReadExpression(tokenizer, stmt);
        if (expr is SqlColumn) {
          alias = ((SqlColumn) expr).GetColumnName();
        }
      }
    }
    return alias;
  }

  private static JoinType GetJoinType(string join) {
    JoinType type;
    if (Utilities.EqualIgnoreCase("LEFT", join)) {
      type = JoinType.LEFT;
    } else if (Utilities.EqualIgnoreCase("RIGHT", join)) {
      type = JoinType.RIGHT;
    } else if (Utilities.EqualIgnoreCase("FULL", join)) {
      type = JoinType.FULL;
    } else if (Utilities.EqualIgnoreCase("INNER", join)) {
      type = JoinType.INNER;
    } else if (Utilities.EqualIgnoreCase("JOIN", join)) {
      type = JoinType.INNER;
    } else if (Utilities.EqualIgnoreCase("NATURAL", join)) {
      type = JoinType.NATURAL;
    } else if (Utilities.EqualIgnoreCase("CROSS", join)) {
      type = JoinType.CROSS;
    } else {
      type = JoinType.NONE;
    }
    return type;
  }

  private static SqlTable ReadCrossApplyTable(SqlTable table, SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) {
    SqlCrossApply ca = ReadCrossApply(tokenizer, select, dialect);
    if ( ca != null ) {
      return new SqlTable(table.GetCatalog(), table.GetSchema(), table.GetName(), table.GetAlias(), table.GetJoin(), table.GetNestedJoin(), table.GetQuery(), table.GetTableValueFunction(), ca);
    }
    return table;
  }

  private static SqlCrossApply ReadCrossApply(SqlTokenizer tokenizer, SqlSelectStatement select, Dialect dialect) {
    int start = tokenizer.CurrentPosition();
    SqlToken next = tokenizer.LookaheadToken2();
    if (!Utilities.EqualIgnoreCase("CROSS", next.Value))
      return null;

    tokenizer.NextToken();
    next = tokenizer.NextToken();
    if (!Utilities.EqualIgnoreCase("APPLY", next.Value)) {
      tokenizer.Backtrack(start);
      return null;
    }

    // FUNCTION([alias.]ColumnName)
    if ( !IsTableValuedFunction(tokenizer, dialect) ) {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.CROSS_APPLY_INVALID_EXPRESSION));
    }
    SqlFormulaColumn function = (SqlFormulaColumn)ReadExpression(tokenizer, select);
    SqlCollection<SqlColumnDefinition> columnDefinitions = new SqlCollection<SqlColumnDefinition>();

    // columns definitions
    next = tokenizer.LookaheadToken2();
    if ( next.IsKeyword("WITH") ) {
      tokenizer.NextToken();
      ParseCrossApplyColumnDefinitions(tokenizer, columnDefinitions, dialect);
    }

    string alias = ReadFromAlias(tokenizer, "", select);

    SqlCrossApply child = ReadCrossApply(tokenizer, select, dialect);
    SqlCrossApply ca = new SqlCrossApply(function, alias, columnDefinitions, child);
    return ca;
  }

  private static void ParseCrossApplyColumnDefinitions(SqlTokenizer tokenizer, SqlCollection<SqlColumnDefinition> definitions, Dialect dialect) {
    tokenizer.EnsureNextToken(SqlToken.Open.Value);
    SqlToken nextToken = tokenizer.LookaheadToken2();
    while (!nextToken.Equals(SqlToken.Close)) {
      if (dialect != null) {
        SqlColumnDefinition COLUMN_DEF = dialect.ParseColumnDefinition(tokenizer);
        if (COLUMN_DEF != null) {
          definitions.Add(COLUMN_DEF);
          nextToken = tokenizer.LookaheadToken2();
          if(nextToken.Equals(",")){
            tokenizer.NextToken();
            nextToken = tokenizer.LookaheadToken2();
            continue;
          } else{
            break;
          }
        }
      }

      SqlColumnDefinition columnDefinition = ParseOneColumnDefinition(tokenizer, dialect);
      definitions.Add(columnDefinition);
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.Equals(",")){
        tokenizer.NextToken();
        nextToken = tokenizer.LookaheadToken2();
      } else{
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
  }

  private static SqlColumn ReadCrossApplySourceColumn(SqlTokenizer tokenizer, SqlTable table) {
    SqlToken id1 = tokenizer.NextIdentifier();
    SqlToken next = tokenizer.LookaheadToken2();
    SqlColumn result = null;
    if (next.Equals(SqlToken.Dot)) {
      tokenizer.NextToken();
      SqlToken id2 = tokenizer.NextIdentifier();

      if (!Utilities.EqualIgnoreCase(id1.Value, table.GetAlias())) {
        throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.CROSS_APPLY_UNKNOWN_TABLE, id1.Value));
      }
      result = new SqlGeneralColumn(table, id2.Value);
    } else {
      result = new SqlGeneralColumn(table, id1.Value);
    }
    return result;
  }


  private static SqlTable ReadJoinTable(SqlTable top, SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    SqlTable last = top;
    JavaVector<SqlTable> joins = new JavaVector<SqlTable>();
    while (true) {
      JoinType type = GetJoinType(tryNext.Value);
      bool implicitInner = Utilities.EqualIgnoreCase("JOIN", tryNext.Value) ? true : false;
      SqlConditionNode condition = null;
      SqlJoin join = null;
      SqlTable right = null;
      if (type != JoinType.NONE) {
        tokenizer.NextToken();
        bool hasOuter = false;
        if (JoinType.LEFT == type || JoinType.RIGHT == type || JoinType.FULL == type) {
          tryNext = tokenizer.LookaheadToken2();
          if (tryNext.Equals("OUTER")) {
            hasOuter = true;
            tokenizer.NextToken();
          }
        }
        if (!implicitInner) tokenizer.EnsureNextToken("JOIN");
        bool isEach = false;
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("EACH")) {
          tokenizer.NextToken();
          isEach = true;
        }
        right = ReadTable(select, tokenizer, dialect);
        right = ReadJoinTable(right, select, tokenizer, dialect);
        tryNext = tokenizer.LookaheadToken2();
        if (type != JoinType.CROSS && tryNext.Equals("ON")) {
          tokenizer.NextToken();
          condition = (SqlConditionNode)ReadExpression(tokenizer, select);
        }

        if (right.HasJoin()) {
          SqlTable wrapperToNested = new SqlTable(null, right);
          join = new SqlJoin(type, wrapperToNested, condition, isEach, hasOuter);
        } else {
          join = new SqlJoin(type, right, condition, isEach, hasOuter);
        }
      } else {
        break;
      }
      last = new SqlTable(last.GetCatalog(), last.GetSchema(), last.GetName(), last.GetAlias(), join, last.GetNestedJoin(), last.GetQuery(), last.GetTableValueFunction(), last.GetCrossApply());
      joins.Add(last);
      last = right;
      tryNext = tokenizer.LookaheadToken2();
    }

    SqlTable lastJoin = top;
    if (joins.Size() > 0) {
      lastJoin = joins.Get(joins.Size() - 1);
    }
    SqlTable newTop = lastJoin;
    for (int i = joins.Size() - 1; i >= 1; --i) {
      SqlTable t = joins.Get(i - 1);
      newTop = new SqlTable(t.GetCatalog(), t.GetSchema(), t.GetName(), t.GetAlias(), new SqlJoin(t.GetJoin().GetJoinType(), lastJoin, t.GetJoin().GetCondition(), t.GetJoin().IsEach(), t.GetJoin().HasOuter()), t.GetNestedJoin(), t.GetQuery(), t.GetTableValueFunction(), t.GetCrossApply());
      lastJoin = newTop;
    }
    top = newTop;
    return top;
  }

  private static void ParseSelectSimpleSelectPart(SqlSelectStatement select, SqlTokenizer tokenizer) {
    Dialect dialect = select.GetDialectProcessor();
    if (dialect != null) {
      SqlExpression option = dialect.ReadOption(tokenizer);
      if (option != null) {
        select.SetOption(option);
      } else {
        ParseOptions(select, tokenizer);
      }
    } else {
      ParseOptions(select, tokenizer);
    }
    select.SetColumns(ParseColumns(tokenizer, select));
  }

  private static void ParseOptions(SqlSelectStatement select, SqlTokenizer tokenizer) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals("_LAST_")) {
      tokenizer.NextToken();
      select.SetFromLast(true);
    } else if (next.Equals("TOP")) {
      ParseTop(select, tokenizer);
    } else if (next.Equals("DISTINCT")) {
      tokenizer.NextToken();
      select.SetDistinct(true);
    }
  }

  private static void ParseTop(SqlSelectStatement select, SqlTokenizer tokenizer) {
    int pos = tokenizer.CurrentPosition();
    tokenizer.NextToken(); // Consume 'TOP'
    SqlExpression limit = ReadTerm(tokenizer, select);
    if (limit is SqlExpressionList) {
      tokenizer.Backtrack(pos);
    } else {
      select.SetLimitExpr(limit);
    }
  }

  private static void ParseHaving(SqlSelectStatement select, SqlTokenizer tokenizer) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("HAVING")) {
      tokenizer.NextToken();
      select.SetHavingClause((SqlConditionNode) ReadExpression(tokenizer, select));
    }
  }

  private static SqlQueryStatement ParseSelectUnionExtension(SqlQueryStatement left, SqlTokenizer tokenizer, Dialect dialectProcessor) {
    SqlSelectUnionStatement union;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (true) {
      if (tryNext.Equals("UNION")) {
        union = new SqlSelectUnionStatement(left, dialectProcessor);
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("ALL")) {
          union.SetUnionType(UnionType.UNION_ALL);
          tokenizer.NextToken();
        } else if (tryNext.Equals("DISTINCT")) {
          union.SetUnionType(UnionType.UNION);
          tokenizer.NextToken();
        } else {
          union.SetUnionType(UnionType.UNION);
        }
        union.SetRight(ParserCore.ParseSelectSub(tokenizer, dialectProcessor, union.GetParameterList().Size()));
        left = union;
      } else if (tryNext.Equals("MINUS") || tryNext.Equals("EXCEPT")) {
        union = new SqlSelectUnionStatement(left, dialectProcessor);
        tokenizer.NextToken();
        union.SetUnionType(UnionType.EXCEPT);
        union.SetRight(ParserCore.ParseSelectSub(tokenizer, dialectProcessor, union.GetParameterList().Size()));
        left = union;
      } else if (tryNext.Equals("INTERSECT")) {
        union = new SqlSelectUnionStatement(left, dialectProcessor);
        tokenizer.NextToken();
        union.SetUnionType(UnionType.INTERSECT);
        union.SetRight(ParserCore.ParseSelectSub(tokenizer, dialectProcessor, union.GetParameterList().Size()));
        left = union;
      } else {
        break;
      }
      tryNext = tokenizer.LookaheadToken2();
    }
    ParseEndofQuery(left, tokenizer);
    return left;
  }

  private static void ParseLimit(SqlQueryStatement query, SqlTokenizer tokenizer) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("LIMIT")) {
      tokenizer.NextToken();
      SqlExpression limit = ParserCore.ReadExpression(tokenizer, query);
      query.SetLimitExpr(limit);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("OFFSET")) {
        tokenizer.NextToken();
        SqlExpression offset = ParserCore.ReadExpression(tokenizer, query);
        query.SetOffsetExpr(offset);
      } else if (tryNext.Equals(",")) {
        tokenizer.NextToken();
        SqlExpression offset = limit;
        limit = ParserCore.ReadExpression(tokenizer, query);
        query.SetOffsetExpr(offset);
        query.SetLimitExpr(limit);
      }
    }
  }

  private static void ParseUpdatability(SqlQueryStatement query, SqlTokenizer tokenizer) {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("FOR")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("READ")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("ONLY");
        query.SetUpdatability(new SqlUpdatability(SqlUpdatability.READ_ONLY));
      } else {
        tokenizer.EnsureNextToken("UPDATE");
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Equals("OF")) {
          tokenizer.NextToken();
          SqlCollection<SqlColumn> columns = ParseColumns(tokenizer, query);
          query.SetUpdatability(new SqlUpdatability(SqlUpdatability.UPDATE, columns));
        } else {
          query.SetUpdatability(new SqlUpdatability(SqlUpdatability.UPDATE));
        }
      }
    }
  }

  private static void ParseEndofQuery(SqlQueryStatement query, SqlTokenizer tokenizer) {
    ParseOrderBy(query, tokenizer);
    ParseLimit(query, tokenizer);
    ParseUpdatability(query, tokenizer);
    ParserCore.ParseComment(query, tokenizer);
  }

  private static bool IsFunctionEscape(SqlTokenizer tokenizer) {
    //https://msdn.microsoft.com/en-us/library/ms709434(v=vs.85).aspx
    bool isFunctionEsc = false;
    int start = tokenizer.CurrentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Kind == TokenKind.ESCInitiator) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Identifier && tryNext.Equals("fn")) {
        isFunctionEsc = true;
      }
    }
    tokenizer.Backtrack(start);
    return isFunctionEsc;
  }

  private static bool IsLikeEscape(SqlTokenizer tokenizer) {
    //https://msdn.microsoft.com/en-us/library/ms710128(v=vs.85).aspx
    bool isLikeEsc = false;
    int start = tokenizer.CurrentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Kind == TokenKind.ESCInitiator) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Identifier && tryNext.Equals("escape")) {
        isLikeEsc = true;
      }
    }
    tokenizer.Backtrack(start);
    return isLikeEsc;
  }

  private static bool IsDateValueEscape(SqlTokenizer tokenizer) {
    //https://msdn.microsoft.com/en-us/library/ms710282(v=vs.85).aspx
    bool isDateValueEsc = false;
    int start = tokenizer.CurrentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Kind == TokenKind.ESCInitiator) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Identifier) {
        if (tryNext.Equals("d") || tryNext.Equals("t") || tryNext.Equals("ts"))
          isDateValueEsc = true;
      }
    }
    tokenizer.Backtrack(start);
    return isDateValueEsc;
  }

  private static void AddParas2Parent(SqlCollection<SqlValueExpression> parentParas, SqlCollection<SqlValueExpression> subParas) {
    foreach(SqlValueExpression p in subParas) {
      parentParas.Add(p);
    }
  }

  private static void ParseWithClause(SqlTokenizer tokenizer, Dialect dialectProcessor, SqlCollection<SqlTable> withes) {
    bool finished = false;

    while (!finished) {
      SqlToken token = tokenizer.NextToken();
      string tableName = token.Value;

      // SKIP CTE columns
      token = tokenizer.LookaheadToken2();
      if(token.Equals("(")) {
        tokenizer.NextToken();
        while (!token.Equals(")")) {
          continue;
        }
      }

      tokenizer.EnsureNextToken("AS");
      SqlQueryStatement sub = ParserCore.ParseSelectUnion(tokenizer, dialectProcessor);
      withes.Add(new SqlTable(sub, tableName, tableName));

      SqlToken next = tokenizer.LookaheadToken2();
      if(!next.Equals(",")) {
        finished = true;
      } else {
        tokenizer.NextToken();
      }
    }
  }
}
}

