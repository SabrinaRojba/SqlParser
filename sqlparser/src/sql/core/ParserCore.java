package core;
/*#
using RSSBus.core;
using RSSBus;
using CData.Sql;
#*/

import cdata.sql.*;
import rssbus.oputils.common.Utilities;
import java.util.*;

public final class ParserCore {
  public static final String DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX = "NESTED_QUERY";
  public static final String DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX = "NESTED_JOIN";
  public static final String DERIVED_VALUES_TABLE_NAME_PREFIX = "VALUES_DERIVED_TABLE";

  public static SqlParser tryParse(SqlTokenizer tokenizer, Dialect dialectProcessor) {
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
        SqlParser parser = new SqlParser(tokenizer.getInputText(), null, type);
        parser.failedParseException = tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EMPTY_STATEMENT));
        return parser;
      }

      if (null != dialectProcessor) {
        stmt = dialectProcessor.parse(tokenizer);
        if (stmt != null) {
          type = new SqlParser(stmt).getStatementType();
          return new SqlParser(tokenizer.GetStatementText(), stmt, type);
        }
      }

      if (token.equals("SELECT") || isQueryClause(tokenizer)) {
        type = StatementType.SELECT;
        stmt = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
        if (stmt instanceof SqlSelectIntoStatement) {
          type = StatementType.SELECTINTO;
        }
      } else if (token.equals("DELETE")) {
        type = StatementType.DELETE;
        stmt = new SqlDeleteStatement(tokenizer, dialectProcessor);

      } else if (token.equals("INSERT")) {
        type = StatementType.INSERT;
        stmt = new SqlInsertStatement(tokenizer, dialectProcessor);

      } else if (token.equals("UPSERT")) {
        type = StatementType.UPSERT;
        stmt = new SqlUpsertStatement(tokenizer, dialectProcessor);

      } else if (token.equals("UPDATE")) {
        type = StatementType.UPDATE;
        stmt = new SqlUpdateStatement(tokenizer, dialectProcessor);

      } else if (token.equals("MERGE")) {
        type = StatementType.MERGE;
        stmt = new SqlMergeStatement(tokenizer, dialectProcessor);

      } else if (token.equals("CACHE")) {
        type = StatementType.CACHE;
        stmt = new SqlCacheStatement(tokenizer, dialectProcessor);

      } else if (token.equals("CHECKCACHE")) {
        type = StatementType.CHECKCACHE;
        stmt = new SqlCheckCacheStatement(tokenizer, dialectProcessor);

      } else if (token.equals("REPLICATE")) {
        type = StatementType.REPLICATE;
        stmt = new SqlReplicateStatement(tokenizer, dialectProcessor);

      } else if (token.equals("KILL")) {
        type = StatementType.KILL;
        stmt = new SqlKillStatement(tokenizer, dialectProcessor);

      } else if (token.equals("GETDELETED")) {
        type = StatementType.GETDELETED;
        stmt = new SqlGetDeletedStatement(tokenizer,dialectProcessor);

      } else if (token.equals("METAQUERY")) {
        type = StatementType.METAQUERY;
        stmt = new SqlMetaQueryStatement(tokenizer, dialectProcessor);

      } else if (token.equals("MEMORYQUERY")) {
        type = StatementType.MEMORYQUERY;
        stmt = new SqlMemoryQueryStatement(tokenizer, dialectProcessor);

      } else if (token.equals("LOADMEMORYQUERY")) {
        type = StatementType.LOADMEMORYQUERY;
        stmt = new SqlLoadMemoryQueryStatement(tokenizer, dialectProcessor);

      } else if (token.equals("QUERYCACHE")) {
        type = StatementType.QUERYCACHE;
        stmt = new SqlQueryCacheStatement(tokenizer, dialectProcessor);

      } else if (token.equals("RESET")) {
        type = StatementType.RESET;
        stmt = new SqlResetStatement(tokenizer, dialectProcessor);

      } else if (token.equals("EXEC") || token.equals("EXECUTE")) {
        type = StatementType.EXEC;
        stmt = new SqlExecStatement(tokenizer, dialectProcessor);

      } else if (token.equals("CALL")) {
        type = StatementType.CALL;
        stmt = new SqlCallSPStatement(tokenizer, dialectProcessor);

      } else if (token.equals("CREATE")) {
        type = StatementType.CREATE;
        stmt = new SqlCreateTableStatement(tokenizer, dialectProcessor);

      } else if (token.equals("DROP")) {
        type = StatementType.DROP;
        stmt = new SqlDropStatement(tokenizer, dialectProcessor);

      } else if (token.equals("COMMIT")) {
        type = StatementType.COMMIT;
        stmt = new SqlCommitStatement(tokenizer, dialectProcessor);
      } else if (token.equals("ALTER")) {
        tokenizer.NextToken();
        token = tokenizer.LookaheadToken2();
        if (token.equals("TABLE")) {
          type = StatementType.ALTERTABLE;
          stmt = new SqlAlterTableStatement(tokenizer, dialectProcessor);
        } else {
          type = StatementType.UNKNOWN;
          throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNRECOGNIZED_KEYWORD, token.Value));
        }
      } else {
        type = StatementType.UNKNOWN;
        if (null != dialectProcessor) {
          stmt = dialectProcessor.parse(tokenizer);
          if (stmt != null) return new SqlParser(tokenizer.GetStatementText(), stmt, type);
        } else {
          throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNRECOGNIZED_KEYWORD, token.Value));
        }
      }
      token = tokenizer.LookaheadToken2();
      if (token.equals(SqlToken.None)) {
        tokenizer.NextToken();
      } else if (!token.IsEmpty()) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNEXPECTED_TOKEN, token.Value));
      }
    } catch (Exception ex) {
      SqlParser parser = new SqlParser(tokenizer.getInputText(), null, type);
      parser.failedParseException = ex;
      return parser;
    }
    return new SqlParser(tokenizer.GetStatementText(), stmt, type);
  }

  public static SqlParser tryParseXml(String text, Dialect dialectProcessor) {
    int start = 0;
    while (start < text.length() && Character./*@*/isSpaceChar/*@*//*#IsWhiteSpace#*/(text.charAt(start))) {
      start++;
    }
    if (start < text.length() && text.charAt(start) == '<') { // A simple test for Updategrams should be good enough for now
      SqlXmlStatement xmlSql = new SqlXmlStatement(text, dialectProcessor);
      return new SqlParser(text, xmlSql, StatementType.XML);
    }
    return null;
  }

  public static boolean containWildColumn(SqlCollection<SqlColumn> columns) {
    boolean ret = false;
    if (columns != null) {
      for (SqlColumn col : columns) {
        if (col instanceof SqlWildcardColumn) {
          ret = true;
          break;
        }
      }
    }
    return ret;
  }

  public static boolean isParameterExpression(SqlExpression expr) {
    if (expr instanceof SqlValueExpression) {
      SqlValueExpression valExpr = (SqlValueExpression) expr;
      return valExpr.isParameter();
    } else {
      return false;
    }
  }

  public static boolean isJoinStatement(SqlSelectStatement selectStatement) {
    boolean isJoin = false;
    if (selectStatement.getTables().size() > 1) {
      isJoin = true;
    }

    if (selectStatement.getJoins().size() > 0) {
      isJoin = true;
    }
    return isJoin;
  }

  public static void parseComment(SqlStatement stmt, SqlTokenizer tokenizer) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.Kind == TokenKind.Comment) {
      stmt.getComments().add(tryNext.Value);
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
    }
  }

  public static SqlCollection<SqlColumn> parseColumns(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlCollection<SqlColumn> columns = new SqlCollection<SqlColumn>();
    Hashtable<String, String> alias2Name = new Hashtable<String, String>();
    SqlToken tryNext;
    do {
      SqlExpression column;
      if (tokenizer.LookaheadToken2().equals("*")) {
        tokenizer.NextToken();
        column = new SqlWildcardColumn();
      } else {
        column = readExpression(tokenizer, stmt);
        tryNext = tokenizer.LookaheadToken2();
        String aliasName = null;
        boolean hasAlias = false;
        if (tryNext.equals("AS")) {
          hasAlias = true;
          tokenizer.NextToken();
        }

        if (TokenKind.Str == tryNext.Kind
            || TokenKind.Identifier == tryNext.Kind) {
          hasAlias = true;
        }

        if (hasAlias) {
          aliasName = tokenizer.NextToken().Value;
          if (column instanceof SqlColumn) {
            if (((SqlColumn) column).getTableName() != null) {
              String fullName = ((SqlColumn) column).getFullName();
              alias2Name.put(aliasName, fullName);
            } else {
              alias2Name.put(aliasName, ((SqlColumn) column).getColumnName());
            }

          }
        } else {
          if (column instanceof SqlColumn) {
            String columnName = ((SqlColumn)column).getAlias();
            int match = 0;
            if (columnName != null) {
              if (((SqlColumn) column).getTableName() != null) {
                columnName = ((SqlColumn) column).getFullName();
              }
              boolean isExistAlias = alias2Name.containsKey(columnName);
              if (isExistAlias) {
                for (Enumeration e = alias2Name.keys(); e.hasMoreElements();) {
                  String existedAlias =  (String)e.nextElement();
                  String fullName = alias2Name.get(existedAlias);
                  if (fullName.equals(columnName)) {
                    match++;
                  }
                }
              }
            }
            if (match > 0) {
              aliasName = columnName + match;
              alias2Name.put(aliasName, columnName);
            } else {
              alias2Name.put(columnName, columnName);
            }
          }
        }

        if (column instanceof SqlOperationExpression) {
          SqlOperationExpression opExpr = (SqlOperationExpression)column;
          if (!Utilities.isNullOrEmpty(aliasName)) {
            column = new SqlOperationColumn(aliasName, opExpr);
          } else {
            column = new SqlOperationColumn(opExpr);
          }
        } else if (column instanceof SqlValueExpression) {
          if (!Utilities.isNullOrEmpty(aliasName)) {
            column = new SqlConstantColumn(aliasName, (SqlValueExpression)column);
          } else {
            column = new SqlConstantColumn((SqlValueExpression)column);
          }
        }  else if (column instanceof SqlSubQueryExpression) {
          if (!Utilities.isNullOrEmpty(aliasName)) {
            column = new SqlSubQueryColumn(aliasName, (SqlSubQueryExpression)column);
          } else {
            column = new SqlSubQueryColumn((SqlSubQueryExpression)column);
          }
        } else if (column instanceof SqlFormulaColumn) {
          if (!Utilities.isNullOrEmpty(aliasName)) {
            SqlFormulaColumn fcol = (SqlFormulaColumn) column;
            column = new SqlFormulaColumn(fcol.getColumnName(),
                    aliasName,
                    fcol.getParameters(),
                    fcol.getOverClause());
          }
        } else if (column instanceof SqlGeneralColumn && !(column instanceof SqlWildcardColumn)) {
          if (!Utilities.isNullOrEmpty(aliasName)) {
            SqlGeneralColumn col = (SqlGeneralColumn) column;
            column = new SqlGeneralColumn(col.getTable(), col.getColumnName(), aliasName, col.getValueExpr());
          }
        }
      }
      if (column != null) {
        columns.add((SqlColumn)column);
      }
      tryNext = tokenizer.LookaheadToken2();
      if (!tryNext.equals(",")) {
        break;
      } else {
        tokenizer.NextToken();
      }
    } while (true);
    return columns;
  }

  public static boolean isQueryClause(SqlTokenizer tokenizer) throws Exception {
    int start = tokenizer.currentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.equals(SqlToken.Open.Value)) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
    }
    boolean isQuery = tryNext.equals("SELECT") || tryNext.equals("FROM") || tryNext.equals("WITH");
    tokenizer.Backtrack(start);
    return isQuery;
  }

  public static SqlExpression readExpression(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception{
    SqlExpression expr = readAndExpr(tokenizer, stmt);
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.equals("OR")) {
      tokenizer.NextToken();
      expr = new SqlCondition(expr, SqlLogicalOperator.Or, readAndExpr(tokenizer, stmt));
      tryNext = tokenizer.LookaheadToken2();
    }
    return expr;
  }

  public static SqlOutputClause readOutputClause(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlToken tN = tokenizer.LookaheadToken2();
    if (!tN.equals("OUTPUT")) {
      return null;
    }
    tokenizer.NextToken();
    SqlCollection<SqlColumn> dml_select_list = parseColumns(tokenizer, stmt);
    tN = tokenizer.LookaheadToken2();
    if (tN.equals("INTO")) {
      tokenizer.NextToken();
      return new SqlOutputClause(dml_select_list, readExpression(tokenizer, stmt));
    } else {
      return new SqlOutputClause(dml_select_list);
    }
  }

  public static SqlOverClause readOverClause(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    tokenizer.EnsureNextToken("OVER");
    tokenizer.EnsureNextToken(SqlToken.Open.Value);
    SqlOverClause overClause;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    ArrayList<SqlExpression> partition = new ArrayList<SqlExpression>();
    if (tryNext.equals("PARTITION")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("BY");

      do {
        SqlExpression p = readExpression(tokenizer, stmt);
        partition.add(p);
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
    }

    tryNext = tokenizer.LookaheadToken2();
    SqlCollection<SqlOrderSpec> orderBy = new SqlCollection<SqlOrderSpec>();
    if (tryNext.equals("ORDER")) {
      SqlSelectStatement select = new SqlSelectStatement(null);
      parseOrderBy(select, tokenizer);
      orderBy = select.getOrderBy();
    }

    tryNext = tokenizer.LookaheadToken2();
    SqlFrameClause frameClause = null;
    if (tryNext.equals("ROWS") || tryNext.equals("RANGE")) {
      frameClause = readFrameClause(tokenizer, stmt);
    }

    overClause = new SqlOverClause(partition.toArray(/*@*/new SqlExpression [0]/*@*//*#typeof(SqlExpression)#*/),
            orderBy.toArray(/*@*/new SqlOrderSpec[0]/*@*//*#typeof(SqlOrderSpec)#*/),
            frameClause);
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    return overClause;
  }

  private static SqlFrameClause readFrameClause(SqlTokenizer  tokenizer, SqlStatement stmt) throws Exception {
    SqlFrameClause frame;
    WinFrameType frameType;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("ROWS")) {
      tokenizer.NextToken();
      frameType = WinFrameType.ROWS;
    } else {
      tokenizer.EnsureNextToken("RANGE");
      frameType = WinFrameType.RANGE;
    }
    tryNext = tokenizer.LookaheadToken2();
    WinFrameOption start;
    WinFrameOption end;
    if (tryNext.equals("BETWEEN")) {
      tokenizer.NextToken();
      start = readFrameOptions(tokenizer, stmt);
      tokenizer.EnsureNextToken("AND");
      end = readFrameOptions(tokenizer, stmt);
    } else {
      start = readFrameOptions(tokenizer, stmt);
      end = null;
    }
    return new SqlFrameClause(frameType,
            start,
            end);
  }

  private static WinFrameOption readFrameOptions(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    WinFrameOptionType type;
    SqlExpression expr = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("UNBOUNDED")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("PRECEDING")) {
        tokenizer.NextToken();
        type = WinFrameOptionType.UNBOUNDED_PRECEDING;
      } else {
        tokenizer.EnsureNextToken("FOLLOWING");
        type = WinFrameOptionType.UNBOUNDED_FOLLOWING;
      }
    } else if (tryNext.equals("CURRENT")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("ROW");
      type = WinFrameOptionType.CURRENT_ROW;
    } else {
      expr = readExpression(tokenizer, stmt);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("PRECEDING")) {
        tokenizer.NextToken();
        type = WinFrameOptionType.PRECEDING;
      } else {
        tokenizer.EnsureNextToken("FOLLOWING");
        type = WinFrameOptionType.FOLLOWING;
      }
    }
    return new WinFrameOption(type, expr);
  }

  public static SqlConditionNode ParseWhere(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    SqlExpression cond = null;
    if (tryNext.equals("WHERE")) {
      tokenizer.NextToken();
      cond = readExpression(tokenizer, stmt);
      if (cond instanceof SqlValueExpression && cond.evaluate().getValueType() == SqlValueType.BOOLEAN){
        if (!cond.evaluate().getValueAsBool(true)) {
          cond = new SqlCriteria(null, ComparisonType.FALSE, null);
        }
      } else if (!(cond instanceof SqlConditionNode)) {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        cond = new SqlCriteria(cond, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
      }
    }
    return (SqlConditionNode)cond;
  }

  public static String[] parsePeriodTableName(SqlTokenizer tokenizer) throws Exception {
    return parsePeriodTableName(tokenizer, null);
  }

  public static String[] parsePeriodTableName(SqlTokenizer tokenizer, Dialect dialect) throws Exception{
    SqlCollection<String> periodNames = new SqlCollection<String>();
    SqlToken nextToken = tokenizer.NextToken();
    do {
      String v = null;
      if (dialect != null) {
        v = dialect.parseIdentifierName(nextToken);
      }
      if (null == v) {
        v = nextToken.Value;
      }
      periodNames.add(v);
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Dot) {
        tokenizer.NextToken();
        nextToken = tokenizer.NextToken();
      } else {
        break;
      }
    } while (true);


    String [] period;//catalog, schema, table
    if (periodNames.size() > 3) {
      period = new String [periodNames.size()];
    } else {
      period = new String [3];
    }

    if (1 == periodNames.size()) {
      period[2] = periodNames.get(0);
    } else if (2 == periodNames.size()) {
      period[1] = periodNames.get(0);
      period[2] = periodNames.get(1);
    } else if (3 == periodNames.size()) {
      period[0] = periodNames.get(0);
      period[1] = periodNames.get(1);
      period[2] = periodNames.get(2);
    } else {
      period[2] = periodNames.get(periodNames.size() - 1);
      period[1] = periodNames.get(periodNames.size() - 2);
      period[0] = periodNames.get(periodNames.size() - 3);
      for (int i = 3 ; i < periodNames.size(); ++i) {
        period[i] = periodNames.get(i - 3);
      }
    }

    return period;
  }

  public static boolean isValidTableName(String name) {
    if (name == null || name.equals("")) {
      return false;
    } else {
      if (name.startsWith(DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX)
              || name.startsWith(DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX)
              || name.startsWith(DERIVED_VALUES_TABLE_NAME_PREFIX)) {
        return false;
      } else {
        return true;
      }
    }
  }

  public static void flattenJoins(SqlCollection<SqlJoin> joins, SqlTable tableExpr) {
    if (tableExpr != null) {
      if (tableExpr.getNestedJoin() != null) {
        SqlTable nestJoin = tableExpr.getNestedJoin();
        flattenJoins(joins, nestJoin);
      }
      if (tableExpr.getJoin() != null) {
        joins.add(tableExpr.getJoin());
        flattenJoins(joins, tableExpr.getJoin().getTable());
      }
    }
  }

  public static SqlQueryStatement parseSelectUnion(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    return parseSelectUnion(tokenizer, dialectProcessor, 0);
  }

  public static SqlQueryStatement parseSelectUnion(SqlTokenizer tokenizer, Dialect dialectProcessor, int parentParasNumber) throws Exception{
    SqlQueryStatement query = ParserCore.parseSelectSub(tokenizer, dialectProcessor, parentParasNumber);
    return parseSelectUnionExtension(query, tokenizer, dialectProcessor);
  }

  public static void ParseGroupBy(SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("GROUP")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      boolean eachGroupBy = false;
      if (tryNext.equals("EACH")) {
        eachGroupBy = true;
        tokenizer.NextToken();
      }
      tokenizer.EnsureNextToken("BY");
      SqlCollection<SqlExpression> groupByList = new SqlCollection<SqlExpression>();
      do {
        SqlExpression expr = ParserCore.readExpression(tokenizer, select);
        if (expr instanceof  SqlValueExpression && expr.evaluate().getValueType() == SqlValueType.NUMBER) {
          if (!containWildColumn(select.getColumns())) {
            int position = Integer.parseInt(expr.evaluate().getValueAsString(null));
            if (position < 0) {
              position = -position;
            }
            SqlColumn column = select.getColumns().get(position - 1);
            if (column instanceof  SqlConstantColumn) {
              expr = column;
            } else  if (column.hasAlias()) {
              expr = new SqlGeneralColumn(column.getAlias());
            } else {
              expr = column;
            }
          }
        }
        tryNext = tokenizer.LookaheadToken2();
        groupByList.add(expr);
        if (tryNext.equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
      select.setGroupByClause(groupByList, eachGroupBy);
    }
  }

  public static void parseOrderBy(SqlQueryStatement select, SqlTokenizer tokenizer) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("ORDER")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("BY");
      SqlCollection<SqlOrderSpec> orderList = new SqlCollection<SqlOrderSpec>();
      do {
        SqlExpression expr = ParserCore.readExpression(tokenizer, select);
        SortOrder type = SortOrder.Asc;
        if (expr instanceof SqlValueExpression && expr.evaluate().getValueType() == SqlValueType.NUMBER) {
          if (!containWildColumn(select.getColumns())) {
            int position = Integer.parseInt(expr.evaluate().getValueAsString(null));
            if (position < 0) {
              type = SortOrder.Desc;
              position = -position;
            }
            SqlColumn column = select.getColumns().get(position - 1);
            if (column instanceof SqlConstantColumn) {
              expr = column;
            } else if (column.hasAlias()) {
              expr = new SqlGeneralColumn(column.getAlias());
            } else {
              expr = column;
            }
          }
        }
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("DESC")) {
          type = SortOrder.Desc;
          tokenizer.NextToken();
        } else if (tryNext.equals("ASC")) {
          tokenizer.NextToken();
        }
        tryNext = tokenizer.LookaheadToken2();
        boolean isNullsFirst = false;
        boolean hasNulls = false;
        if (tryNext.equals("NULLS")) {
          hasNulls = true;
          tokenizer.NextToken();
          tryNext = tokenizer.LookaheadToken2();
          if (tryNext.equals("FIRST")) {
            tokenizer.NextToken();
            isNullsFirst = true;
          } else if (tryNext.equals("LAST")) {
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
        orderList.add(order);
        if (tryNext.equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
      select.setOrderBy(orderList);
    }
  }

  public static void parseColumnDefinitions(SqlTokenizer tokenizer,
                                            SqlCollection<SqlColumnDefinition> ColumnDefinitions,
                                            Dialect dialect) throws Exception {
    tokenizer.EnsureNextToken(SqlToken.Open.Value);
    SqlToken nextToken = tokenizer.LookaheadToken2();
    while (true) {
      if (dialect != null) {
        SqlColumnDefinition COLUMN_DEF = dialect.parseColumnDefinition(tokenizer);
        if (COLUMN_DEF != null) {
          ColumnDefinitions.add(COLUMN_DEF);
          nextToken = tokenizer.LookaheadToken2();
          if(nextToken.equals(",")){
            tokenizer.NextToken();
            nextToken = tokenizer.LookaheadToken2();
            continue;
          } else{
            break;
          }
        }
      }

      if(nextToken.equals("PRIMARY")) {
        parsePrimaryKey(tokenizer, ColumnDefinitions);
      } else if(nextToken.equals("CONSTRAINT")) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNSUPPORTED_CONSTRAINT));
      } else{
        SqlColumnDefinition columnDefinition = parseOneColumnDefinition(tokenizer, dialect);
        ColumnDefinitions.add(columnDefinition);
      }
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.equals(",")){
        tokenizer.NextToken();
        nextToken = tokenizer.LookaheadToken2();
      } else{
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    if(!tokenizer.NextToken().IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SYNTAX, tokenizer.NextToken().Value));
    }

    if(ColumnDefinitions.size() <= 0){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_COL_DEF));
    }
  }

  public static SqlColumnDefinition parseOneColumnDefinition(SqlTokenizer tokenizer, Dialect dialect) throws Exception {
    SqlColumnDefinition newColumn = new SqlColumnDefinition();
    String columnName = null;
    SqlToken tk = tokenizer.NextToken();
    if (dialect != null) {
      columnName = dialect.parseIdentifierName(tk);
    }

    if (null == columnName) {
      columnName = tk.Value;
    }

    String dataType = tokenizer.NextToken().Value;
    if(Utilities.isNullOrEmpty(columnName)){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_COLNAME));
    }
    if(Utilities.isNullOrEmpty(dataType)){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_DATATYPE));
    }
    newColumn.ColumnName = columnName;
    newColumn.DataType = dataType;
    SqlToken nextToken = tokenizer.LookaheadToken2();
    if(nextToken.equals("(")){
      tokenizer.NextToken();
      nextToken = tokenizer.NextToken();
      if(nextToken.IsEmpty()){
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_COLSIZE));
      }
      newColumn.ColumnSize = nextToken.Value;
      nextToken = tokenizer.LookaheadToken2();
      if (nextToken.equals(",")) {
        tokenizer.NextToken();
        nextToken = tokenizer.NextToken();
        newColumn.Scale = nextToken.Value;
      }

      tokenizer.EnsureNextToken(")");
    }

    while(true){
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.equals("NOT")){
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("NULL");
        newColumn.IsNullable = false;
      } else if(nextToken.equals("PRIMARY")){
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("KEY");
        newColumn.IsKey = true;
      } else if(nextToken.equals("AUTO_INCREMENT") || nextToken.equals("AUTOINCREMENT")){
        newColumn.AutoIncrement = nextToken.Value;
        tokenizer.NextToken();
      } else if(nextToken.equals("DEFAULT")) {
        tokenizer.NextToken();
        SqlToken token = tokenizer.NextToken();
        String DefaultValue;
        if (token.WasQuoted) {
          DefaultValue = "'" + token.Value + "'";
        } else {
          DefaultValue = token.Value;
        }
        newColumn.DefaultValue = DefaultValue;
      } else if(nextToken.equals("UNIQUE")) {
        tokenizer.NextToken();
        newColumn.IsUnique = true;
      } else{
        break;
      }
    }
    return newColumn;
  }

  public static boolean parseIFEXISTS(SqlTokenizer tokenizer) throws Exception {
    boolean ifExists = false;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if(tryNext.equals("IF")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("EXISTS");
      ifExists = true;
    }
    return ifExists;
  }

  private static void parsePrimaryKey(SqlTokenizer tokenizer, SqlCollection<SqlColumnDefinition> ColumnDefinitions) throws Exception {
    tokenizer.NextToken();
    tokenizer.EnsureNextToken("KEY");
    tokenizer.EnsureNextToken("(");
    while(true){
      SqlToken nextToken = tokenizer.NextToken();
      if(nextToken.IsEmpty()){
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_PRIMARY_KEY));
      }
      boolean found = false;
      for(SqlColumnDefinition scd:ColumnDefinitions){
        if(scd.ColumnName.toLowerCase().equals(nextToken.Value.toLowerCase())){
          scd.IsKey = true;
          found = true;
          break;
        }
      }
      if(!found){
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.NO_COL_DEF, nextToken.Value));
      }
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.equals(",")){
        tokenizer.NextToken();
      } else if(nextToken.equals(")")){
        tokenizer.NextToken();
        break;
      } else{
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.SYNTAX, nextToken.Value));
      }
    }
  }

  private static SqlQueryStatement parseSelectSub(SqlTokenizer tokenizer, Dialect dialectProcessor, int parentParasNumber) throws Exception{
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals("(")) {
      tokenizer.NextToken();
      SqlQueryStatement query = parseSelectUnion(tokenizer, dialectProcessor, parentParasNumber);
      tokenizer.EnsureNextToken(")");
      return query;
    } else if(next.equals("WITH")) {
      SqlQueryStatement query = parseWithPart(dialectProcessor, tokenizer);
      return query;
    }
    SqlSelectStatement select = parseSelectSimple(tokenizer, dialectProcessor, parentParasNumber);
    return select;
  }

  private static boolean isTableValuedFunction(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    int start = tokenizer.currentPosition();
    SqlExpression expr = readExpression(tokenizer, new SqlSelectStatement(dialectProcessor));
    tokenizer.Backtrack(start);
    return expr instanceof SqlFormulaColumn;
  }

  private static SqlExpression readTerm(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlExpression expr = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    Dialect dialect = stmt.getDialectProcessor();
    if (dialect != null) {
      SqlExpression term = dialect.readTerm(tokenizer);
      if (term != null) {
        if (isParameterExpression(term)) {
          stmt.getParameterList().add((SqlValueExpression) term);
        }
        return term;
      }
    }
    if (tryNext.Kind == TokenKind.Open) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals(")")) {
        expr = new SqlExpressionList(new SqlExpression[0]);
      } else {
        SqlCollection<SqlExpression> list = new SqlCollection<SqlExpression>();
        do {
          expr = readExpression(tokenizer, stmt);
          list.add(expr);
          tryNext = tokenizer.LookaheadToken2();
          if (tryNext.equals(",")) {
            tokenizer.NextToken();
          } else {
            break;
          }
        } while (true);
        if (list.size() > 1) {
          expr = new SqlExpressionList(list.toArray(/*@*/new SqlExpression[0]/*@*//*#typeof(SqlExpression)#*/));
        }
      }
      tokenizer.EnsureNextToken(")");
    } else if (tryNext.Kind == TokenKind.ESCInitiator) {
      if (isFunctionEscape(tokenizer) || isDateValueEscape(tokenizer)) {
        tokenizer.EnsureNextToken(SqlToken.ESCInitiator.Value);
        tokenizer.NextToken();
        expr = readExpression(tokenizer, stmt);
        if (expr instanceof SqlFormulaColumn) {
          SqlFormulaColumn sourceFunction = (SqlFormulaColumn) expr;
          expr = new SqlFormulaColumn(SqlFormulaColumn.SCALAR_FUNCTION_NAME_PREFIX + sourceFunction.getColumnName(),
                  sourceFunction.getParameters(),
                  sourceFunction.getOverClause());
        }
        tokenizer.EnsureNextToken(SqlToken.ESCTerminator.Value);
      }
    } else if (tryNext.Kind == TokenKind.Parameter) {
      tokenizer.NextToken();
      if (tryNext.equals(SqlToken.AnonParam)) {
        expr = new SqlValueExpression("@" + (stmt.getParentParasNumber() + stmt.getParameterList().size() + 1));
        stmt.getParameterList().add((SqlValueExpression) expr);
      } else {
        expr = new SqlValueExpression(tryNext.Value);
        if (!tryNext.Value.startsWith("@@")) {
          stmt.getParameterList().add((SqlValueExpression) expr);
        }
      }
    } else if (tryNext.Kind == TokenKind.Keyword) {
      if (tryNext.equals("SELECT")) {
        SqlQueryStatement query = parseSelectUnion(tokenizer, stmt.getDialectProcessor(), stmt.getParameterList().size());
        addParas2Parent(stmt.getParameterList(), query.getParameterList());
        expr = new SqlSubQueryExpression(query);
      } else if (tryNext.equals("CURRENT_TIME") && !tryNext.WasQuoted) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Kind == TokenKind.Open) {
          tokenizer.EnsureNextToken(SqlToken.Open.Value);
          SqlCollection<String> pn = new SqlCollection<String>();
          pn.add("CURRENT_TIME");
          expr = readFunctionExpr(tokenizer, pn, stmt);
        } else {
          expr = new SqlFormulaColumn("CURRENT_TIME", new SqlCollection<SqlExpression>());
        }
      } else if (tryNext.equals("CURRENT_TIMESTAMP") && !tryNext.WasQuoted) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.Kind == TokenKind.Open) {
          tokenizer.EnsureNextToken(SqlToken.Open.Value);
          SqlCollection<String> pn = new SqlCollection<String>();
          pn.add("CURRENT_TIMESTAMP");
          expr = readFunctionExpr(tokenizer, pn, stmt);
        } else {
          expr = new SqlFormulaColumn("CURRENT_TIMESTAMP", new SqlCollection<SqlExpression>());
        }
      }
    } else if (tryNext.Kind == TokenKind.Identifier &&
        (tryNext.equals("TIMESTAMP")
            || tryNext.equals("TIME")
            || tryNext.equals("DATE"))) {
      SqlToken priorTK = tryNext;
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Open) {
        tokenizer.EnsureNextToken(SqlToken.Open.Value);
        SqlCollection<String> pn = new SqlCollection<String>();
        pn.add(priorTK.Value);
        expr = readFunctionExpr(tokenizer, pn, stmt);
      } else if (tryNext.Kind == TokenKind.Str) {
        SqlToken v = tokenizer.NextToken();
        int dataType = ColumnInfo.DATA_TYPE_TIMESTAMP;
        if (priorTK.equals("TIME")) {
          dataType = ColumnInfo.DATA_TYPE_TIME;
        } else if(priorTK.equals("DATE")) {
          dataType = ColumnInfo.DATA_TYPE_DATE;
        }
        expr = new SqlValueExpression(new SqlValue(dataType, v.Value, SqlValue.DEFAULTDATETIMEFORMAT));
      } else {
        expr = new SqlGeneralColumn(tokenizer.LastToken().Value);
      }
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals(".")) {
        SqlCollection<String> periodNames = new SqlCollection<String>();
        periodNames.add(tokenizer.LastToken().Value);
        tokenizer.NextToken();
        expr = readTermWithDot(tokenizer, periodNames, stmt);
      }
    } else if (tryNext.Kind == TokenKind.Identifier) {
      String name = tryNext.Value;
      SqlToken nameTK = tryNext;
      boolean isQuoted = tryNext.WasQuoted;
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      SqlCollection<String> periodNames;
      if (tryNext.equals(".")) {
        tokenizer.NextToken();
        periodNames = new SqlCollection<String>();
        periodNames.add(name);
        expr = readTermWithDot(tokenizer, periodNames, stmt);
      } else if (name.equalsIgnoreCase("CASE") && !isQuoted && !tryNext.equals(SqlToken.Open)) {
        expr = readCase(tokenizer, stmt);
      } else if (tryNext.equals(SqlToken.Open)) {
        tokenizer.NextToken();
        periodNames = new SqlCollection<String>();
        periodNames.add(name);
        expr = readFunctionExpr(tokenizer, periodNames, stmt);
      } else {
        if (name.equals("*")) {
          expr = new SqlWildcardColumn();
        } else {
          if (dialect != null) {
            String identifier = dialect.parseIdentifierName(nameTK);
            if (Utilities.isNullOrEmpty(identifier)) {
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
      expr = new SqlValueExpression(SqlValueType.STRING, tryNext.Value, /*@*/Locale.getDefault()/*@*//*#System.Globalization.CultureInfo.CurrentCulture#*/);
      tryNext = tokenizer.LookaheadToken2();
      if (TokenKind.Keyword == tryNext.Kind && tryNext.equals("COLLATE")) {
        tokenizer.NextToken();
        tokenizer.NextToken();
      }
    } else if (tryNext.Kind == TokenKind.Null) {
      tokenizer.NextToken();
      expr = new SqlValueExpression(SqlValue.getNullValueInstance());
    } else if (tryNext.Kind == TokenKind.Operator && tryNext.equals("-")) {
      tokenizer.NextToken();
      expr = readTerm(tokenizer, stmt);
      SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
      paras.add(expr);
      expr = new SqlFormulaColumn("NEGATE", paras);
    } else {
      tokenizer.NextToken();
    }
    return expr;
  }

  private static SqlExpression readTermWithDot(SqlTokenizer tokenizer,
                                               SqlCollection<String> periodNames,
                                               SqlStatement stmt) throws Exception {
    Dialect dialect = stmt.getDialectProcessor();
    SqlExpression expr = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (tryNext.Kind == TokenKind.Identifier) {
      SqlToken token = tokenizer.NextToken();
      String identifier = token.Value;
      if (dialect != null) {
        identifier = dialect.parseIdentifierName(token);
      }
      if (identifier != null) {
        periodNames.add(identifier);
      } else {
        periodNames.add(token.Value);
      }

      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals(SqlToken.Dot)) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
      } else {
        break;
      }
    }
    if (tryNext.equals(SqlToken.Open)) {
      tokenizer.NextToken();
      expr = readFunctionExpr(tokenizer, periodNames, stmt);
    } else {
      String [] period = parsePeriodColumnName(periodNames);
      if (Utilities.equalIgnoreCase("*", period[3])) {
        expr = new SqlWildcardColumn(new SqlTable(period[0], period[1], period[2]));
      } else {
        expr = new SqlGeneralColumn(new SqlTable(period[0], period[1], period[2]), period[3]);
      }
    }
    return expr;
  }

  private static String[] parsePeriodColumnName(SqlCollection<String> periodNames) {
    String [] period = new String [] {null, null, null, null};//catalog, schema, table, column
    if (periodNames != null && periodNames.size() > 0) {
      if (1 == periodNames.size()) {
        period[3] = periodNames.get(0);
      } else if (2 == periodNames.size()) {
        period[2] = periodNames.get(0);
        period[3] = periodNames.get(1);
      } else if (3 == periodNames.size()) {
        period[1] = periodNames.get(0);
        period[2] = periodNames.get(1);
        period[3] = periodNames.get(2);
      } else if (4 == periodNames.size()) {
        period[0] = periodNames.get(0);
        period[1] = periodNames.get(1);
        period[2] = periodNames.get(2);
        period[3] = periodNames.get(3);
      } else{
        period[3] = periodNames.get(periodNames.size() - 1);
        period[2] = periodNames.get(periodNames.size() - 2);
        period[1] = periodNames.get(periodNames.size() - 3);
        period[0] = periodNames.get(periodNames.size() - 4);
      }
    }
    return period;
  }

  private static SqlExpression readCase(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlFormulaColumn caseWhen = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
    if (tryNext.equals("WHEN")) {
      tokenizer.NextToken();
      paras.add(null);
      do {
        paras.add(readExpression(tokenizer, stmt));
        tokenizer.EnsureNextToken("THEN");
        paras.add(readExpression(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("WHEN")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while(true);
    } else {
      paras.add(readExpression(tokenizer, stmt));
      tokenizer.EnsureNextToken("WHEN");
      do {
        paras.add(readExpression(tokenizer, stmt));
        tokenizer.EnsureNextToken("THEN");
        paras.add(readExpression(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("WHEN")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
    }
    if (tryNext.equals("ELSE")) {
      tokenizer.NextToken();
      paras.add(readExpression(tokenizer, stmt));
    }
    tokenizer.EnsureNextToken("END");
    caseWhen = new SqlFormulaColumn("CASE", paras);
    return caseWhen;
  }

  private static SqlExpression readFunctionExpr(SqlTokenizer tokenizer, SqlCollection<String> periodName, SqlStatement stmt) throws Exception{
    SqlToken nextTry;
    String name = periodName.get(periodName.size() - 1);
    SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
    do {
      SqlExpression para;
      if (Utilities.equalIgnoreCase("CAST", name)) {
        paras.add(readExpression(tokenizer, stmt));
        tokenizer.EnsureNextToken("AS");
        para = readExpression(tokenizer, stmt);
        if (para instanceof SqlGeneralColumn) {
          paras.add(new SqlValueExpression(SqlValueType.STRING, ((SqlGeneralColumn) para).getFullName()));
        } else if (para instanceof SqlFormulaColumn) {
          SqlFormulaColumn typeDef = (SqlFormulaColumn)para;
          paras.add(new SqlValueExpression(SqlValueType.STRING, typeDef.getColumnName()));
          for (SqlExpression expr : typeDef.getParameters()) {
            paras.add(expr);
          }
        }
      } else if (SqlUtilities.isKnownAggragation(name)) {
        nextTry = tokenizer.LookaheadToken2();
        if (nextTry.equals("DISTINCT") && nextTry.Kind == TokenKind.Keyword) {
          tokenizer.NextToken();
          SqlCollection<SqlExpression> distinctPara = new SqlCollection<SqlExpression>();
          distinctPara.add(readExpression(tokenizer, stmt));
          paras.add(new SqlFormulaColumn("DISTINCT", distinctPara));
        } else {
          nextTry = tokenizer.LookaheadToken2();
          if (nextTry.equals("*")) {
            tokenizer.NextToken();
            paras.add(new SqlWildcardColumn());
          } else {
            paras.add(readExpression(tokenizer, stmt));
          }
        }
      } else if (Utilities.equalIgnoreCase("TOSTRING", name)) {
        if (periodName.size() > 1) {
          paras.add(new SqlGeneralColumn(periodName.get(periodName.size() - 2)));
        } else {
          paras.add(readExpression(tokenizer, stmt));
        }
      } else if (Utilities.equalIgnoreCase("CONCAT", name)) {
        if (periodName.size() > 1) {
          paras.add(new SqlGeneralColumn(periodName.get(periodName.size() - 2)));
          paras.add(readExpression(tokenizer, stmt));
        } else {
          paras.add(readExpression(tokenizer, stmt));
        }
      } else if (Utilities.equalIgnoreCase("EXTRACT", name)) {
        SqlToken part = tokenizer.NextToken();
        paras.add(new SqlValueExpression(SqlValueType.STRING, part.Value));
        tokenizer.EnsureNextToken("FROM");

        SqlExpression source = readExpression(tokenizer, stmt);

        if (source instanceof SqlValueExpression) {
          SqlValue dtValue = source.evaluate();
          if (dtValue.getDataType() == ColumnInfo.DATA_TYPE_TIMESTAMP) {
            paras.add(new SqlValueExpression(SqlValueType.STRING, "TIMESTAMP"));
          } else if (dtValue.getDataType() == ColumnInfo.DATA_TYPE_DATE) {
            paras.add(new SqlValueExpression(SqlValueType.STRING, "DATE"));
          } else if (dtValue.getDataType() == ColumnInfo.DATA_TYPE_TIME) {
            paras.add(new SqlValueExpression(SqlValueType.STRING, "TIME"));
          }
          paras.add(new SqlValueExpression(SqlValueType.DATETIME, "" + dtValue.getOriginalValue()));
        } else {
          paras.add(source);
        }

        nextTry = tokenizer.LookaheadToken2();
        if (nextTry.equals("AT")) {
          tokenizer.NextToken();
          nextTry = tokenizer.LookaheadToken2();
          if (nextTry.equals("LOCAL")) {
            tokenizer.NextToken();
            paras.add(new SqlValueExpression(SqlValueType.STRING, "AT LOCAL"));
          } else {
            tokenizer.EnsureNextToken("TIME");
            tokenizer.EnsureNextToken("ZONE");
            SqlToken tz_spec = tokenizer.NextToken();
            paras.add(new SqlValueExpression(SqlValueType.STRING, "AT TIME ZONE '" + tz_spec.Value + "'"));
          }
        }
      } else {
        nextTry = tokenizer.LookaheadToken2();
        if (!nextTry.equals(SqlToken.Close)) {
          paras.add(readExpression(tokenizer, stmt));
        }
      }
      nextTry = tokenizer.LookaheadToken2();
      if (nextTry.equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    } while (true);
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
    nextTry = tokenizer.LookaheadToken2();
    SqlFormulaColumn func;
    if (nextTry.equals("OVER")) {
      SqlOverClause overClause = readOverClause(tokenizer, stmt);
      func = new SqlFormulaColumn(name, paras, overClause);
    } else {
      func = new SqlFormulaColumn(name, paras);
    }
    return func;
  }

  private static SqlExpression readAndExpr(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception{
    SqlExpression r = readConditionExpr(tokenizer, stmt);
    while (tokenizer.LookaheadToken2().equals("AND")) {
      tokenizer.NextToken();
      r = new SqlCondition(r, SqlLogicalOperator.And, readConditionExpr(tokenizer, stmt));
    }
    return r;
  }

  private static SqlExpression readConditionExpr(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlExpression expr;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("NOT") && tryNext.Kind == TokenKind.Keyword) {
      tokenizer.NextToken();
      expr = new SqlConditionNot(readConditionExpr(tokenizer, stmt));
      return expr;
    } else if (tryNext.equals("EXISTS") && tryNext.Kind == TokenKind.Keyword) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("(");
      SqlQueryStatement query = parseSelectUnion(tokenizer, stmt.getDialectProcessor(), stmt.getParameterList().size());
      tokenizer.EnsureNextToken(")");
      addParas2Parent(stmt.getParameterList(), query.getParameterList());
      expr = new SqlConditionExists(query);
      return expr;
    }

    expr = readOperandExpr(tokenizer, stmt);
    tryNext = tokenizer.LookaheadToken2();

    while (true) {
      boolean hasNot = false;
      if (tryNext.equals("NOT")) {
        hasNot = true;
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
      }
      if (tryNext.equals("LIKE")) {
        tokenizer.NextToken();
        SqlExpression right = readOperandExpr(tokenizer, stmt);
        tryNext = tokenizer.LookaheadToken2();
        boolean isODBCLikeEscape = false;
        if (tryNext.equals(SqlToken.ESCInitiator)) {
          isODBCLikeEscape = isLikeEscape(tokenizer);
          tokenizer.NextToken();
          tryNext = tokenizer.LookaheadToken2();
        }
        if (tryNext.equals("ESCAPE")) {
          tokenizer.NextToken();
          SqlExpression escape = readOperandExpr(tokenizer, stmt);
          expr = new SqlCriteria(expr, right, escape);
        } else {
          expr = new SqlCriteria(expr, ComparisonType.LIKE, right);
        }
        if (isODBCLikeEscape) {
          tokenizer.EnsureNextToken(SqlToken.ESCTerminator.Value);
        }
      } else if (tryNext.equals("IS")) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("NOT")) {
          tokenizer.NextToken();
          SqlExpression right = readOperandExpr(tokenizer, stmt);
          expr = new SqlCriteria(expr, ComparisonType.IS_NOT, right);
        } else {
          SqlExpression right = readOperandExpr(tokenizer, stmt);
          expr = new SqlCriteria(expr, ComparisonType.IS, right);
        }
      } else if (tryNext.equals("IN")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("(");
        if (isQueryClause(tokenizer)) {
          SqlQueryStatement query = parseSelectUnion(tokenizer, stmt.getDialectProcessor(), stmt.getParameterList().size());
          expr = new SqlConditionInSelect(expr, query, false, ComparisonType.EQUAL);
          addParas2Parent(stmt.getParameterList(), query.getParameterList());
        } else {
          SqlCollection<SqlExpression> values = new SqlCollection<SqlExpression>();
          do {
            values.add(readExpression(tokenizer, stmt));
            tryNext = tokenizer.LookaheadToken2();
            if (tryNext.equals(",")) {
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
          expr = new SqlCriteria(expr, type, new SqlExpressionList(values.toArray(/*@*/new SqlExpression[0]/*@*//*#typeof(SqlExpression)#*/)));
        }
        tokenizer.EnsureNextToken(")");
      } else if (tryNext.equals("BETWEEN")) {
        tokenizer.NextToken();
        SqlExpression low = readOperandExpr(tokenizer, stmt);
        tokenizer.EnsureNextToken("AND");
        SqlExpression high = readOperandExpr(tokenizer, stmt);
        SqlCriteria conLow = new SqlCriteria(expr, ComparisonType.BIGGER_EQUAL, low);
        SqlCriteria conHigh = new SqlCriteria(expr, ComparisonType.SMALLER_EQUAL, high);
        expr = new SqlCondition(conLow, SqlLogicalOperator.And, conHigh);
      } else {
        ComparisonType type = SqlCriteria.getCompareType(tryNext.Value);
        String customOp = null;
        if (type == ComparisonType.NONE) {
          if (stmt.getDialectProcessor() != null) {
            customOp = stmt.getDialectProcessor().getCustomCompareOp(tokenizer);
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
          boolean isAll = false;
          boolean isComparsionSelect = false;
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
            SqlQueryStatement query = parseSelectUnion(tokenizer, stmt.getDialectProcessor(), stmt.getParameterList().size());
            addParas2Parent(stmt.getParameterList(), query.getParameterList());
            expr = new SqlConditionInSelect(expr, query, isAll, type);
            tokenizer.EnsureNextToken(")");
          } else {
            SqlExpression right = readOperandExpr(tokenizer, stmt);
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

  private static SqlExpression readOperandExpr(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlExpression sum = readSumExpr(tokenizer, stmt);
    SqlToken next = tokenizer.LookaheadToken2();
    while (next.equals("||")) {
      tokenizer.NextToken();
      SqlExpression sum2 = readSumExpr(tokenizer, stmt);
      sum = new SqlOperationExpression(SqlOperationType.CONCAT, sum, sum2);
      next = tokenizer.LookaheadToken2();
    }
    return sum;
  }

  private static SqlExpression readSumExpr(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlExpression factor = readFactorExpr(tokenizer, stmt);
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (true) {
      if (tryNext.equals("+")) {
        tokenizer.NextToken();
        factor = new SqlOperationExpression(SqlOperationType.PLUS, factor, readFactorExpr(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
      } else if (tryNext.equals("-")) {
        tokenizer.NextToken();
        factor = new SqlOperationExpression(SqlOperationType.MINUS, factor, readFactorExpr(tokenizer, stmt));
        tryNext = tokenizer.LookaheadToken2();
      } else if (tryNext.Kind == TokenKind.Number && tryNext.Value.startsWith("-")) {
        //column-1
        tokenizer.NextToken();
        String signValue = tryNext.Value;
        String v = signValue.substring(1, tryNext.Value.length());
        SqlValueExpression vExpr = new SqlValueExpression(SqlValueType.NUMBER, v);
        factor = new SqlOperationExpression(SqlOperationType.MINUS, factor, vExpr);
        tryNext = tokenizer.LookaheadToken2();
      } else {
        return factor;
      }
    }
  }

  private static SqlExpression readFactorExpr(SqlTokenizer tokenizer, SqlStatement stmt) throws Exception {
    SqlExpression r = readTerm(tokenizer, stmt);
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (true) {
      if (tryNext.equals("*")) {
        tokenizer.NextToken();
        r = new SqlOperationExpression(SqlOperationType.MULTIPLY, r, readTerm(tokenizer, stmt));
      } else if (tryNext.equals("/")) {
        tokenizer.NextToken();
        r = new SqlOperationExpression(SqlOperationType.DIVIDE, r, readTerm(tokenizer, stmt));
      } else if (tryNext.equals("%")) {
        tokenizer.NextToken();
        r = new SqlOperationExpression(SqlOperationType.MODULUS, r, readTerm(tokenizer, stmt));
      } else {
        return r;
      }
      tryNext = tokenizer.LookaheadToken2();
    }
  }

  private static boolean is_SCOPE_IDENTITY(SqlCollection<SqlColumn> columns) {
    if (1 == columns.size()) {
      String name = columns.get(0).getColumnName();
      if (name != null) {
        if (name.equalsIgnoreCase("LAST_INSERT_ID") ||
           name.equalsIgnoreCase("SCOPE_IDENTITY")) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean is_ALL_CONSTANT_COLUMNS(SqlCollection<SqlColumn> columns) {
    boolean result = true;
    for (SqlColumn c : columns) {
      if (!(c instanceof SqlConstantColumn)) {
        result = false;
        break;
      }
    }
    return result;
  }

  private static SqlSelectStatement parseSelectSimple(SqlTokenizer tokenizer, Dialect dialectProcessor, int parentParasNumber) throws Exception{
    boolean fromFirst = false;
    boolean withFirst = false;
    boolean isSelectInto = false;
    SqlValueExpression externalDatabase = null;
    SqlTable intoTable = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("SELECT")) {
      tokenizer.NextToken();
      fromFirst = false;
    } else if(tryNext.equals("WITH")) {
      withFirst = true;
    } else {
      tokenizer.EnsureNextToken("FROM");
      fromFirst = true;
    }
    SqlSelectStatement select = new SqlSelectStatement(dialectProcessor, parentParasNumber);
    if (fromFirst) {
      parseSelectSimpleFromPart(dialectProcessor, select, tokenizer);
      tokenizer.EnsureNextToken("SELECT");
      parseSelectSimpleSelectPart(select, tokenizer);
    } else if(withFirst) {
      parseSimpleWithPart(select, tokenizer);
    } else {
      parseSelectSimpleSelectPart(select, tokenizer);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("FROM")) {
        tokenizer.NextToken();
        parseSelectSimpleFromPart(dialectProcessor, select, tokenizer);
      } else if (tryNext.equals("INTO")) {
        isSelectInto = true;
        tokenizer.NextToken();
        String [] period = parsePeriodTableName(tokenizer, dialectProcessor);
        String intoTableName = period[2];
        intoTable = new SqlTable(period[0], period[1], intoTableName, intoTableName);
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("IN")) {
          tokenizer.NextToken();
          SqlToken token = tokenizer.NextToken();
          externalDatabase = new SqlValueExpression(SqlValueType.STRING, token.Value);
        }
        tokenizer.EnsureNextToken("FROM");
        parseSelectSimpleFromPart(dialectProcessor, select, tokenizer);
      } else if (!tryNext.IsEmpty()) {
        if (is_SCOPE_IDENTITY(select.getColumns())
            || is_ALL_CONSTANT_COLUMNS(select.getColumns())) {
          return select;
        } else if (!tryNext.equals("FROM")){
          return select;
        } else {
          tokenizer.EnsureNextToken("FROM");
        }
      }
    }
    select.addCondition(ParseWhere(tokenizer, select));
    ParseGroupBy(select, tokenizer);
    ParseHaving(select, tokenizer);
    if (isSelectInto) {
      select = new SqlSelectIntoStatement(intoTable,
              externalDatabase,
              select.getColumns(),
              select.getHavingClause(),
              select.getCriteria(),
              select.getOrderBy(),
              select.getGroupBy(),
              select.getEachGroupBy(),
              select.getTables(),
              select.getParameterList(),
              select.getFromLast(),
              select.getLimitExpr(),
              select.getOffsetExpr(),
              select.isDistinct(),
              select.getDialectProcessor());
    }
    return select;
  }

  private static void parseSimpleWithPart(SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    tokenizer.EnsureNextToken("WITH");
    SqlCollection<SqlTable> withes = new SqlCollection<SqlTable>();
    parseWithClause(tokenizer, select.getDialectProcessor(), withes);
    select.setWithClause(withes);

    SqlToken tryNext = tokenizer.LookaheadToken2();
    if(tryNext.equals("SELECT")) {
      SqlQueryStatement query = parseSelectUnion(tokenizer, select.getDialectProcessor());

      if(query instanceof SqlSelectStatement) {
        SqlSelectStatement sub = (SqlSelectStatement) query;
        SqlCollection<SqlTable> tables = sub.getTables();

        for(SqlTable t: tables) {
          select.addTable(t);
        }
        select.setOrderBy(sub.getOrderBy());
        select.setColumns(sub.getColumns());
        select.setDistinct(sub.isDistinct());
        select.setFromLast(sub.getFromLast());
        select.setCriteria(sub.getCriteria());
        select.setLimitExpr(sub.getLimitExpr());
        select.setOffsetExpr(sub.getOffsetExpr());
        select.setHavingClause(sub.getHavingClause());
        select.setGroupByClause(sub.getGroupBy(), sub.getEachGroupBy());
      }
    } else {
      // CTE define must be used.
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNEXPECTED_TOKEN, tryNext.Value));
    }
  }

  private static SqlQueryStatement parseWithPart(Dialect dialect, SqlTokenizer tokenizer) throws Exception {
    tokenizer.EnsureNextToken("WITH");
    SqlCollection<SqlTable> withes = new SqlCollection<SqlTable>();
    parseWithClause(tokenizer, dialect, withes);

    SqlToken tryNext = tokenizer.LookaheadToken2();
    if(tryNext.equals("SELECT")) {
      SqlQueryStatement query = parseSelectUnion(tokenizer, dialect);
      query.setWithClause(withes);
      return query;
    } else {
      // CTE define must be used.
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNEXPECTED_TOKEN, tryNext.Value));
    }
  }

  private static void parseSelectSimpleFromPart(Dialect dialect, SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    parseTableReferences(dialect, select, tokenizer);
  }

  public static void parseTableReferences(Dialect dialect, SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    do {
      SqlTable table = readTable(select, tokenizer, dialect);
      parseJoinTable(table, select, tokenizer, dialect);
      SqlToken tryNext = tokenizer.LookaheadToken2();
      if (!tryNext.equals(",")) {
        break;
      } else {
        tokenizer.NextToken();
      }
    } while (true);
  }

  private static void parseJoinTable(SqlTable top, SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) throws Exception {
    top = readJoinTable(top, select, tokenizer, dialect);
    if (null == select.getTable()) {
      select.setTable(top);
      select.setTableName(top.getName());
    } else {
      select.addTable(top);
    }
  }

  private static SqlTable readTable(SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) throws Exception {
    SqlTable table = null;
    String tableName = null;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals(SqlToken.Open)) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("SELECT") || tryNext.equals("WITH")) {
        SqlQueryStatement query = parseSelectUnion(tokenizer, select.getDialectProcessor(), select.getParameterList().size());
        addParas2Parent(select.getParameterList(), query.getParameterList());
        tableName = DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX + tokenizer.currentPosition();
        table = new SqlTable(query, tableName, null);
      } else {
        //nested join SELECT * FROM (TableA A LEFT JOIN TableB B) LEFT JOIN TableC C
        table = readTable(select, tokenizer, dialect);
        table = readCrossApplyTable(table, select, tokenizer, dialect);
        table = readJoinTable(table, select, tokenizer, dialect);
        if (table.getJoin() != null || table.getQuery() != null || table.getNestedJoin() != null) {
          table = new SqlTable(null, null, DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX + tokenizer.currentPosition(), null, table);
        }
      }
      tableName = table.getName();
      tokenizer.EnsureNextToken(SqlToken.Close.Value);
    } else if (tryNext.equals(SqlToken.ESCInitiator)) {
      //support parsing odbc join style {oj ...}
      //http://msdn.microsoft.com/en-us/library/ms714641%28v=vs.85%29.aspx
      tokenizer.NextToken();
      tokenizer.EnsureNextIdentifier("oj");
      table = readTable(select, tokenizer, dialect);
      table = readCrossApplyTable(table, select, tokenizer, dialect);
      table = readJoinTable(table, select, tokenizer, dialect);
      tokenizer.EnsureNextToken(SqlToken.ESCTerminator.Value);
      return table;
    } else if (tryNext.equals("VALUES")) {
      //SELECT * FROM (VALUES(1,1),(2,2),(3,3)) AS MyTable(a, b);
      tokenizer.NextToken();
      SqlCollection<SqlExpression> list = new SqlCollection<SqlExpression>();
      do {
        SqlExpression expr = readExpression(tokenizer, select);
        list.add(expr);
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals(",")) {
          tokenizer.NextToken();
        } else {
          break;
        }
      } while (true);
      tableName = DERIVED_VALUES_TABLE_NAME_PREFIX;
      table = new SqlTable(tableName, tableName);
    } else if (isTableValuedFunction(tokenizer, select.getDialectProcessor())) {
      SqlFormulaColumn tableValueFunc = (SqlFormulaColumn)readExpression(tokenizer, select);
      tableName = tableValueFunc.getColumnName();
      table = new SqlTable(tableValueFunc);
    } else {
      if (tryNext == null || tryNext.OpenQuote == "\'" || tryNext.CloseQuote == "\'") {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_TABLENAME_BEFORE_WHERE));
      }
      String [] period = parsePeriodTableName(tokenizer, select.getDialectProcessor());
      tableName = period[2];
      if (period.length > 3) {
        StringBuilder sb = new StringBuilder();
        for (int i = 3 ; i < period.length; ++i) {
          sb.append(period[i]).append(SqlToken.Dot.Value);
        }
        sb.append(period[0]);
        table = new SqlTable(sb.toString(), period[1], tableName, tableName);
      } else {
        table = new SqlTable(period[0], period[1], tableName, tableName);
      }

    }
    String tableAlias = readFromAlias(tokenizer, tableName, select);
    table = new SqlTable(table.getCatalog(), table.getSchema(), table.getName(), tableAlias, table.getJoin(), table.getNestedJoin(), table.getQuery(), table.getTableValueFunction(), table.getCrossApply());
    table = readCrossApplyTable(table, select, tokenizer, dialect);
    return table;
  }

  private static String readFromAlias(SqlTokenizer tokenizer, String alias, SqlStatement stmt) throws Exception{
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("AS")) {
      tokenizer.NextToken();
      int pos = tokenizer.currentPosition();
      SqlExpression expr = readExpression(tokenizer, stmt);
      if (expr instanceof SqlFormulaColumn) {
        alias = ((SqlFormulaColumn) expr).getColumnName();
      } else {
        tokenizer.Backtrack(pos);
        alias = tokenizer.NextToken().Value;
      }
    } else if (tryNext.Kind == TokenKind.Identifier){
      if (!tryNext.IsEmpty() && !tryNext.equals("LEFT") && !tryNext.equals("RIGHT") && !tryNext.equals("FULL")) {
        SqlExpression expr = readExpression(tokenizer, stmt);
        if (expr instanceof SqlColumn) {
          alias = ((SqlColumn) expr).getColumnName();
        }
      }
    }
    return alias;
  }

  private static JoinType getJoinType(String join) {
    JoinType type;
    if (Utilities.equalIgnoreCase("LEFT", join)) {
      type = JoinType.LEFT;
    } else if (Utilities.equalIgnoreCase("RIGHT", join)) {
      type = JoinType.RIGHT;
    } else if (Utilities.equalIgnoreCase("FULL", join)) {
      type = JoinType.FULL;
    } else if (Utilities.equalIgnoreCase("INNER", join)) {
      type = JoinType.INNER;
    } else if (Utilities.equalIgnoreCase("JOIN", join)) {
      type = JoinType.INNER;
    } else if (Utilities.equalIgnoreCase("NATURAL", join)) {
      type = JoinType.NATURAL;
    } else if (Utilities.equalIgnoreCase("CROSS", join)) {
      type = JoinType.CROSS;
    } else {
      type = JoinType.NONE;
    }
    return type;
  }

  private static SqlTable readCrossApplyTable(SqlTable table, SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) throws Exception {
    SqlCrossApply ca = readCrossApply(tokenizer, select, dialect);
    if ( ca != null ) {
      return new SqlTable(table.getCatalog(), table.getSchema(), table.getName(), table.getAlias(), table.getJoin(), table.getNestedJoin(), table.getQuery(), table.getTableValueFunction(), ca);
    }
    return table;
  }

  private static SqlCrossApply readCrossApply(SqlTokenizer tokenizer, SqlSelectStatement select, Dialect dialect) throws Exception {
    int start = tokenizer.currentPosition();
    SqlToken next = tokenizer.LookaheadToken2();
    if (!Utilities.equalIgnoreCase("CROSS", next.Value))
      return null;

    tokenizer.NextToken();
    next = tokenizer.NextToken();
    if (!Utilities.equalIgnoreCase("APPLY", next.Value)) {
      tokenizer.Backtrack(start);
      return null;
    }

    // FUNCTION([alias.]ColumnName)
    if ( !isTableValuedFunction(tokenizer, dialect) ) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.CROSS_APPLY_INVALID_EXPRESSION));
    }
    SqlFormulaColumn function = (SqlFormulaColumn)readExpression(tokenizer, select);
    SqlCollection<SqlColumnDefinition> columnDefinitions = new SqlCollection<SqlColumnDefinition>();

    // columns definitions
    next = tokenizer.LookaheadToken2();
    if ( next.IsKeyword("WITH") ) {
      tokenizer.NextToken();
      parseCrossApplyColumnDefinitions(tokenizer, columnDefinitions, dialect);
    }

    String alias = readFromAlias(tokenizer, "", select);

    SqlCrossApply child = readCrossApply(tokenizer, select, dialect);
    SqlCrossApply ca = new SqlCrossApply(function, alias, columnDefinitions, child);
    return ca;
  }

  private static void parseCrossApplyColumnDefinitions(SqlTokenizer tokenizer, SqlCollection<SqlColumnDefinition> definitions, Dialect dialect) throws Exception {
    tokenizer.EnsureNextToken(SqlToken.Open.Value);
    SqlToken nextToken = tokenizer.LookaheadToken2();
    while (!nextToken.equals(SqlToken.Close)) {
      if (dialect != null) {
        SqlColumnDefinition COLUMN_DEF = dialect.parseColumnDefinition(tokenizer);
        if (COLUMN_DEF != null) {
          definitions.add(COLUMN_DEF);
          nextToken = tokenizer.LookaheadToken2();
          if(nextToken.equals(",")){
            tokenizer.NextToken();
            nextToken = tokenizer.LookaheadToken2();
            continue;
          } else{
            break;
          }
        }
      }

      SqlColumnDefinition columnDefinition = parseOneColumnDefinition(tokenizer, dialect);
      definitions.add(columnDefinition);
      nextToken = tokenizer.LookaheadToken2();
      if(nextToken.equals(",")){
        tokenizer.NextToken();
        nextToken = tokenizer.LookaheadToken2();
      } else{
        break;
      }
    }
    tokenizer.EnsureNextToken(SqlToken.Close.Value);
  }

  private static SqlColumn readCrossApplySourceColumn(SqlTokenizer tokenizer, SqlTable table) throws Exception {
    SqlToken id1 = tokenizer.NextIdentifier();
    SqlToken next = tokenizer.LookaheadToken2();
    SqlColumn result = null;
    if (next.equals(SqlToken.Dot)) {
      tokenizer.NextToken();
      SqlToken id2 = tokenizer.NextIdentifier();

      if (!Utilities.equalIgnoreCase(id1.Value, table.getAlias())) {
        throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.CROSS_APPLY_UNKNOWN_TABLE, id1.Value));
      }
      result = new SqlGeneralColumn(table, id2.Value);
    } else {
      result = new SqlGeneralColumn(table, id1.Value);
    }
    return result;
  }


  private static SqlTable readJoinTable(SqlTable top, SqlSelectStatement select, SqlTokenizer tokenizer, Dialect dialect) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    SqlTable last = top;
    Vector<SqlTable> joins = new Vector<SqlTable>();
    while (true) {
      JoinType type = getJoinType(tryNext.Value);
      boolean implicitInner = Utilities.equalIgnoreCase("JOIN", tryNext.Value) ? true : false;
      SqlConditionNode condition = null;
      SqlJoin join = null;
      SqlTable right = null;
      if (type != JoinType.NONE) {
        tokenizer.NextToken();
        boolean hasOuter = false;
        if (JoinType.LEFT == type || JoinType.RIGHT == type || JoinType.FULL == type) {
          tryNext = tokenizer.LookaheadToken2();
          if (tryNext.equals("OUTER")) {
            hasOuter = true;
            tokenizer.NextToken();
          }
        }
        if (!implicitInner) tokenizer.EnsureNextToken("JOIN");
        boolean isEach = false;
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("EACH")) {
          tokenizer.NextToken();
          isEach = true;
        }
        right = readTable(select, tokenizer, dialect);
        right = readJoinTable(right, select, tokenizer, dialect);
        tryNext = tokenizer.LookaheadToken2();
        if (type != JoinType.CROSS && tryNext.equals("ON")) {
          tokenizer.NextToken();
          condition = (SqlConditionNode)readExpression(tokenizer, select);
        }

        if (right.hasJoin()) {
          SqlTable wrapperToNested = new SqlTable(null, right);
          join = new SqlJoin(type, wrapperToNested, condition, isEach, hasOuter);
        } else {
          join = new SqlJoin(type, right, condition, isEach, hasOuter);
        }
      } else {
        break;
      }
      last = new SqlTable(last.getCatalog(), last.getSchema(), last.getName(), last.getAlias(), join, last.getNestedJoin(), last.getQuery(), last.getTableValueFunction(), last.getCrossApply());
      joins.add(last);
      last = right;
      tryNext = tokenizer.LookaheadToken2();
    }

    SqlTable lastJoin = top;
    if (joins.size() > 0) {
      lastJoin = joins.get(joins.size() - 1);
    }
    SqlTable newTop = lastJoin;
    for (int i = joins.size() - 1; i >= 1; --i) {
      SqlTable t = joins.get(i - 1);
      newTop = new SqlTable(t.getCatalog(), t.getSchema(), t.getName(), t.getAlias(), new SqlJoin(t.getJoin().getJoinType(), lastJoin, t.getJoin().getCondition(), t.getJoin().isEach(), t.getJoin().hasOuter()), t.getNestedJoin(), t.getQuery(), t.getTableValueFunction(), t.getCrossApply());
      lastJoin = newTop;
    }
    top = newTop;
    return top;
  }

  private static void parseSelectSimpleSelectPart(SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    Dialect dialect = select.getDialectProcessor();
    if (dialect != null) {
      SqlExpression option = dialect.readOption(tokenizer);
      if (option != null) {
        select.setOption(option);
      } else {
        ParseOptions(select, tokenizer);
      }
    } else {
      ParseOptions(select, tokenizer);
    }
    select.setColumns(parseColumns(tokenizer, select));
  }

  private static void ParseOptions(SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals("_LAST_")) {
      tokenizer.NextToken();
      select.setFromLast(true);
    } else if (next.equals("TOP")) {
      ParseTop(select, tokenizer);
    } else if (next.equals("DISTINCT")) {
      tokenizer.NextToken();
      select.setDistinct(true);
    }
  }

  private static void ParseTop(SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    int pos = tokenizer.currentPosition();
    tokenizer.NextToken(); // Consume 'TOP'
    SqlExpression limit = readTerm(tokenizer, select);
    if (limit instanceof SqlExpressionList) {
      tokenizer.Backtrack(pos);
    } else {
      select.setLimitExpr(limit);
    }
  }

  private static void ParseHaving(SqlSelectStatement select, SqlTokenizer tokenizer) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("HAVING")) {
      tokenizer.NextToken();
      select.setHavingClause((SqlConditionNode) readExpression(tokenizer, select));
    }
  }

  private static SqlQueryStatement parseSelectUnionExtension(SqlQueryStatement left, SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    SqlSelectUnionStatement union;
    SqlToken tryNext = tokenizer.LookaheadToken2();
    while (true) {
      if (tryNext.equals("UNION")) {
        union = new SqlSelectUnionStatement(left, dialectProcessor);
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("ALL")) {
          union.setUnionType(UnionType.UNION_ALL);
          tokenizer.NextToken();
        } else if (tryNext.equals("DISTINCT")) {
          union.setUnionType(UnionType.UNION);
          tokenizer.NextToken();
        } else {
          union.setUnionType(UnionType.UNION);
        }
        union.setRight(ParserCore.parseSelectSub(tokenizer, dialectProcessor, union.getParameterList().size()));
        left = union;
      } else if (tryNext.equals("MINUS") || tryNext.equals("EXCEPT")) {
        union = new SqlSelectUnionStatement(left, dialectProcessor);
        tokenizer.NextToken();
        union.setUnionType(UnionType.EXCEPT);
        union.setRight(ParserCore.parseSelectSub(tokenizer, dialectProcessor, union.getParameterList().size()));
        left = union;
      } else if (tryNext.equals("INTERSECT")) {
        union = new SqlSelectUnionStatement(left, dialectProcessor);
        tokenizer.NextToken();
        union.setUnionType(UnionType.INTERSECT);
        union.setRight(ParserCore.parseSelectSub(tokenizer, dialectProcessor, union.getParameterList().size()));
        left = union;
      } else {
        break;
      }
      tryNext = tokenizer.LookaheadToken2();
    }
    parseEndofQuery(left, tokenizer);
    return left;
  }

  private static void parseLimit(SqlQueryStatement query, SqlTokenizer tokenizer) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("LIMIT")) {
      tokenizer.NextToken();
      SqlExpression limit = ParserCore.readExpression(tokenizer, query);
      query.setLimitExpr(limit);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("OFFSET")) {
        tokenizer.NextToken();
        SqlExpression offset = ParserCore.readExpression(tokenizer, query);
        query.setOffsetExpr(offset);
      } else if (tryNext.equals(",")) {
        tokenizer.NextToken();
        SqlExpression offset = limit;
        limit = ParserCore.readExpression(tokenizer, query);
        query.setOffsetExpr(offset);
        query.setLimitExpr(limit);
      }
    }
  }

  private static void parseUpdatability(SqlQueryStatement query, SqlTokenizer tokenizer) throws Exception {
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("FOR")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("READ")) {
        tokenizer.NextToken();
        tokenizer.EnsureNextToken("ONLY");
        query.setUpdatability(new SqlUpdatability(SqlUpdatability.READ_ONLY));
      } else {
        tokenizer.EnsureNextToken("UPDATE");
        tryNext = tokenizer.LookaheadToken2();
        if (tryNext.equals("OF")) {
          tokenizer.NextToken();
          SqlCollection<SqlColumn> columns = parseColumns(tokenizer, query);
          query.setUpdatability(new SqlUpdatability(SqlUpdatability.UPDATE, columns));
        } else {
          query.setUpdatability(new SqlUpdatability(SqlUpdatability.UPDATE));
        }
      }
    }
  }

  private static void parseEndofQuery(SqlQueryStatement query, SqlTokenizer tokenizer) throws Exception{
    parseOrderBy(query, tokenizer);
    parseLimit(query, tokenizer);
    parseUpdatability(query, tokenizer);
    ParserCore.parseComment(query, tokenizer);
  }

  private static boolean isFunctionEscape(SqlTokenizer tokenizer) throws Exception {
    //https://msdn.microsoft.com/en-us/library/ms709434(v=vs.85).aspx
    boolean isFunctionEsc = false;
    int start = tokenizer.currentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Kind == TokenKind.ESCInitiator) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Identifier && tryNext.equals("fn")) {
        isFunctionEsc = true;
      }
    }
    tokenizer.Backtrack(start);
    return isFunctionEsc;
  }

  private static boolean isLikeEscape(SqlTokenizer tokenizer) throws Exception {
    //https://msdn.microsoft.com/en-us/library/ms710128(v=vs.85).aspx
    boolean isLikeEsc = false;
    int start = tokenizer.currentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Kind == TokenKind.ESCInitiator) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Identifier && tryNext.equals("escape")) {
        isLikeEsc = true;
      }
    }
    tokenizer.Backtrack(start);
    return isLikeEsc;
  }

  private static boolean isDateValueEscape(SqlTokenizer tokenizer) throws Exception {
    //https://msdn.microsoft.com/en-us/library/ms710282(v=vs.85).aspx
    boolean isDateValueEsc = false;
    int start = tokenizer.currentPosition();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Kind == TokenKind.ESCInitiator) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Kind == TokenKind.Identifier) {
        if (tryNext.equals("d") || tryNext.equals("t") || tryNext.equals("ts"))
          isDateValueEsc = true;
      }
    }
    tokenizer.Backtrack(start);
    return isDateValueEsc;
  }

  private static void addParas2Parent(SqlCollection<SqlValueExpression> parentParas, SqlCollection<SqlValueExpression> subParas) {
    for (SqlValueExpression p : subParas) {
      parentParas.add(p);
    }
  }

  private static void parseWithClause(SqlTokenizer tokenizer, Dialect dialectProcessor, SqlCollection<SqlTable> withes) throws Exception {
    boolean finished = false;

    while (!finished) {
      SqlToken token = tokenizer.NextToken();
      String tableName = token.Value;

      // SKIP CTE columns
      token = tokenizer.LookaheadToken2();
      if(token.equals("(")) {
        tokenizer.NextToken();
        while (!token.equals(")")) {
          continue;
        }
      }

      tokenizer.EnsureNextToken("AS");
      SqlQueryStatement sub = ParserCore.parseSelectUnion(tokenizer, dialectProcessor);
      withes.add(new SqlTable(sub, tableName, tableName));

      SqlToken next = tokenizer.LookaheadToken2();
      if(!next.equals(",")) {
        finished = true;
      } else {
        tokenizer.NextToken();
      }
    }
  }
}

