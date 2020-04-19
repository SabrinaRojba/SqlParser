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
using System.Collections.Generic;

public sealed class SqlNormalization {
  private const string CRITERIA_SUBQUERY_EXCEPTION_CODE = "normalizeCriteriaSubQuery";
  private const string TABLE_EXPRESSION_EXCEPTION_CODE = "normalizeTableExprWithQuery";
  private const string NESTED_QUERY_TABLE_NAME_EXCEPTION_CODE = "normalizeNestedQueryTableName";
  private const string IMPLICIT_JOIN_EXCEPTION_CODE = "normalizeImplicitJoin";
  private const string NESTED_JOIN_EXCEPTION_CODE = "normalizeNestedJoin";
  private const string NORMALIZATION_EXCEPTION_CODE = "normalize";
  private SqlNormalizationException _throwEx;

  public SqlStatement NormalizeStatement(NormalizationOption option, SqlStatement stmt, IDataMetadata dataMetadata) {
    JavaArrayList<Exception> exceptions = new JavaArrayList<Exception>();
    SqlStatement normalized = Normalize(option, stmt, dataMetadata, exceptions);
    if (exceptions.Size() > 0) {
      this._throwEx = new SqlNormalizationException(NORMALIZATION_EXCEPTION_CODE, exceptions);
    }
    return normalized;
  }

  public Exception GetException() {
    return this._throwEx;
  }

  public static SqlStatement Normalize(NormalizationOption option, SqlStatement stmt, IDataMetadata dataMetaData) {
    JavaArrayList<Exception> exceptions = new JavaArrayList<Exception>();
    SqlStatement normalized = Normalize(option, stmt, dataMetaData, exceptions);
    if (exceptions.Size() > 0) {
      SqlNormalizationException throwEx = new SqlNormalizationException(NORMALIZATION_EXCEPTION_CODE, exceptions);
      throw throwEx;
    }

    return normalized;
  }

  private static SqlStatement Normalize(NormalizationOption option, SqlStatement stmt, IDataMetadata dataMetaData, IList<Exception> exceptions) {
    SqlStatement simplified = stmt;
    JavaArrayList<NormalizationOptions> configs = BuildConfigedOptions(option);
    bool support = stmt is SqlQueryStatement;
    if (support) {
      for (int i = 0 ; i < configs.Size(); ++i) {
        try {
          simplified = Normalize(configs.Get(i), simplified, dataMetaData);
        } catch (Exception ex) {
          exceptions.Add(ex);
        }
      }
    }
    return simplified;
  }

  private static SqlStatement Normalize(NormalizationOptions options, SqlStatement stmt, IDataMetadata dataMetaData) {
    SqlStatement simplified = stmt;
    if (!IsStandardQuery(simplified)) {
      if (NormalizationOptions.FunctionSubstitute == options) {
        simplified = NormalizedFunctionSubstitute((SqlQueryStatement) simplified);
      } else if (NormalizationOptions.OperationExpression == options) {
        simplified = NormalizeOperationExpression((SqlQueryStatement)simplified);
      }
      return simplified;
    }
    if (NormalizationOptions.ImplicitCrossJoin == options) {
      simplified = NormalizeImplicitJoin((SqlQueryStatement) simplified, JoinType.CROSS);
    } else if (NormalizationOptions.ImplicitNaturalJoin == options) {
      simplified = NormalizeImplicitJoin((SqlQueryStatement)simplified, JoinType.NATURAL);
    } else if (NormalizationOptions.ImplicitCommaJoin == options) {
      simplified = NormalizeImplicitJoin((SqlQueryStatement)simplified, JoinType.COMMA);
    } else if (NormalizationOptions.ImplicitInnerJoin == options) {
      simplified = NormalizeImplicitJoin((SqlQueryStatement)simplified, JoinType.INNER);
    } else if (NormalizationOptions.EquiInnerJoin == options) {
      simplified = NormalizeEquiInnerJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.TableExprWithQuery == options && ContainsNestedQuery(stmt.GetTable())) {
      simplified = NormalizeTableExprWithQuery((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.CriteriaInJoin == options) {
      simplified = NormalizeCriteriaInJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.ResolveTableAlias == options) {
      simplified = NormalizeTableAlias((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.CriteriaWithSubQuery == options) {
      simplified = NormalizeCriteriaWithSubQuery((SqlQueryStatement)simplified, JoinType.INNER);
    } else if (NormalizationOptions.CriteriaWithNot == options) {
      simplified = NormalizeCriteriaWithNot((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.Criteria == options) {
      simplified = NormalizeCriteria((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.FixUniqueAlias == options) {
      simplified = NormalizeFixUniqueAlias((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.OperationExpression == options) {
      simplified = NormalizeOperationExpression((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.AppendNestedQueryTableName == options) {
      simplified = NormalizeNestedQueryTableName((SqlQueryStatement)simplified, dataMetaData);
    } else if (NormalizationOptions.RightJoin == options) {
      simplified = NormalizeRightJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.MinimizeCriteria == options) {
      simplified = NormalizeMinimizeCriteria((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.ConstantColumn == options) {
      simplified = NormalizeConstantColumn((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.FormulaAlias == options) {
      simplified = NormalizeUnnamedColumn((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.NestedJoin == options) {
      simplified = NormalizeNestedJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.Distinct == options) {
      simplified = NormalizeDistinct((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.Count_Distinct == options) {
      simplified = NormalizeCountDistinct((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.PredictTrue == options) {
      simplified = NormalizePredictTrue((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.SemiAntiJoin == options) {
      simplified = NormalizeCriteriaWithSubQuery((SqlQueryStatement)simplified, JoinType.LEFT_SEMI);
    } else if (NormalizationOptions.FunctionSubstitute == options) {
      simplified = NormalizedFunctionSubstitute((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.RemoveDistinctIfColumnUnique == options) {
      simplified = NormalizeRemoveDistinctIfColumnUnique((SqlQueryStatement) simplified, dataMetaData);
    }
    return simplified;
  }

  private static SqlQueryStatement NormalizedFunctionSubstitute(SqlQueryStatement statement) {
    if (statement is SqlSelectUnionStatement) {
      SqlSelectUnionStatement union = (SqlSelectUnionStatement) statement;
      SqlQueryStatement left = union.GetLeft();
      SqlQueryStatement right = union.GetRight();
      NormalizedFunctionSubstitute(left);
      NormalizedFunctionSubstitute(right);
      return statement;
    }
    FunctionModifier funcModifier = new FunctionModifier();
    SqlParser parser = new SqlParser(statement);
    funcModifier.Modify(parser);
    SqlSelectStatement select = (SqlSelectStatement) statement;
    return select;
  }

  private static SqlQueryStatement NormalizeRemoveDistinctIfColumnUnique(SqlQueryStatement statement, IDataMetadata dataMetadata) {
    RemoveDistinctVisitor removeDistinctVisitor = new RemoveDistinctVisitor(dataMetadata);
    statement.Accept(removeDistinctVisitor);
    return statement;
  }

  private static SqlStatement NormalizeUnnamedColumn(SqlQueryStatement statement) {
    if (statement is SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) statement;
      SqlCollection<SqlColumn> columns = select.GetColumns();
      FixUnnamedColumnAlias(columns);
    } else if (statement is SqlSelectUnionStatement){
      SqlSelectUnionStatement unionStatement = (SqlSelectUnionStatement) statement;
      SqlQueryStatement left = unionStatement.GetLeft();
      NormalizeUnnamedColumn(left);
      SqlQueryStatement right = unionStatement.GetRight();
      NormalizeUnnamedColumn(right);
    }
    return statement;
  }

  public static void FixUnnamedColumnAlias(SqlCollection<SqlColumn> columns) {
    int incrementNum = 0;

    for (int i = 0 ; i < columns.Size(); ++i) {
      SqlColumn col = columns.Get(i);
      if (col is SqlConstantColumn) {
        ATATVariableSubstitue ATAT = new ATATVariableSubstitue();
        string alias = null;
        if (null == col.GetAlias()) {
          if (ATAT.Match(col)) {
            SqlValueExpression p = (SqlValueExpression) col.GetExpr();
            string pn = p.GetParameterName();
            alias = RSSBus.core.j2cs.Converter.GetSubstring(pn, 2);
          } else {
            alias = "Column" + (incrementNum++);
          }
          SqlConstantColumn c = (SqlConstantColumn)col;
          col = new SqlConstantColumn(alias, (SqlValueExpression)c.GetExpr());
          columns.Set(i, col);
        }
      } else if (col is SqlOperationColumn) {
        if (null == col.GetAlias()) {
          string alias = "Column" + (incrementNum++);
          SqlOperationColumn c = (SqlOperationColumn)col;
          col = new SqlOperationColumn(alias, (SqlOperationExpression)c.GetExpr());
          columns.Set(i, col);
        }
      } else if (col is SqlSubQueryColumn) {
        if (null == col.GetAlias()) {
          string alias = "Column" + (incrementNum++);
          SqlSubQueryColumn c = (SqlSubQueryColumn)col;
          col = new SqlSubQueryColumn(alias, (SqlSubQueryExpression)c.GetExpr());
          columns.Set(i, col);
        }
      } else {
        continue;
      }
    }
  }

  public static void NormalizeFormulaAlias(SqlCollection<SqlColumn> columns) {
    JavaArrayList<SqlFormulaColumn> functions = new JavaArrayList<SqlFormulaColumn>();
    for (int i = 0 ; i < columns.Size(); ++i) {
      SqlColumn column = columns.Get(i);
      if (column is SqlFormulaColumn) {
        functions.Add((SqlFormulaColumn) column);
      }
    }

    for (int i = 0 ; i < columns.Size(); ++i) {
      SqlColumn column = columns.Get(i);
      if (column is SqlFormulaColumn) {
        SqlFormulaColumn formulaColumn = (SqlFormulaColumn) column;
        if (!formulaColumn.HasAlias()) {
          int matched = 0;
          for (int j = 0 ; j < functions.Size(); ++j) {
            SqlFormulaColumn f = functions.Get(j);
            if (Utilities.EqualIgnoreCase(f.GetColumnName(), formulaColumn.GetColumnName())) {
              ++matched;
            }
          }
          if (1 == matched) {
            SqlFormulaColumn tempFormula = new SqlFormulaColumn(formulaColumn.GetColumnName(),
                    formulaColumn.GetColumnName(),
                    formulaColumn.GetParameters(),
                    formulaColumn.GetOverClause());
            columns.Set(i, tempFormula);
          }
        }
      } else {
        continue;
      }
    }
  }

  private static void FixUniqueColumnAlias(SqlCollection<SqlColumn> columns, SqlTable mainTable) {
    if (columns == null) return;
    for (int i = columns.Size() - 1 ; i >= 0; --i) {
      SqlColumn col = columns.Get(i);
      if ((col is SqlGeneralColumn && !(col is SqlWildcardColumn)) || col is SqlFormulaColumn) {
        string alias = col.GetAlias();
        int match = 0;
        if (alias != null) {
          for (int j = 0 ; j < columns.Size(); ++j) {
            SqlColumn c = columns.Get(j);
            if (j == i) continue;
            if (c.GetAlias() != null) {
              bool matched = false;
              if (alias.Equals(c.GetAlias())) {
                if (c.GetTable() != null && col.GetTable() != null) {
                  string t1 = c.GetTable().GetAlias();
                  string t2 = col.GetTable().GetAlias();
                  if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(t1, t2)) {
                    matched = true;
                  }
                } else if (c.GetTable() != null) {
                  string t1 = c.GetTable().GetAlias();
                  string t2 = mainTable.GetAlias();
                  if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(t1, t2)) {
                    matched = true;
                  }
                } else if (col.GetTable() != null) {
                  string t1 = col.GetTable().GetAlias();
                  string t2 = mainTable.GetAlias();
                  if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(t1, t2)) {
                    matched = true;
                  }
                } else {
                  matched = true;
                }
              }
              if (matched) {
                match++;
              }
            }
          }
        }
        if (match > 0) {
          string aliasName = alias + match;
          if (col is SqlGeneralColumn) {
            col = new SqlGeneralColumn(col.GetTable(), col.GetColumnName(), aliasName, col.GetValueExpr());
          } else if (col is SqlFormulaColumn) {
            col = new SqlFormulaColumn(col.GetColumnName(),
                    aliasName,
                    ((SqlFormulaColumn)col).GetParameters(),
                    ((SqlFormulaColumn) col).GetOverClause());
          }
          columns.Set(i, col);
        }
      } else {
        continue;
      }
    }
  }

  public static SqlCollection<SqlColumn> GetPrimitiveColumns(SqlCollection<SqlColumn> columns) {
    SqlCollection<SqlColumn> primitiveColumns = new SqlCollection<SqlColumn>();
    foreach(SqlColumn c in columns) {
      if (c is SqlGeneralColumn) {
        primitiveColumns.Add(c);
      }
    }
    return primitiveColumns;
  }

  public static SqlColumn ResolveColumnAlias(SqlColumn c, SqlCollection<SqlColumn> primitiveColumns) {
    SqlColumn resolved = c;
    if (c is SqlFormulaColumn) {
      SqlFormulaColumn fcol = (SqlFormulaColumn)c;
      SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
      bool modified = false;
      foreach(SqlExpression para in fcol.GetParameters()) {
        if (para is SqlColumn) {
          SqlColumn resolvedPara = ResolveColumnAlias((SqlColumn) para, primitiveColumns);
          paras.Add(resolvedPara);
          if (resolvedPara != para) {
            modified = true;
          }
        } else if (para is SqlConditionNode) {
          SqlConditionNode resolvedC = ResolveColumnAliasInCriteria((SqlConditionNode) para, primitiveColumns);
          paras.Add(resolvedC);
          if (resolvedC != para) {
            modified = true;
          }
        } else {
          paras.Add(para);
        }
      }
      if (modified) {
        resolved = new SqlFormulaColumn(fcol.GetColumnName(),
                fcol.GetAlias(),
                paras,
                fcol.GetOverClause());
      } else {
        return fcol;
      }
    } else if (c is SqlOperationColumn) {
      SqlOperationColumn opcol = (SqlOperationColumn)c;
      SqlOperationExpression opExpr = (SqlOperationExpression)opcol.GetExpr();
      SqlExpression left = opExpr.GetLeft();
      SqlExpression right = opExpr.GetRight();
      if (left is SqlColumn) {
        left = ResolveColumnAlias((SqlColumn)left, primitiveColumns);
      }
      if (right is SqlColumn) {
        right = ResolveColumnAlias((SqlColumn)right, primitiveColumns);
      }
      resolved = new SqlOperationColumn(opcol.GetAlias(), new SqlOperationExpression(opExpr.GetOperator(), left, right));
    } else if (c is SqlWildcardColumn) {
      //do nothing.
    } else if (c is SqlGeneralColumn) {
      SqlGeneralColumn column = (SqlGeneralColumn)c;
      if (column.HasAlias()) {
        resolved = new SqlGeneralColumn(column.GetTable(), column.GetColumnName());
      } else {
        SourceColumnComparer cc = new SourceColumnComparer();
        SqlColumn matched = null;
        foreach(SqlColumn sc in primitiveColumns) {
          if (cc.Compare(column, sc)) {
            matched = sc;
            break;
          }
        }

        if (matched is SqlGeneralColumn) {
          if (column.GetTable() != null) {
            resolved = new SqlGeneralColumn(column.GetTable(), matched.GetColumnName());
          } else {
            resolved = new SqlGeneralColumn(matched.GetTable(), matched.GetColumnName());
          }
        } else if (matched is SqlFormulaColumn && matched.HasAlias()) {
          resolved = new SqlFormulaColumn(matched.GetColumnName(),
              ((SqlFormulaColumn) matched).GetParameters(),
              ((SqlFormulaColumn) matched).GetOverClause());
        } else if (matched != null) {
          resolved = matched;
        }
      }
    } else if (c is SqlConstantColumn) {
      //do nothing.
    } else if (c is SqlSubQueryColumn) {
      //do nothing.
    }
    return resolved;
  }

  private static SqlQueryStatement NormalizePredictTrue(SqlQueryStatement query) {
    SqlQueryStatement resolved = query;
    SqlConditionNode where = query.GetCriteria();
    if (where == null) return resolved;
    PredictTrueModifier modifier = new PredictTrueModifier();
    modifier.Modify(where);
    resolved.SetCriteria((SqlConditionNode) modifier.GetModifiedExpr());
    return resolved;
  }

  private static SqlQueryStatement NormalizeDistinct(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) query;

      if (select.IsDistinct() && ContainsAggragation(select.GetColumns())) {
        SqlCollection<SqlColumn> outerColumns = new SqlCollection<SqlColumn>();
        SqlCollection<SqlColumn> innerColumns = new SqlCollection<SqlColumn>();
        foreach(SqlColumn c in select.GetColumns()) {
          if (c is SqlFormulaColumn) {
            innerColumns.Add(c);
            outerColumns.Add(new SqlGeneralColumn(c.GetAlias()));
            continue;
          }
          innerColumns.Add(c);
          outerColumns.Add(c);
        }
        SqlCollection<SqlExpression> outerGroupBy = new SqlCollection<SqlExpression>();
        foreach(SqlColumn c in outerColumns) {
          outerGroupBy.Add(c);
        }
        SqlSelectStatement nestedQuery = new SqlSelectStatement(innerColumns,
                select.GetHavingClause(),
                select.GetCriteria(),
                select.GetOrderBy(),
                select.GetGroupBy(),
                select.GetEachGroupBy(),
                select.GetTable(),
                select.GetParameterList(),
                select.GetFromLast(),
                select.GetLimitExpr(),
                select.GetOffsetExpr(),
                false,
                select.GetDialectProcessor());
        SqlTable nestTable = new SqlTable(nestedQuery, select.GetTable().GetAlias());

        select.SetColumns(outerColumns);
        select.SetGroupByClause(outerGroupBy, select.GetEachGroupBy());
        select.SetTable(nestTable);
        select.SetDistinct(false);
        select.SetCriteria(null);
        return select;
      }

      if (select.IsDistinct()) {
        SqlCollection<SqlExpression> groupBy = select.GetGroupBy();
        foreach(SqlColumn c in select.GetColumns()) {
          if (c.HasAlias()) {
            groupBy.Add(new SqlGeneralColumn(c.GetAlias()));
          } else {
            groupBy.Add(c);
          }
        }
        select.SetDistinct(false);
        select.SetGroupByClause(groupBy, select.GetEachGroupBy());
      }

      SqlTable t = select.GetTable();

      do {
        if (t.IsNestedQueryTable()) {
          SqlQueryStatement nq = t.GetQuery();
          NormalizeDistinct(nq);
        } else if (t.IsNestedJoinTable()) {
          SqlTable nt = t.GetNestedJoin();
          SqlSelectStatement temp = new SqlSelectStatement(null);
          temp.SetTable(nt);
          SqlCollection<SqlColumn> tempC = new SqlCollection<SqlColumn>();
          tempC.Add(new SqlWildcardColumn());
          temp.SetColumns(tempC);
          NormalizeDistinct(temp);
        }

        if (t.HasJoin()) {
          SqlJoin j = t.GetJoin();
          t = j.GetTable();
        } else {
          break;
        }
      } while(true);

      return select;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeCountDistinct(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) query;

      if (ContainsAggragation(select.GetColumns())) {
        for (int i = 0 ; i < select.GetColumns().Size(); ++i) {
          SqlColumn c = select.GetColumns().Get(i);
          if (!(c is SqlFormulaColumn)) {
            continue;
          }

          if (!Utilities.EqualIgnoreCase("COUNT", c.GetColumnName())) {
            continue;
          }

          SqlFormulaColumn COUNT_FUNC = (SqlFormulaColumn) c;
          SqlExpression P1 = COUNT_FUNC.GetParameters().Get(0);

          if (!(P1 is SqlFormulaColumn)) {
            continue;
          }

          SqlFormulaColumn DISTINCT_FUNC = (SqlFormulaColumn) P1;
          if (Utilities.EqualIgnoreCase("DISTINCT", DISTINCT_FUNC.GetColumnName())) {
            SqlFormulaColumn COUNT_DISTINCT_FUNC = new SqlFormulaColumn("COUNT_DISTINCT", COUNT_FUNC.GetAlias(), DISTINCT_FUNC.GetParameters());
            select.GetColumns().Set(i, COUNT_DISTINCT_FUNC);
          }
        }
      }

      SqlTable t = select.GetTable();

      do {
        if (t.IsNestedQueryTable()) {
          SqlQueryStatement nq = t.GetQuery();
          NormalizeDistinct(nq);
        } else if (t.IsNestedJoinTable()) {
          SqlTable nt = t.GetNestedJoin();
          SqlSelectStatement temp = new SqlSelectStatement(null);
          temp.SetTable(nt);
          SqlCollection<SqlColumn> tempC = new SqlCollection<SqlColumn>();
          tempC.Add(new SqlWildcardColumn());
          temp.SetColumns(tempC);
          NormalizeCountDistinct(temp);
        }

        if (t.HasJoin()) {
          SqlJoin j = t.GetJoin();
          t = j.GetTable();
        } else {
          break;
        }
      } while(true);

      return select;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeFixUniqueAlias(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();
      FixUniqueColumnAlias(columns, stmt.GetTable());
      stmt.SetColumns(columns);
      return stmt;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeOperationExpression(SqlQueryStatement query) {
    OperationColumnModifier modifier = new OperationColumnModifier();
    SqlParser parser = new SqlParser(query);
    modifier.Modify(parser);
    return parser.GetSelect();
  }

  private static SqlTable ConcatJoinTable(SqlTable l, SqlTable r, JoinType type, SqlConditionNode criteria) {
    SqlTable newTop = l;
    if (l.GetJoin() != null) {
      SqlJoin j = l.GetJoin();
      SqlTable nr = ConcatJoinTable(j.GetTable(), r, type, criteria);
      SqlJoin nj = new SqlJoin(j.GetJoinType(), nr, j.GetCondition(), j.IsEach(), j.HasOuter());
      newTop = new SqlTable(newTop.GetCatalog(),
              newTop.GetSchema(),
              newTop.GetName(),
              newTop.GetAlias(),
              nj,
              newTop.GetNestedJoin(),
              newTop.GetQuery(),
              newTop.GetTableValueFunction(),
              newTop.GetCrossApply());

    } else {
      SqlJoin j = new SqlJoin(type, r, criteria);
      newTop = new SqlTable(newTop.GetCatalog(),
              newTop.GetSchema(),
              newTop.GetName(),
              newTop.GetAlias(),
              j,
              newTop.GetNestedJoin(),
              newTop.GetQuery(),
              newTop.GetTableValueFunction(),
              newTop.GetCrossApply());
    }
    return newTop;
  }

  private static SqlCriteria[] ResolveEquiJoinCriteria(SqlConditionNode[] inOutWherem, SqlCollection<SqlTable> tables) {
    SqlCriteria[] criterias = new SqlCriteria[tables.Size() - 1];
    for(int index = 0; (index + 1) < tables.Size(); ) {
      criterias[index] = ResolveEquiJoinCriteria(inOutWherem, tables.Get(index), tables.Get(++index));
    }
    return criterias;
  }

  private static SqlCriteria ResolveEquiJoinCriteria(SqlConditionNode [] inOutWhere, SqlTable left, SqlTable right) {
    SqlCriteria equiOn = null;
    bool isEquiJoin = false;
    SqlConditionNode where = inOutWhere[0];
    if (where is SqlCondition) {
      SqlCondition andCond = (SqlCondition) where;
      if (andCond.GetLogicOp() == SqlLogicalOperator.And) {
        SqlExpression l = andCond.GetLeft();
        SqlExpression r = andCond.GetRight();
        if (l is SqlConditionNode) {
          isEquiJoin = SqlNormalizationHelper.IsEquiJoinCriteria((SqlConditionNode) l, left, right);
        }
        if (isEquiJoin) {
          inOutWhere[0] = (SqlConditionNode) l;
          equiOn = ResolveEquiJoinCriteria(inOutWhere, left, right);
          if (inOutWhere[0] != null) {
            inOutWhere[0] = new SqlCondition(inOutWhere[0], SqlLogicalOperator.And, r);
          } else {
            inOutWhere[0] = (SqlConditionNode) r;
          }

          return equiOn;
        }

        if (r is SqlConditionNode) {
          isEquiJoin = SqlNormalizationHelper.IsEquiJoinCriteria((SqlConditionNode) r, left, right);
        }
        if (isEquiJoin) {
          inOutWhere[0] = (SqlConditionNode) r;
          equiOn = ResolveEquiJoinCriteria(inOutWhere, left, right);
          if (inOutWhere[0] != null) {
            inOutWhere[0] = new SqlCondition(l, SqlLogicalOperator.And, inOutWhere[0]);
          } else {
            inOutWhere[0] = (SqlConditionNode) l;
          }
          return equiOn;
        }
        inOutWhere[0] = where;
      }
    } else if (where is SqlCriteria) {
      isEquiJoin = SqlNormalizationHelper.IsEquiJoinCriteria((SqlCriteria)where, left, right);
      if (isEquiJoin) {
        equiOn = (SqlCriteria)where;
        inOutWhere[0] = null;
      }
    }
    return equiOn;
  }

  private static SqlQueryStatement NormalizeCriteriaInJoin(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlTable mainTable = stmt.GetTable();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();

      SqlCollection<SqlJoin> joins = stmt.GetJoins();
      bool onlyInnerJoin = true;
      if (joins != null) {
        foreach(SqlJoin join in joins) {
          if (JoinType.INNER != join.GetJoinType()) {
            onlyInnerJoin = false;
            break;
          }
        }
      }

      if (!onlyInnerJoin) {
        return query;
      }

      if (SqlNormalizationHelper.IsStandardCriteriaInJoin(mainTable.GetJoin())) {
        return query;
      }

      SqlTable [] mainTableInOut = {mainTable};
      SqlConditionNode [] whereInOut = {condition};
      StandardJoin(mainTableInOut, whereInOut);

      SqlTable aliasMainTable = new SqlTable(mainTable.GetAlias());

      SqlCollection<SqlTable> OWNERS = new SqlCollection<SqlTable>();
      OWNERS.Add(aliasMainTable);

      AttachOwnerTableForColumns(OWNERS, columns);

      orderBy = AttachOwnerTableForOrderBy(orderBy, OWNERS);

      groupBy = AttachOwnerTableForGroupBy(groupBy, OWNERS);

      havingClause = AttachTableForCriteria(OWNERS, havingClause);

      condition = AttachTableForCriteria(OWNERS, whereInOut[0]);

      stmt.SetColumns(columns);
      stmt.SetHavingClause(havingClause);
      stmt.SetCriteria(condition);
      stmt.SetOrderBy(orderBy);
      stmt.SetGroupByClause(groupBy, stmt.GetEachGroupBy());
      stmt.SetTable(mainTableInOut[0]);
      return stmt;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeRightJoin(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {

      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      bool normalized = false;

      for (int i = 0 ; i < tables.Size(); ++i) {
        SqlTable t = tables.Get(i);
        if (!HasRightJoin(t)) {
          continue;
        }
        normalized = true;
        SqlTable wrapped = WrapToNestedJoin(t);
        SqlTable tt = SwapRightJoin(wrapped);
        tables.Set(i, tt);
      }

      if (normalized) {
        stmt.SetTables(tables);
        return stmt;
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeNestedJoin(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      bool normalized = false;

      for (int i = 0 ; i < tables.Size(); ++i) {
        SqlTable t = tables.Get(i);
        if (t.IsNestedJoinTable()) {
          SqlTable unWrapped = UnWrapNestedJoin(t);
          tables.Set(i, unWrapped);
          normalized = true;
          t = unWrapped;
        }

        if (SqlNormalizationHelper.ContainsNestedJoin(t)) {
          bool associative = SqlNormalizationHelper.IsJoinsAssociative(t);
          if (associative) {
            SqlTable unWrapped = UnWrapNestedJoin(t);
            tables.Set(i, unWrapped);
            normalized = true;
          } else {
            ThrowNoSupportNormalizationException(NESTED_JOIN_EXCEPTION_CODE, query);
          }
        }
      }

      if (normalized) {
        stmt.SetTables(tables);
        return stmt;
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private static SqlTable WrapToNestedJoin(SqlTable top) {
    if (top == null || top.GetJoin() == null) {
      return top;
    }

    SqlTable nestedTableTop;
    if (top.IsNestedJoinTable()) {
      nestedTableTop = top.GetNestedJoin();
    } else if (top.IsNestedQueryTable()) {
      nestedTableTop = new SqlTable(top.GetQuery(), top.GetAlias());
    } else {
      nestedTableTop = new SqlTable(top.GetCatalog(),
              top.GetSchema(),
              top.GetName(),
              top.GetAlias());
    }

    SqlJoin join = top.GetJoin();
    do {
      SqlTable right = join.GetTable();

      SqlTable trimedJoinRight = new SqlTable(right.GetCatalog(),
              right.GetSchema(),
              right.GetName(),
              right.GetAlias(),
              null,
              right.GetNestedJoin(),
              right.GetQuery(),
              right.GetTableValueFunction(),
              right.GetCrossApply());

      if (nestedTableTop.HasJoin()) {
        nestedTableTop = new SqlTable(null, nestedTableTop);
      }

      nestedTableTop = ConcatJoinTable(nestedTableTop,
              trimedJoinRight,
              join.GetJoinType(),
              join.GetCondition());

      join  = right.GetJoin();
    } while (join != null);

    return nestedTableTop;
  }

  private static SqlTable UnWrapNestedJoin(SqlTable top) {
    SqlTable newTop = top;

    if (top.IsNestedJoinTable()) {
      SqlTable nestedJ = top.GetNestedJoin();
      newTop = UnWrapNestedJoin(nestedJ);
    } else {
      newTop = new SqlTable(top.GetCatalog(),
              top.GetSchema(),
              top.GetName(),
              top.GetAlias(),
              null,
              null,
              top.GetQuery(),
              top.GetTableValueFunction(),
              top.GetCrossApply());
    }

    bool associative = SqlNormalizationHelper.IsJoinsAssociative(top);
    if (associative) {
      SqlCollection<SqlJoin> joins = new SqlCollection<SqlJoin>();
      if (top.HasJoin()) {
        joins.Add(top.GetJoin());
        ParserCore.FlattenJoins(joins, top.GetJoin().GetTable());
      }

      for (int i = 0 ; i < joins.Size(); ++i) {
        SqlJoin j = joins.Get(i);
        SqlTable r = j.GetTable();
        while (r.IsNestedJoinTable()) {
          r = r.GetNestedJoin();
        }
        SqlTable nr = new SqlTable(r.GetCatalog(),
                r.GetSchema(),
                r.GetName(),
                r.GetAlias(),
                null,
                null,
                r.GetQuery(),
                r.GetTableValueFunction(),
                r.GetCrossApply());
        newTop = ConcatJoinTable(newTop,
                nr,
                j.GetJoinType(),
                j.GetCondition());
      }
    } else {
      SqlJoin join = top.GetJoin();
      newTop = ConcatJoinTable(newTop,
              join.GetTable(),
              join.GetJoinType(),
              join.GetCondition());
    }
    return newTop;
  }

  private static SqlTable SwapRightJoin(SqlTable top) {
    if (top == null || (!top.HasJoin() && !top.IsNestedJoinTable())) {
      return top;
    }

    if (top.IsNestedJoinTable()) {
      SqlTable n1 = top.GetNestedJoin();
      SqlTable n2 = SwapRightJoin(n1);
      if (n1 != n2) {
        top = new SqlTable(top.GetCatalog(),
                top.GetSchema(),
                top.GetName(),
                top.GetAlias(),
                top.GetJoin(),
                n2,
                top.GetQuery(),
                top.GetTableValueFunction(),
                top.GetCrossApply());
      }
    }

    if (top.GetJoin() != null) {
      SqlJoin join = top.GetJoin();
      if (JoinType.RIGHT == join.GetJoinType()) {
        SqlTable right = join.GetTable();
        if (right.IsNestedJoinTable()) {
          right = SwapRightJoin(right);
        }
        SqlTable left = right;
        if (top.IsNestedJoinTable()) {
          SqlTable n = top.GetNestedJoin();
          right = new SqlTable(null, n);
        } else if (top.IsNestedQueryTable()){
          right = new SqlTable(top.GetQuery(),
                  top.GetName(),
                  top.GetAlias());
        } else if (top.IsFunctionValueTable()) {
          right = new SqlTable(top.GetTableValueFunction());
        } else {
          right = new SqlTable(top.GetCatalog(),
                  top.GetSchema(),
                  top.GetName(),
                  top.GetAlias(),
                  null,
                  top.GetNestedJoin(),
                  top.GetQuery(),
                  top.GetTableValueFunction(),
                  top.GetCrossApply());
        }
        join = new SqlJoin(JoinType.LEFT,
                right,
                join.GetCondition(),
                join.IsEach(),
                join.HasOuter());

        top = new SqlTable(left.GetCatalog(),
                left.GetSchema(),
                left.GetName(),
                left.GetAlias(),
                join,
                left.GetNestedJoin(),
                left.GetQuery(),
                left.GetTableValueFunction(),
                left.GetCrossApply());
      }
    }
    SqlTable resolved = top;
    return resolved;
  }

  private static bool HasRightJoin(SqlTable table) {
    bool hasRightJoin = false;
    if (table != null && table.HasJoin()) {
      if (table.IsNestedJoinTable()) {
        hasRightJoin = HasRightJoin(table.GetNestedJoin());
      }
      if (!hasRightJoin && table.HasJoin()) {
        SqlJoin join = table.GetJoin();
        if (JoinType.RIGHT == join.GetJoinType()) {
          hasRightJoin = true;
        } else {
          hasRightJoin = HasRightJoin(join.GetTable());
        }
      }
    }
    return hasRightJoin;
  }

  private static SqlCollection<SqlOrderSpec> AttachOwnerTableForOrderBy(SqlCollection<SqlOrderSpec> orders, SqlCollection<SqlTable> owners) {
    SqlCollection<SqlOrderSpec> resolved = new SqlCollection<SqlOrderSpec>();
    foreach(SqlOrderSpec order in orders) {
      SqlExpression expr = order.GetExpr();
      if (expr is SqlColumn) {
        SqlColumn c = (SqlColumn)order.GetExpr();
        if (null == c.GetTable()) {
          c = AttachOwnerTableForColumn(owners, c);
          resolved.Add(new SqlOrderSpec(c, order.GetOrder(), order.IsNullsFirst(), order.HasNulls()));
        } else {
          resolved.Add(order);
        }
      } else {
        resolved.Add(order);
      }
    }
    return resolved;
  }

  private static SqlCollection<SqlExpression> AttachOwnerTableForGroupBy(SqlCollection<SqlExpression> groups, SqlCollection<SqlTable> owners) {
    SqlCollection<SqlExpression> resolved = new SqlCollection<SqlExpression>();
    foreach(SqlExpression group in groups) {
      if (group is SqlColumn) {
        SqlColumn column = (SqlColumn)group;
        if (null == column.GetTable()) {
          resolved.Add(AttachOwnerTableForColumn(owners, column));
        } else {
          resolved.Add(group);
        }
      } else {
        resolved.Add(group);
      }
    }
    return resolved;
  }

  private static void StandardJoin(SqlTable [] mainTableInOut, SqlConditionNode [] whereInOut) {
    SqlTable mainIn = mainTableInOut[0];
    SqlTable mainOut = mainTableInOut[0];
    SqlConditionNode whereIn = whereInOut[0];
    whereInOut[0] = null;
    SqlTable top = mainIn;
    JavaVector<SqlTable> rightWithJoins = new JavaVector<SqlTable>();
    SqlJoin join = top.GetJoin();
    while (join != null) {
      SqlConditionNode on = join.GetCondition();
      bool isStandard = SqlNormalizationHelper.IsStandardOnCriteria(on);
      if (!isStandard) {
        SqlConditionNode [] onInOut = new SqlConditionNode[] {on};
        StandardCriteriaInJoin(onInOut, whereInOut);
        join = new SqlJoin(join.GetJoinType(), join.GetTable(), onInOut[0], join.IsEach(), join.HasOuter());
      }
      top = new SqlTable(top.GetCatalog(), top.GetSchema(), top.GetName(), top.GetAlias(), join, top.GetNestedJoin(), top.GetQuery(), top.GetTableValueFunction(), top.GetCrossApply());
      rightWithJoins.Add(top);
      top = join.GetTable();
      join = top.GetJoin();
    }
    SqlTable l, r;
    if (rightWithJoins.Size() > 0) {
      mainOut = rightWithJoins.Get(0);
      for (int i = rightWithJoins.Size() - 1; i >= 1; --i) {
        r = rightWithJoins.Get(i);
        l = rightWithJoins.Get(i - 1);
        SqlJoin j = l.GetJoin();
        l = new SqlTable(l.GetCatalog(), l.GetSchema(), l.GetName(), l.GetAlias(), new SqlJoin(j.GetJoinType(), r, j.GetCondition(), j.IsEach(), j.HasOuter()), l.GetNestedJoin(), l.GetQuery(), l.GetTableValueFunction(), l.GetCrossApply());
        mainOut = l;
      }
    }
    mainTableInOut[0] = mainOut;
    if (whereIn != null) {
      if (whereInOut[0] != null) {
        whereInOut[0] = new SqlCondition(whereIn, SqlLogicalOperator.And, whereInOut[0]);
      } else {
        whereInOut[0] = whereIn;
      }
    }
  }

  private static void StandardCriteriaInJoin(SqlConditionNode [] onInOut, SqlConditionNode [] whereInOut) {
    SqlConditionNode onIn = onInOut[0];
    SqlConditionNode onOut = onInOut[0];
    SqlConditionNode whereIn = whereInOut[0];
    SqlConditionNode whereOut = whereInOut[0];
    if (onIn is SqlCondition) {
      SqlCondition condition = (SqlCondition) onInOut[0];
      SqlConditionNode left = (SqlConditionNode)condition.GetLeft();
      SqlConditionNode [] newLeft = new SqlConditionNode[] {left};
      bool isLeftResolved = false;
      bool isRightResolved = false;
      if (!SqlNormalizationHelper.IsStandardOnCriteria(left)) {
        StandardCriteriaInJoin(newLeft, whereInOut);
        isLeftResolved = true;
      }
      SqlConditionNode right = (SqlConditionNode)condition.GetRight();
      SqlConditionNode [] newRight = new SqlConditionNode[] {right};
      if (!SqlNormalizationHelper.IsStandardOnCriteria(right)) {
        StandardCriteriaInJoin(newRight, whereInOut);
        isRightResolved = true;
      }
      if (isLeftResolved || isRightResolved) {
        if (newLeft[0] != null && newRight[0] != null) {
          onOut = new SqlCondition(newLeft[0], condition.GetLogicOp(), newRight[0]);
          whereOut = whereInOut[0];
        } else if (newLeft[0] != null) {
          onOut = newLeft[0];
          whereOut = whereInOut[0];
        } else if (newRight[0] != null) {
          onOut = newRight[0];
          whereOut = whereInOut[0];
        } else {
          onOut = null;
          whereOut = whereInOut[0];
        }
      }
    } else if (onIn is SqlCriteria) {
      SqlCriteria c = (SqlCriteria) onIn;
      if (!SqlNormalizationHelper.IsStandardOnCriteria(c)) {
        onOut = null;
        if (whereOut != null) {
          whereOut = new SqlCondition(whereIn, SqlLogicalOperator.And, c);
        } else {
          whereOut = c;
        }
      }
    }
    onInOut[0] = onOut;
    whereInOut[0] = whereOut;
  }

  private static SqlQueryStatement NormalizeImplicitJoin(SqlQueryStatement query, JoinType type) {
    if (SqlNormalizationHelper.IsImplicitJoin(query)) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();

      SqlCollection<SqlTable> commaTables = stmt.GetTables();
      for (int i = 0 ; i < commaTables.Size(); ++i) {
        SqlTable commaTable = commaTables.Get(i);
        if (commaTable.IsNestedQueryTable()) {
          SqlQueryStatement commaQuery = NormalizeImplicitJoin(commaTable.GetQuery(), type);
          commaTable = new SqlTable(commaTable.GetCatalog(),
                  commaTable.GetSchema(),
                  commaTable.GetName(),
                  commaTable.GetAlias(),
                  commaTable.GetJoin(),
                  null,
                  commaQuery,
                  commaTable.GetTableValueFunction(),
                  commaTable.GetCrossApply());
          commaTables.Set(i, commaTable);
        }
      }

      bool equiInnerFactor = false;

      if (type == JoinType.INNER) {
        equiInnerFactor = SqlNormalizationHelper.IsEquiJoinCriteria(condition, tables);
      }

      bool support = type == JoinType.INNER && equiInnerFactor || type != JoinType.INNER;

      if (!support) {
        ThrowNoSupportNormalizationException(IMPLICIT_JOIN_EXCEPTION_CODE, query);
      }

      SqlCriteria[] equiOn = null;
      if (equiInnerFactor) {
        SqlConditionNode [] inWhere = new SqlConditionNode[] {condition};
        equiOn = ResolveEquiJoinCriteria(inWhere, tables);
        condition = inWhere[0];
      }

      SqlTable newTop = stmt.GetTables().Get(stmt.GetTables().Size() - 1);
      if (newTop.IsNestedQueryTable()) {
        newTop = new SqlTable(NormalizeImplicitJoin(newTop.GetQuery(), type),
                newTop.GetName(),
                newTop.GetAlias());
      }
      for (int i = tables.Size() - 1 ; i >= 1; --i) {
        SqlTable l = tables.Get(i - 1);
        if (l.IsNestedQueryTable()) {
          l = new SqlTable(NormalizeImplicitJoin(l.GetQuery(), type),
                  l.GetName(),
                  l.GetAlias());
        }
        bool isOuter = type == JoinType.LEFT || type == JoinType.RIGHT || type == JoinType.FULL;
        if (newTop.HasJoin() && isOuter) {
          newTop = new SqlTable(null, newTop);
        }
        newTop = ConcatJoinTable(l,
                newTop,
                type,
                equiOn == null ? null : equiOn[i - 1]);
      }
      SqlCollection<SqlTable> ts = new SqlCollection<SqlTable>();
      ts.Add(newTop);
      stmt.SetTables(ts);
      stmt.SetCriteria(condition);
      return stmt;
    } else if (query is SqlSelectStatement
            && ContainsNestedQuery(query.GetTable())) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlTable mainTable = stmt.GetTable();
      if (mainTable.IsNestedQueryTable()) {
        SqlQueryStatement nestedQuery = mainTable.GetQuery();
        SqlQueryStatement resolvedNestedQuery = NormalizeImplicitJoin(nestedQuery, type);
        mainTable = new SqlTable(null,
                null,
                mainTable.GetName(),
                mainTable.GetAlias(),
                mainTable.GetJoin(),
                null,
                resolvedNestedQuery,
                null,
                mainTable.GetCrossApply());
      }
      query.SetTable(mainTable);
      return query;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeEquiInnerJoin(SqlQueryStatement query) {

    if (!(query is SqlSelectStatement)) return query;

    bool allInner = true;
    SqlCollection<SqlJoin> joins = query.GetJoins();
    SqlCollection<SqlTable> flattenTables = new SqlCollection<SqlTable>();
    SqlTable mainTable = query.GetTable();
    flattenTables.Add(new SqlTable(mainTable.GetCatalog(),
            mainTable.GetSchema(),
            mainTable.GetName(),
            mainTable.GetAlias(),
            null,
            null,
            mainTable.GetQuery(),
            mainTable.GetTableValueFunction(),
            mainTable.GetCrossApply()));

    foreach(SqlJoin j in joins) {
      if ((j.GetJoinType() != JoinType.INNER && j.GetJoinType() != JoinType.NATURAL)
          || j.GetTable().IsNestedJoinTable()) {
        allInner = false;
        break;
      }
      SqlTable right = j.GetTable();
      flattenTables.Add(new SqlTable(right.GetCatalog(),
              right.GetSchema(),
              right.GetName(),
              right.GetAlias(),
              null,
              null,
              right.GetQuery(),
              right.GetTableValueFunction(),
              right.GetCrossApply()));
    }

    if (!allInner) return query;

    bool equiInnerFactor = SqlNormalizationHelper.IsEquiJoinCriteria(query.GetCriteria(), flattenTables);
    if (!equiInnerFactor) return query;

    SqlSelectStatement stmt = (SqlSelectStatement)query;
    SqlCollection<SqlColumn> columns = stmt.GetColumns();
    SqlConditionNode havingClause = stmt.GetHavingClause();
    SqlConditionNode condition = stmt.GetCriteria();
    SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
    SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();

    SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
    bool fromLast = stmt.GetFromLast();
    SqlExpression limitExpr = stmt.GetLimitExpr();
    SqlExpression offsetExpr = stmt.GetOffsetExpr();

    SqlCriteria[] equiOn = null;
    if (equiInnerFactor) {
      SqlConditionNode [] inWhere = new SqlConditionNode[] {condition};
      equiOn = ResolveEquiJoinCriteria(inWhere, flattenTables);
      condition = inWhere[0];
    }

    SqlTable newTop = flattenTables.Get(flattenTables.Size() - 1);
    if (newTop.IsNestedQueryTable()) {
      newTop = new SqlTable(NormalizeImplicitJoin(newTop.GetQuery(), JoinType.INNER),
              newTop.GetName(),
              newTop.GetAlias());
    }

    for (int i = flattenTables.Size() - 1 ; i >= 1; --i) {
      SqlTable l = flattenTables.Get(i - 1);
      if (l.IsNestedQueryTable()) {
        l = new SqlTable(NormalizeImplicitJoin(l.GetQuery(), JoinType.INNER),
                l.GetName(),
                l.GetAlias());
      }

      newTop = ConcatJoinTable(l,
              newTop,
              JoinType.INNER,
              equiOn == null ? null : equiOn[i - 1]);
    }

    stmt.SetTable(newTop);
    stmt.SetCriteria(condition);
    return stmt;
  }

  private static SqlQueryStatement NormalizeTableAlias(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      SqlCollection<SqlTable> flattens = SqlNormalizationHelper.FlattenTables(query);
      columns = ResolveTableAliasInColumns(columns, flattens);

      condition = ResolveTableAliasInCriteria(condition, flattens);

      havingClause = ResolveTableAliasInCriteria(havingClause, flattens);

      groupBy = ResolveTableAliasInGroupBy(groupBy, flattens);

      orderBy = ResolveTableAliasInOrderBy(orderBy, flattens);

      for (int i = 0 ; i < tables.Size(); ++i) {
        SqlTable t = tables.Get(i);
        if (t.IsNestedQueryTable()) {
          SqlQueryStatement nq = NormalizeTableAlias(t.GetQuery());
        }
        t = ResolvedMainTableAlias(t, flattens);
        tables.Set(i, t);
      }
      query.SetColumns(columns);
      query.SetCriteria(condition);
      ((SqlSelectStatement) query).SetHavingClause(havingClause);
      ((SqlSelectStatement) query).SetTables(tables);
      ((SqlSelectStatement) query).SetGroupByClause(groupBy, query.GetEachGroupBy());
      query.SetOrderBy(orderBy);
      return query;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeCriteriaWithNot(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();
      condition = ResolvedConditionNot(condition);
      stmt.SetCriteria(condition);
      return stmt;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeCriteria(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();
      condition = ResolvedCondition(condition);
      query.SetCriteria(condition);
      return query;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement NormalizeMinimizeCriteria(SqlQueryStatement query) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();
      SqlCollection<SqlValueExpression> parameters = stmt.GetParameterList();
      bool fromLast = stmt.GetFromLast();
      SqlExpression limitExpr = stmt.GetLimitExpr();
      SqlExpression offsetExpr = stmt.GetOffsetExpr();

      bool modified = false;

      SqlConditionNode minimizeCriteria = (SqlConditionNode) MinimizeCriteria(condition);
      if (minimizeCriteria != condition) {
        modified = true;
      }

      SqlConditionNode minimizeHaving = (SqlConditionNode) MinimizeCriteria(havingClause);
      if (minimizeHaving != condition) {
        modified = true;
      }

      if (modified) {
        stmt.SetCriteria(minimizeCriteria);
        stmt.SetHavingClause(minimizeHaving);
        return stmt;
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private static SqlExpression MinimizeCriteria(SqlExpression node) {
    SqlExpression resolved = node;
    if (node is SqlCondition) {
      SqlCondition condition = (SqlCondition) node;
      SqlExpression left = condition.GetLeft();
      SqlExpression right = condition.GetRight();

      if (SqlLogicalOperator.And == condition.GetLogicOp()) {
        if (left.IsEvaluatable()) {
          SqlValue trueOrfalse = left.Evaluate();
          bool v = trueOrfalse.GetValueAsBool(true);
          if (v) {
            resolved = MinimizeCriteria(right);
          } else {
            resolved = null;
          }
        } else if (right.IsEvaluatable()) {
          SqlValue trueOrfalse = right.Evaluate();
          bool v = trueOrfalse.GetValueAsBool(true);
          if (v) {
            resolved = MinimizeCriteria(left);
          } else {
            resolved = null;
          }
        } else {
          SqlExpression mLeft = MinimizeCriteria(left);
          SqlExpression mRight = MinimizeCriteria(right);
          if (mLeft != null && mRight != null) {
            resolved = new SqlCondition(mLeft, SqlLogicalOperator.And, mRight);
          } else if (mLeft != null) {
            resolved = mLeft;
          } else if (mRight != null) {
            resolved = mRight;
          } else {
            resolved = null;
          }
        }
      } else if (SqlLogicalOperator.Or == condition.GetLogicOp()) {
        if (left.IsEvaluatable()) {
          SqlValue trueOrfalse = left.Evaluate();
          bool v = trueOrfalse.GetValueAsBool(false);
          if (v) {
            resolved = null;
          } else {
            resolved = MinimizeCriteria(right);
          }
        } else if (right.IsEvaluatable()) {
          SqlValue trueOrfalse = right.Evaluate();
          bool v = trueOrfalse.GetValueAsBool(false);
          if (v) {
            resolved = null;
          } else {
            resolved = MinimizeCriteria(left);
          }
        } else {
          SqlExpression mLeft = MinimizeCriteria(left);
          SqlExpression mRight = MinimizeCriteria(right);
          if (mLeft != null && mRight != null) {
            resolved = new SqlCondition(mLeft, SqlLogicalOperator.Or, mRight);
          } else if (mLeft != null) {
            resolved = mLeft;
          } else if (mRight != null) {
            resolved = mRight;
          } else {
            resolved = null;
          }
        }
      } else {
        //do nothing.
      }
    } else if (node is SqlConditionNot) {
      SqlConditionNot notCondition = (SqlConditionNot) node;
      SqlExpression expr = notCondition.GetCondition();
      if (expr.IsEvaluatable()) {
        SqlValue trueOrfalse = expr.Evaluate();
        bool v = trueOrfalse.GetValueAsBool(true);
        if (v) {
          resolved = new SqlCriteria(null, ComparisonType.FALSE, null);
        } else {
          resolved = null;
        }
      } else {
        SqlExpression minExpr = MinimizeCriteria(expr);
        if (minExpr != expr) {
          if (minExpr != null) {
            resolved = new SqlConditionNot(minExpr);
          }
        }
      }
    } else if (node is SqlCriteria) {
      SqlCriteria cr = (SqlCriteria) node;
      if (cr.IsEvaluatable()) {
        resolved = null;
      }
    }
    return resolved;
  }

  private static SqlConditionNode ResolvedCondition(SqlConditionNode condition) {
    SqlConditionNode resolved = condition;
    if (condition is SqlCondition) {
      SqlCondition c = (SqlCondition)condition;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (l is SqlConditionNode) {
        l = ResolvedCondition((SqlConditionNode)l);
      }
      if (r is SqlConditionNode) {
        r = ResolvedCondition((SqlConditionNode)r);
      }
      resolved = new SqlCondition(l, c.GetLogicOp(), r);
    } else if (condition is SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)condition;
      if (criteria.GetRight() is SqlGeneralColumn && criteria.GetLeft() is SqlValueExpression) {
        switch (criteria.GetOperator()) {
          case ComparisonType.EQUAL:
            resolved = new SqlCriteria(criteria.GetRight(), ComparisonType.EQUAL, criteria.GetLeft());
            break;
          case ComparisonType.BIGGER_EQUAL:
            resolved = new SqlCriteria(criteria.GetRight(), ComparisonType.SMALLER_EQUAL, criteria.GetLeft());
            break;
          case ComparisonType.BIGGER:
            resolved = new SqlCriteria(criteria.GetRight(), ComparisonType.SMALLER, criteria.GetLeft());
            break;
          case ComparisonType.SMALLER_EQUAL:
            resolved = new SqlCriteria(criteria.GetRight(), ComparisonType.BIGGER_EQUAL, criteria.GetLeft());
            break;
          case ComparisonType.SMALLER:
            resolved = new SqlCriteria( criteria.GetRight(), ComparisonType.BIGGER, criteria.GetLeft());
            break;
          case ComparisonType.NOT_EQUAL:
            resolved = new SqlCriteria(criteria.GetRight(), ComparisonType.NOT_EQUAL, criteria.GetLeft());
            break;
        }
      }
    }
    return resolved;
  }

  private static SqlConditionNode ResolvedConditionNot(SqlConditionNode condition) {
    SqlConditionNode resolved = condition;
    if (condition is SqlCondition) {
      SqlCondition c = (SqlCondition)condition;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (l is SqlConditionNode) {
        l = ResolvedConditionNot((SqlConditionNode)l);
      }
      if (r is SqlConditionNode) {
        r = ResolvedConditionNot((SqlConditionNode)r);
      }
      resolved = new SqlCondition(l, c.GetLogicOp(), r);
    } else if (condition is SqlConditionNot) {
      SqlConditionNode conditionInNot = (SqlConditionNode)((SqlConditionNot) condition).GetCondition();
      resolved = ResolvedConditionInNot(conditionInNot);
    }
    return resolved;
  }

  private static SqlConditionNode ResolvedConditionInNot(SqlConditionNode c) {
    SqlConditionNode resolved = c;
    if (c is SqlConditionInSelect) {
      //do noting now.
      resolved = new SqlConditionNot(c);
    } else if (c is SqlConditionExists) {
      //do noting now.
      resolved = new SqlConditionNot(c);
    } else if (c is SqlCondition) {
      SqlCondition condition = (SqlCondition)c;
      SqlExpression l = condition.GetLeft();
      SqlExpression r = condition.GetRight();
      SqlLogicalOperator newOp;
      if (condition.GetLogicOp() == SqlLogicalOperator.And) {
        newOp = SqlLogicalOperator.Or;
      } else {
        newOp = SqlLogicalOperator.And;
      }
      resolved = ResolvedConditionNot(new SqlCondition(new SqlConditionNot(l), newOp, new SqlConditionNot(r)));
    } else if (c is SqlConditionNot) {
      resolved = ResolvedConditionNot((SqlConditionNode)((SqlConditionNot) c).GetCondition());
    } else if (c is SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)c;
      switch (criteria.GetOperator()) {
        case ComparisonType.EQUAL:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.NOT_EQUAL, criteria.GetRight());
          break;
        case ComparisonType.BIGGER_EQUAL:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.SMALLER, criteria.GetRight());
          break;
        case ComparisonType.BIGGER:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.SMALLER_EQUAL, criteria.GetRight());
          break;
        case ComparisonType.SMALLER_EQUAL:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.BIGGER, criteria.GetRight());
          break;
        case ComparisonType.SMALLER:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.BIGGER_EQUAL, criteria.GetRight());
          break;
        case ComparisonType.NOT_EQUAL:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.EQUAL, criteria.GetRight());
          break;
        case ComparisonType.IS_NOT:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.IS, criteria.GetRight());
          break;
        case ComparisonType.IS:
          resolved = new SqlCriteria(criteria.GetLeft(), ComparisonType.IS_NOT, criteria.GetRight());
          break;
        case ComparisonType.FALSE:
        case ComparisonType.LIKE:
        case ComparisonType.IN:
          //do noting now.
          resolved = new SqlConditionNot(criteria);
          break;
        case ComparisonType.CUSTOM:
          resolved = new SqlCriteria(criteria.GetLeft(), criteria.GetCustomOp(), criteria.GetRight());
          break;
      }
    }
    return resolved;
  }

  private static SqlQueryStatement NormalizeCriteriaWithSubQuery(SqlQueryStatement query, JoinType joinType) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlTable table = stmt.GetTable();
      SqlCollection<SqlTable> tables = stmt.GetTables();

      SqlTable [] tableInOut = {table};
      SqlConditionNode [] conditionInOut = {condition};
      SqlExpression [] LIMIT_OFFSET = {null, null};
      NormalizeCriteriaSubQuery(columns,
          tableInOut,
          conditionInOut,
          joinType,
          LIMIT_OFFSET);
      table = tableInOut[0];
      condition = conditionInOut[0];
      if (condition != stmt.GetCriteria()) {
        SqlCollection<SqlTable> owner = new SqlCollection<SqlTable>();
        owner.Add(new SqlTable(table.GetAlias()));
        condition = AttachTableForCriteria(owner, condition);
      }
      tables.Set(0, table);
      stmt.SetTables(tables);
      stmt.SetCriteria(condition);
      if (null == stmt.GetLimitExpr()) {
        stmt.SetLimitExpr(LIMIT_OFFSET[0]);
      }

      if (null == stmt.GetOffsetExpr()) {
        stmt.SetOffsetExpr(LIMIT_OFFSET[1]);
      }
      return stmt;
    } else {
      return query;
    }
  }

  private static void GetNestedQueryTables(SqlTable table, SqlCollection<SqlTable> nestedTables) {
    if (table == null) return;
    if (ContainsNestedQuery(table)) {
      if (table.IsNestedQueryTable()) {
        nestedTables.Add(table);
      }
      if (table.HasJoin()) {
        SqlJoin join = table.GetJoin();
        SqlTable r;
        do {
          r = join.GetTable();
          if (r.IsNestedQueryTable()) {
            nestedTables.Add(r);
          }
          if (r.IsNestedJoinTable()) {
            GetNestedQueryTables(r.GetNestedJoin(), nestedTables);
          }
          join = r.GetJoin();
        } while (r.HasJoin());
      }
    }
  }

  private static SqlTable MatchOwnerOfColumn(SqlColumn mc, SqlCollection<SqlTable> tableList) {
    SqlTable owner = null;
    string tNameOrAlias = null;
    if (mc.GetTable() != null) {
      tNameOrAlias = mc.GetTable().GetAlias();
    }
    foreach(SqlTable t in tableList) {
      bool match = false;
      SqlTable matchedTable = null;
      if (t.IsNestedQueryTable()) {
        if (Utilities.EqualIgnoreCaseInvariant(t.GetAlias(), tNameOrAlias)) {
          match = true;
          matchedTable = t;
        } else if (null == tNameOrAlias) {
          SqlColumn mappingColumn = SqlNormalizationHelper.GetMappingNestedColumn(mc, t.GetQuery().GetColumns());
          if (mappingColumn != null) {
            match = true;
            matchedTable = t;
          }
        }
      } else if (Utilities.EqualIgnoreCaseInvariant(t.GetAlias(), tNameOrAlias)) {
        match = true;
        matchedTable = t;
      } else {
        if (1 == tableList.Size()) {
          match = true;
          owner = t;
        } else {
          continue;
        }
      }
      if (match) {
        if (owner == null) {
          if (IsSimpleTable(matchedTable)) {
            owner = new SqlTable(matchedTable.GetName(), matchedTable.GetAlias());
          } else if (matchedTable.IsNestedQueryTable()){
            owner = (SqlTable) matchedTable.Clone();
          }
        }
        break;
      }
    }
    return owner;
  }

  private static SqlQueryStatement NormalizeNestedQueryTableName(SqlQueryStatement query, IDataMetadata dataMetaData) {
    if (query is SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.GetColumns();
      SqlConditionNode havingClause = stmt.GetHavingClause();
      SqlConditionNode condition = stmt.GetCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.GetOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.GetGroupBy();
      SqlCollection<SqlTable> tables = stmt.GetTables();

      if (ContainsNestedQuery(tables)) {
        SqlTable mainTable = stmt.GetTable();
        SqlCollection<SqlTable> nestedQueryTables = new SqlCollection<SqlTable>();
        foreach(SqlTable table in tables) {
          GetNestedQueryTables(table, nestedQueryTables);
        }

        if (query.IsAsteriskQuery() && query.IsJoinQuery()) {
          SqlWildcardColumn wc = null;
          for (int i = 0 ; i < columns.Size(); ++i) {
            SqlColumn c = columns.Get(i);
            if (c is SqlWildcardColumn && null == c.GetTable()) {
              wc = (SqlWildcardColumn) c;
              break;
            }
          }
          if (wc != null) {
            columns.Remove(wc);
            SqlCollection<SqlTable> flatten = SqlNormalizationHelper.FlattenTables(query);
            foreach(SqlTable t in flatten) {
              columns.Add(new SqlWildcardColumn(new SqlTable(t.GetAlias())));
            }
          }
        }
        AttachOwnerTableForColumns(nestedQueryTables, columns);
        if (mainTable.IsNestedQueryTable()) {
          NormalizeNestedQueryTableName(mainTable.GetQuery(), dataMetaData);
        }

        SqlJoin join = mainTable.GetJoin();
        SqlTable newTop = new SqlTable(mainTable.GetCatalog(),
            mainTable.GetSchema(),
            mainTable.GetName(),
            mainTable.GetAlias(),
            null,
            mainTable.GetNestedJoin(),
            mainTable.GetQuery(),
            mainTable.GetTableValueFunction(),
            mainTable.GetCrossApply());
        while (join != null) {
          SqlTable r = join.GetTable();
          if (r.IsNestedQueryTable()) {
            NormalizeNestedQueryTableName(r.GetQuery(), dataMetaData);
          }

          SqlConditionNode on = AttachTableForCriteria(nestedQueryTables, join.GetCondition());
          SqlTable nr = new SqlTable(r.GetCatalog(),
              r.GetSchema(),
              r.GetName(),
              r.GetAlias(),
              null,
              r.GetNestedJoin(),
              r.GetQuery(),
              r.GetTableValueFunction(),
              r.GetCrossApply());
          newTop = ConcatJoinTable(newTop, nr, join.GetJoinType(), on);
          join = r.GetJoin();
        }

        tables.Set(0, newTop);

        orderBy = AttachOwnerTableForOrderBy(orderBy, nestedQueryTables);

        groupBy = AttachOwnerTableForGroupBy(groupBy, nestedQueryTables);

        havingClause = AttachTableForCriteria(nestedQueryTables, havingClause);

        condition = AttachTableForCriteria(nestedQueryTables, condition);

        stmt.SetColumns(columns);
        stmt.SetHavingClause(havingClause);
        stmt.SetOrderBy(orderBy);
        stmt.SetGroupByClause(groupBy, stmt.GetEachGroupBy());
        stmt.SetTables(tables);
        stmt.SetCriteria(condition);
      }

      if (stmt.IsJoinQuery()) {
        ColumnOwnerModifier ownerModifier = new ColumnOwnerModifier(stmt, dataMetaData);
        ownerModifier.Modify(new SqlParser(stmt));
        Exception error = ownerModifier.GetError();
        if (error != null) {
          throw error;
        }
      }

      return stmt;
    } else {
      return query;
    }
  }

  private static void NormalizeCriteriaSubQuery(SqlCollection<SqlColumn> columnsInOut,
                                                SqlTable [] tableInOut,
                                                SqlConditionNode [] conditionInOut,
                                                JoinType joinType,
                                                SqlExpression [] outLimitOffSet) {
    SqlConditionNode conditionParaIn = conditionInOut[0];
    if (conditionParaIn is SqlConditionInSelect
        || conditionParaIn is SqlCriteria) {
      SqlTable table = tableInOut[0];
      SqlCriteria c = (SqlCriteria)conditionParaIn;
      if (c.GetRight() is SqlSubQueryExpression) {
        SqlSubQueryExpression sub = (SqlSubQueryExpression)c.GetRight();
        SqlQueryStatement sq = sub.GetQuery();
        bool support = !ContainsAggragation(sq.GetColumns()) && sq.GetGroupBy().Size() == 0 && IsSimpleTable(sq.GetTable());
        if (!support) {
          ThrowNoSupportNormalizationException(CRITERIA_SUBQUERY_EXCEPTION_CODE, sq);
        }
        string leftAlias = table.GetAlias();
        string rightAlias = sq.GetTable().GetAlias();
        if (leftAlias.Equals(rightAlias)) {
          leftAlias = leftAlias + "1";
          rightAlias = rightAlias + "2";
        }
        SqlTable right = new SqlTable(sq.GetTable().GetCatalog(), sq.GetTable().GetSchema(), sq.GetTable().GetName(), rightAlias);
        SqlTable leftAliasTable = new SqlTable(table.GetCatalog(), table.GetSchema(), leftAlias, leftAlias);
        SqlCollection<SqlTable> LEFTOWNERS = new SqlCollection<SqlTable>();
        LEFTOWNERS.Add(leftAliasTable);
        SqlTable rightALiasTable = new SqlTable(right.GetCatalog(), table.GetSchema(), rightAlias, rightAlias);
        SqlCollection<SqlTable> RIGHTOWNERS = new SqlCollection<SqlTable>();
        RIGHTOWNERS.Add(rightALiasTable);
        SqlCriteria on = new SqlCriteria(AttachOwnerTableForColumn(LEFTOWNERS, (SqlColumn)c.GetLeft()),
                c.GetOperator(),
                c.GetCustomOp(),
                AttachOwnerTableForColumn(RIGHTOWNERS, sq.GetColumns().Get(0)));
        SqlJoin join = new SqlJoin(joinType, right, on);
        table = new SqlTable(table.GetCatalog(),
                table.GetSchema(),
                table.GetName(),
                leftAlias,
                join,
                table.GetNestedJoin(),
                table.GetQuery(),
                table.GetTableValueFunction(),
                table.GetCrossApply());
        tableInOut[0] = table;
        conditionInOut[0] = AttachTableForCriteria(RIGHTOWNERS, sq.GetCriteria());
        outLimitOffSet[0] = sq.GetLimitExpr();
        outLimitOffSet[1] = sq.GetOffsetExpr();
        AttachOwnerTableForColumns(LEFTOWNERS, columnsInOut);
      }
    } else if (conditionParaIn is SqlConditionExists) {
      SqlConditionExists exists = (SqlConditionExists) conditionParaIn;
      SqlQueryStatement nq = exists.GetSubQuery();
      SqlCollection<SqlTable> twoTables = new SqlCollection<SqlTable>();
      SqlTable table = tableInOut[0];
      twoTables.Add(new SqlTable(table.GetName(), table.GetAlias()));
      twoTables.Add(new SqlTable(nq.GetTable().GetName(), nq.GetTable().GetAlias()));
      bool equiInnerFactor = SqlNormalizationHelper.IsEquiJoinCriteria(nq.GetCriteria(), twoTables);
      bool support = !ContainsAggragation(nq.GetColumns())
          && nq.GetGroupBy().Size() == 0
          && IsSimpleTable(nq.GetTable())
          && equiInnerFactor;
      if (!support) {
        ThrowNoSupportNormalizationException(CRITERIA_SUBQUERY_EXCEPTION_CODE, nq);
      }
      table = ConcatJoinTable(table,
          nq.GetTable(),
          joinType,
          nq.GetCriteria());
      tableInOut[0] = table;
      conditionInOut[0] = null;
    } else if (conditionParaIn is SqlCondition) {
      SqlCondition c = (SqlCondition)conditionParaIn;
      SqlExpression l = c.GetLeft();
      SqlConditionNode [] leftOutIn = {(SqlConditionNode) l};
      SqlExpression r = c.GetRight();
      SqlConditionNode [] rightOutIn = {(SqlConditionNode) r};
      NormalizeCriteriaSubQuery(columnsInOut,
          tableInOut,
          leftOutIn,
          joinType,
          outLimitOffSet);
      NormalizeCriteriaSubQuery(columnsInOut,
          tableInOut,
          rightOutIn,
          joinType,
          outLimitOffSet);
      if (leftOutIn[0] != null && rightOutIn[0] != null) {
        conditionInOut[0] = new SqlCondition(leftOutIn[0], c.GetLogicOp(), rightOutIn[0]);
      } else if (leftOutIn[0] != null) {
        conditionInOut[0] = leftOutIn[0];
      } else if (rightOutIn[0] != null) {
        conditionInOut[0] = rightOutIn[0];
      }
    } else if (conditionParaIn is SqlConditionNot && joinType == JoinType.LEFT_SEMI) {
      SqlExpression c = ((SqlConditionNot) conditionParaIn).GetCondition();
      if (c is SqlConditionNode) {
        SqlConditionNode [] inSideOutIn = {(SqlConditionNode) c};
        NormalizeCriteriaSubQuery(columnsInOut,
            tableInOut,
            inSideOutIn,
            JoinType.LEFT_ANTI,
            outLimitOffSet);
        if (inSideOutIn[0] != c) {
          conditionInOut[0] = inSideOutIn[0];
        }
      }
    }
  }

  private static void AttachOwnerTableForColumns(SqlCollection<SqlTable> tables, SqlCollection<SqlColumn> columns) {
    for (int i = 0 ; i < columns.Size(); ++i) {
      SqlColumn c = AttachOwnerTableForColumn(tables, columns.Get(i));
      columns.Set(i, c);
    }
  }

  private static SqlConditionNode AttachTableForCriteria(SqlCollection<SqlTable> owners, SqlConditionNode c) {
    SqlConditionNode resovled = c;
    if (c is SqlConditionInSelect) {
      SqlConditionInSelect s = (SqlConditionInSelect)c;
      SqlColumn left = AttachOwnerTableForColumn(owners, (SqlColumn) s.GetLeft());
      resovled = new SqlConditionInSelect(left, s.GetRightQuery(), s.IsAll(), s.GetOperator());
    } else if (c is SqlConditionExists) {
      //do nothing now.
    } else if (c is SqlConditionNot) {
      SqlConditionNot nc = (SqlConditionNot)c;
      resovled = new SqlConditionNot(AttachTableForCriteria(owners, (SqlConditionNode)nc.GetCondition()));
    } else if (c is  SqlCondition) {
      SqlCondition condition = (SqlCondition) c;
      SqlExpression l = condition.GetLeft();
      SqlExpression r = condition.GetRight();
      if (l is SqlConditionNode) {
        l = AttachTableForCriteria(owners, (SqlConditionNode)l);
      }
      if (r is SqlConditionNode) {
        r = AttachTableForCriteria(owners, (SqlConditionNode)r);
      }
      resovled = new SqlCondition(l, condition.GetLogicOp(), r);
    } else if (c is SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)c;
      SqlExpression l = criteria.GetLeft();
      SqlExpression r = criteria.GetRight();
      if (l is SqlColumn) {
        SqlColumn column = (SqlColumn)l;
        if (column.GetTable() == null) {
          l = AttachOwnerTableForColumn(owners, column);
        }
      }
      if (r is SqlColumn) {
        SqlColumn column = (SqlColumn)r;
        if (column.GetTable() == null) {
          r = AttachOwnerTableForColumn(owners, column);
        }
      }
      resovled = new SqlCriteria(l, criteria.GetOperator(), criteria.GetCustomOp(), r);
    }
    return resovled;
  }

  private static SqlColumn AttachOwnerTableForColumn(SqlCollection<SqlTable> owners, SqlColumn c) {
    SqlColumn resovled = c;
    if (c is SqlGeneralColumn) {
      if (null == c.GetTable()) {
        SqlTable owner = MatchOwnerOfColumn(c, owners);
        if (owner != null) {
          if (c is SqlWildcardColumn) {
            resovled =  new SqlWildcardColumn(owner);
          } else {
            SqlGeneralColumn col = (SqlGeneralColumn)c;
            resovled = col.HasAlias() ? new SqlGeneralColumn(owner, col.GetColumnName(), col.GetAlias()) :  new SqlGeneralColumn(owner, col.GetColumnName());
          }
        }
      }
    } else if (c is SqlFormulaColumn) {
      SqlFormulaColumn fc = (SqlFormulaColumn)c;
      for (int i = 0 ; i < fc.GetParameters().Size(); ++i) {
        SqlExpression para = fc.GetParameters().Get(i);
        if (para is SqlColumn) {
          para = AttachOwnerTableForColumn(owners, (SqlColumn)para);
          fc.GetParameters().Set(i, para);
        } else if (para is SqlConditionNode) {
          para = AttachTableForCriteria(owners, (SqlConditionNode) para);
          fc.GetParameters().Set(i, para);
        }
      }
    } else if (c is SqlOperationColumn) {
      SqlOperationColumn oc = (SqlOperationColumn)c;
      SqlOperationExpression oe = (SqlOperationExpression)oc.GetExpr();
      SqlExpression l = oe.GetLeft();
      SqlExpression r = oe.GetRight();
      if (l is SqlColumn) {
        l = AttachOwnerTableForColumn(owners, (SqlColumn)l);
      }
      if (r is SqlColumn) {
        r = AttachOwnerTableForColumn(owners, (SqlColumn)r);
      }
      resovled = new SqlOperationColumn(oc.GetAlias(), new SqlOperationExpression(oe.GetOperator(), l, r));
    }
    return resovled;
  }

  private static bool CheckNormalizeSupport(SqlQueryStatement mainQuery, SqlQueryStatement nestedQuery) {
    bool isSelect = nestedQuery is SqlSelectStatement;
    bool noWhereInMain = mainQuery.GetCriteria() == null || mainQuery.GetCriteria() is SqlConditionExists;
    bool noJoinInNested = !IsJoinQuery(nestedQuery);
    bool noJoinInMain = !mainQuery.GetTable().HasJoin();
    bool joinFactor = noJoinInNested || noJoinInMain;
    bool noAggInMain = !ContainsAggragation(mainQuery.GetColumns()) && 0 == mainQuery.GetGroupBy().Size();
    bool noAggInNested = !ContainsAggragation(nestedQuery.GetColumns()) &&  0 == nestedQuery.GetGroupBy().Size();
    bool asteriskFactor = noJoinInNested || !nestedQuery.IsAsteriskQuery();
    bool aggFactor = false;
    bool noImplicitJoin = true;
    bool hasLimitInNested = nestedQuery.GetLimitExpr() != null;
    bool limitFactor = hasLimitInNested && (mainQuery.IsJoinQuery() || !noAggInMain);
    bool distinctFactor = nestedQuery.IsDistinct();
    if (nestedQuery is SqlSelectStatement) {
      noImplicitJoin = ((SqlSelectStatement) nestedQuery).GetTables().Size() <= 1;
    }

    if (noAggInMain && noAggInNested) {
      aggFactor = true;
    } else if (noAggInMain) {
      if (noJoinInMain && noWhereInMain) {
        aggFactor = true;
      }
    } else if (noAggInNested) {
      aggFactor = true;
    }
    bool support = isSelect && joinFactor && aggFactor && noImplicitJoin && !limitFactor && !distinctFactor && asteriskFactor;
    return support;
  }

  private static SqlQueryStatement NormalizeConstantColumn(SqlQueryStatement query) {
    if (SqlNormalizationHelper.IsConstantQuery(query)) {
      SqlSelectStatement noTableSelect = new SqlSelectStatement(null);
      noTableSelect.SetColumns(query.GetColumns());
      return noTableSelect;
    } else {
      SqlCollection<SqlColumn> columns = query.GetColumns();
      for (int i = 0 ; i < columns.Size(); ++i) {
        SqlColumn c = columns.Get(i);
        if (!(c is SqlFormulaColumn)) {
          continue;
        }

        SqlFormulaColumn function = (SqlFormulaColumn)c;
        string aggFun =  function.GetColumnName();
        bool SUM_OR_COUNT = false;
        if (Utilities.EqualIgnoreCase("SUM", Utilities.ToUpper(aggFun))) {
          SUM_OR_COUNT = true;
        }

        if (Utilities.EqualIgnoreCase("COUNT", Utilities.ToUpper(aggFun))
            || Utilities.EqualIgnoreCase("COUNT_BIG", Utilities.ToUpper(aggFun))) {
          SqlExpression PARA = function.GetParameters().Get(0);
          if (PARA is SqlFormulaColumn) {
            SqlFormulaColumn DISTINCT = (SqlFormulaColumn) PARA;
            if (Utilities.EqualIgnoreCase("DISTINCT", DISTINCT.GetColumnName())
                && DISTINCT.GetParameters().Get(0) is SqlValueExpression) {
              SqlValue ONE = new SqlValue(SqlValueType.NUMBER, "1");
              columns.Set(i, new SqlConstantColumn(function.GetAlias(), new SqlValueExpression(ONE)));
              continue;
            }
          }
          SUM_OR_COUNT = true;
        }

        if (SUM_OR_COUNT) continue;

        if (!SqlUtilities.IsKnownAggragation(aggFun)) continue;

        if (!(function.GetParameters().Get(0) is SqlValueExpression)) continue;

        columns.Set(i, new SqlConstantColumn(function.GetAlias(), (SqlValueExpression) function.GetParameters().Get(0)));
      }
      return query;
    }
  }

  private static SqlQueryStatement NormalizeTableExprWithQuery(SqlQueryStatement mainQuery) {
    if (mainQuery is SqlSelectStatement) {
      SqlSelectStatement newQuery = (SqlSelectStatement)mainQuery;
      if (ContainsNestedQuery(mainQuery.GetTable())) {
        if (mainQuery.GetTable().IsNestedQueryTable()) {
          SqlQueryStatement nestedQuery = mainQuery.GetTable().GetQuery();
          bool support = CheckNormalizeSupport(mainQuery, nestedQuery);
          if (support) {
            newQuery = ResolveMainTableWithQuery((SqlSelectStatement)mainQuery, mainQuery.GetTable());
          } else {
            //TODO: Felix do more testing. SELECT * FROM A JOIN B JOIN (SELECT * FROM C)
            ThrowNoSupportNormalizationException(TABLE_EXPRESSION_EXCEPTION_CODE, mainQuery);
          }
        }
        bool hasQueryInJoin = IsJoinQuery(mainQuery) && ContainsNestedQuery(mainQuery.GetTable().GetJoin().GetTable());
        if (hasQueryInJoin) {
          SqlTable right = mainQuery.GetTable().GetJoin().GetTable();
          bool support = !IsJoinQuery(right.GetQuery()) && !right.IsNestedJoinTable();
          if (support) {
            SqlTable mainTable = newQuery.GetTable();
            SqlTable [] top = {mainTable};
            newQuery = ResolveMainTableJoinWithQuery(newQuery, top);
          } else {
            //TODO: Felix do more testing. SELECT * FROM A JOIN B JOIN (SELECT * FROM C)
            ThrowNoSupportNormalizationException(TABLE_EXPRESSION_EXCEPTION_CODE, mainQuery);
          }
        }
      }
      return newQuery;
    } else {
      //TODO: SqlSelectUnionStatement
      return mainQuery;
    }
  }

  private static bool ContainsNestedQuery(SqlTable mainTable) {
    bool contains = false;
    if (null == mainTable) {
      return contains;
    }
    if (mainTable.GetQuery() != null) {
      contains = true;
    } else if (mainTable.GetNestedJoin() != null) {
      contains = ContainsNestedQuery(mainTable.GetNestedJoin());
    }
    if (!contains) {
      SqlJoin join = mainTable.GetJoin();
      while (join != null) {
        SqlTable right = join.GetTable();
        if (ContainsNestedQuery(right)) {
          contains = true;
          break;
        } else {
          join = right.GetJoin();
        }
      }
    }
    return contains;
  }

  private static bool ContainsNestedQuery(SqlCollection<SqlTable> tables) {
    bool contains = false;
    foreach(SqlTable t in tables) {
      if (ContainsNestedQuery(t)) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  private static SqlQueryStatement NormalizeTableExpr(SqlQueryStatement mainQuery) {
    if (mainQuery is SqlSelectStatement) {
      SqlTable mainTable = mainQuery.GetTable();
      if (ContainsNestedQuery(mainTable)) {
        //SELECT ... FROM (SELECT FROM A JOIN B) JOIN (SELECT FROM C JOIN D) JOIN (SELECT FROM E JOIN F)
        //TODO: Felix do the refactoring for normalizeTableExprWithQuery.
        return mainQuery;
      } else {
        return mainQuery;
      }
    } else {
      //TODO: SqlSelectUnionStatement
      return mainQuery;
    }
  }

  private static SqlExpression ResolveMainQueryOptions(SqlExpression ml, SqlExpression ql) {
    SqlExpression resolved = ml;
    if (ql is SqlValueExpression) {
      if (ml is SqlValueExpression) {
        int main = ml.Evaluate().GetValueAsInt(-1);
        int query = ql.Evaluate().GetValueAsInt(-1);
        if (query < main) {
          resolved = ql;
        }
      } else {
        resolved = ql;
      }
    }
    return resolved;
  }

  private static SqlTable ResolvedMainTableAlias(SqlTable mainTable, SqlCollection<SqlTable> flattenTables) {
    SqlTable resolved = mainTable;
    if (!IsSimpleTable(mainTable)) {
      if (mainTable.IsNestedJoinTable()) {
        SqlTable nestedJoin = ResolvedMainTableAlias(mainTable.GetNestedJoin(), flattenTables);
        mainTable = new SqlTable(mainTable.GetCatalog(), mainTable.GetSchema(), mainTable.GetName(), mainTable.GetAlias(), mainTable.GetJoin(), nestedJoin, mainTable.GetQuery(), mainTable.GetTableValueFunction(), mainTable.GetCrossApply());
        resolved = mainTable;
      }
      if (mainTable.HasJoin()) {
        SqlJoin join =  mainTable.GetJoin();
        SqlConditionNode on = ResolveTableAliasInCriteria(join.GetCondition(), flattenTables);
        SqlTable right = ResolvedMainTableAlias(join.GetTable(), flattenTables);
        join = new SqlJoin(join.GetJoinType(), right, on, join.IsEach(), join.HasOuter());
        resolved = new SqlTable(mainTable.GetCatalog(), mainTable.GetSchema(), mainTable.GetName(), mainTable.GetAlias(), join, mainTable.GetNestedJoin(), mainTable.GetQuery(), mainTable.GetTableValueFunction(), mainTable.GetCrossApply());
      }
    }
    return resolved;
  }

  private static SqlSelectStatement ResolveMainTableWithQuery(SqlSelectStatement mainQuery, SqlTable nestedTable) {
    SqlCollection<SqlColumn> columns = mainQuery.GetColumns();
    SqlCollection<SqlColumn> copyColumns = new SqlCollection<SqlColumn>();
    foreach(SqlColumn c in columns) {
      copyColumns.Add(c);
    }
    SqlConditionNode havingClause = mainQuery.GetHavingClause();
    SqlConditionNode condition = mainQuery.GetCriteria();
    SqlCollection<SqlOrderSpec> orderBy = mainQuery.GetOrderBy();
    SqlCollection<SqlExpression> groupBy = mainQuery.GetGroupBy();
    bool fromLast = mainQuery.GetFromLast();
    SqlExpression limitExpr = mainQuery.GetLimitExpr();
    SqlExpression offsetExpr = mainQuery.GetOffsetExpr();
    SqlCollection<SqlValueExpression> parameters = mainQuery.GetParameterList();

    SqlTable mainTable = mainQuery.GetTable();

    SqlQueryStatement nestedQueryRaw = nestedTable.GetQuery();

    SqlQueryStatement nestedQuery = nestedQueryRaw;
    if (SqlNormalizationHelper.IsConstantQuery(nestedQueryRaw)) {
      if (null == nestedQueryRaw.GetTable()) {
        nestedQueryRaw.SetTable(new SqlTable(SqlNormalizationHelper.NO_TABLE_QUERY_TABLE_NAME_PREFIX));
      }
    } else {
      try {
        nestedQuery = NormalizeTableExprWithQuery(nestedQueryRaw);
      } catch (Exception ex) {
     	//do nothing.
        ;
      }
      if (!CheckNormalizeSupport(mainQuery, nestedQuery)) {
        return mainQuery;
      }
    }

    nestedQuery = NormalizeTableAlias(nestedQuery);

    SqlCollection<SqlColumn> nestedColumns = nestedQuery.GetColumns();

    if (nestedQueryRaw != nestedQuery) {
      nestedTable = new SqlTable(nestedQuery, nestedTable.GetName(), nestedTable.GetAlias());
    }

    columns = ResolveMainQueryColumns(columns, nestedTable);

    if (NeedResolveNestedQueryAliasInMainQuery(nestedQuery)) {
      JavaHashtable<string, SqlCollection<SqlColumn>> tAlias2NestedColumnsMap = new JavaHashtable<string, SqlCollection<SqlColumn>>();
      string KEY = Utilities.ToUpper(nestedTable.GetAlias());
      tAlias2NestedColumnsMap.Put(KEY, nestedColumns);

      MainOwnerColumnsModifier mTableAliasModifier = new MainOwnerColumnsModifier(copyColumns, tAlias2NestedColumnsMap);
      mainQuery.SetColumns(columns);
      SqlParser parser = new SqlParser(null, mainQuery, StatementType.SELECT);
      mTableAliasModifier.Modify(parser);

      columns = mainQuery.GetColumns();
      condition = mainQuery.GetCriteria();
      havingClause = mainQuery.GetHavingClause();
      orderBy = mainQuery.GetOrderBy();
      groupBy = mainQuery.GetGroupBy();
    }

    if (nestedQuery.GetCriteria() != null
            || nestedQuery.GetOrderBy().Size() > 0
            || nestedQuery.GetGroupBy().Size() > 0) {
      if (!IsJoinQuery(nestedQuery)) {
        NestedQueryModifier nestedQueryModifier = new NestedQueryModifier(new SqlTable(mainTable.GetAlias(), mainTable.GetAlias()));
        nestedQueryModifier.Modify(new SqlParser(null, nestedQuery, StatementType.SELECT));
      }
    }

    SqlCollection<SqlColumn> populateColumns = PopulateNestedColumns2Main(nestedTable);

    condition = ResolveColumnAliasInCriteria(condition, populateColumns);

    condition = PopulateNestedWhere2Main(condition, nestedQuery.GetCriteria());

    havingClause = PopulateNestedHaving2Main(havingClause, ((SqlSelectStatement) nestedQuery).GetHavingClause());

    for (int i = 0 ; i < orderBy.Size(); ++i) {
      SqlOrderSpec od = orderBy.Get(i);
      SqlExpression oe = od.GetExpr();
      if (!(oe is SqlGeneralColumn)) {
        continue;
      }
      SqlColumn c = ResolveColumnAlias((SqlColumn) oe, populateColumns);
      if (c != oe) {
        od = new SqlOrderSpec(c, od.GetOrder(), od.IsNullsFirst(), od.HasNulls());
        orderBy.Set(i, od);
      }
    }

    orderBy = PopulateNestedOrder2Main(orderBy, nestedQuery.GetOrderBy(), nestedTable);

    for (int i = 0 ; i < groupBy.Size(); ++i) {
      SqlExpression group = groupBy.Get(i);
      if (!(group is SqlGeneralColumn)) {
        continue;
      }
      SqlColumn c = ResolveColumnAlias((SqlColumn) group, populateColumns);
      if (c != group) {
        groupBy.Set(i, c);
      }
    }

    groupBy = PopulateNestedGroup2Main(groupBy, nestedQuery.GetGroupBy());

    limitExpr = ResolveMainQueryOptions(limitExpr, nestedQuery.GetLimitExpr());

    if (SqlNormalizationHelper.IsImplicitJoin(nestedQuery)) {
      bool support = !mainTable.HasJoin();
      if (!support) {
        ThrowNoSupportNormalizationException("resolveMainTableWithQuery", mainQuery);
      }
      SqlCollection<SqlTable> nestedImplicitTables = ((SqlSelectStatement) nestedQuery).GetTables();
      mainQuery.SetColumns(columns);
      mainQuery.SetHavingClause(havingClause);
      mainQuery.SetCriteria(condition);
      mainQuery.SetOrderBy(orderBy);
      mainQuery.SetGroupByClause(groupBy, mainQuery.GetEachGroupBy());
      mainQuery.SetTables(nestedImplicitTables);
      return mainQuery;
    } else {
      SqlTable newTable = mainTable;
      string mainTableName = mainTable.GetName();
      if (mainTableName.StartsWith(ParserCore.DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX)) {
        SqlTable resolvedMain = nestedQuery.GetTable();
        if (mainTable.GetJoin() == null) {
          string mainTableAlias = NeedResolveNestedQueryAliasInMainQuery(nestedQuery) ? resolvedMain.GetAlias() : mainTable.GetAlias();
          newTable = new SqlTable(resolvedMain.GetCatalog(),
                  resolvedMain.GetSchema(),
                  resolvedMain.GetName(),
                  mainTableAlias,
                  resolvedMain.GetJoin(),
                  resolvedMain.GetNestedJoin(),
                  resolvedMain.GetQuery(),
                  mainTable.GetTableValueFunction(),
                  mainTable.GetCrossApply());
        } else {
          //TODO: SELECT * FROM (SELECT * FROM A JOIN B) AS NESTED_ALIAS JOIN C;
          SqlJoin j = mainTable.GetJoin();
          SqlConditionNode on = ResolveColumnAliasInCriteria(j.GetCondition(), populateColumns);
          if (on != j.GetCondition()) {
            j = new SqlJoin(j.GetJoinType(),
                    j.GetTable(),
                    on,
                    j.IsEach(),
                    j.HasOuter());
          }
          newTable = new SqlTable(resolvedMain.GetCatalog(),
                  resolvedMain.GetSchema(),
                  resolvedMain.GetName(),
                  mainTable.GetAlias(),
                  j,
                  mainTable.GetNestedJoin(),
                  null,
                  mainTable.GetTableValueFunction(),
                  mainTable.GetCrossApply());
        }
      }
      mainQuery.SetColumns(columns);
      mainQuery.SetHavingClause(havingClause);
      mainQuery.SetCriteria(condition);
      mainQuery.SetOrderBy(orderBy);
      mainQuery.SetGroupByClause(groupBy, mainQuery.GetEachGroupBy());
      mainQuery.SetTable(newTable);
      mainQuery.SetLimitExpr(limitExpr);
      mainQuery.SetOffsetExpr(offsetExpr);
      return mainQuery;
    }
  }

  private static SqlSelectStatement ResolveMainTableJoinWithQuery(SqlSelectStatement mainQuery, SqlTable [] topInOut) {
    SqlTable topTable = topInOut[0];
    SqlJoin join = topTable.GetJoin();
    SqlTable left = new SqlTable(topTable.GetCatalog(), topTable.GetSchema(), topTable.GetName(), topTable.GetAlias(), null, null, topTable.GetQuery(), topTable.GetTableValueFunction(), topTable.GetCrossApply());
    while (join != null) {
      SqlTable right = join.GetTable();
      if (right.IsNestedQueryTable()) {
        SqlQueryStatement nestedQuery = right.GetQuery();
        bool noWhereInNestedQuery = null == nestedQuery.GetCriteria();
        bool support = CheckNormalizeSupport(mainQuery, nestedQuery) && noWhereInNestedQuery;
        if (support) {
          mainQuery = ResolveMainTableWithQuery(mainQuery, right);
          SqlTable resolvedRight = new SqlTable(right.GetCatalog(), right.GetSchema(), nestedQuery.GetTableName(), right.GetAlias());
          SqlCollection<SqlColumn> populateColumns = PopulateNestedColumns2Main(right);
          SqlConditionNode on = ResolveColumnAliasInCriteria(join.GetCondition(), populateColumns);
          left = ConcatJoinTable(left, resolvedRight, join.GetJoinType(), on);
        } else {
          ThrowNoSupportNormalizationException(TABLE_EXPRESSION_EXCEPTION_CODE, mainQuery);
        }
      } else {
        left = right;
      }
      join = right.GetJoin();
    }
    topInOut[0] = left;
    mainQuery.SetTable(topInOut[0]);
    return mainQuery;
  }

  private static SqlCollection<SqlOrderSpec> PopulateNestedOrder2Main(SqlCollection<SqlOrderSpec> mainOrderBy,
                                                                      SqlCollection<SqlOrderSpec> nestedOrderBy,
                                                                      SqlTable nestedTable) {
    SqlTable outerOwner = new SqlTable(nestedTable.GetQuery().GetTable().GetAlias(), nestedTable.GetAlias());
    for (int i = 0 ; i < nestedOrderBy.Size(); ++i) {
      SqlOrderSpec o = nestedOrderBy.Get(i);
      SqlExprModifier modifier = new NestedExprPopulateModifier(outerOwner);
      if (modifier.Modify(o.GetExpr())) {
        SqlExpression expr = modifier.GetModifiedExpr();
        o = new SqlOrderSpec(expr, o.GetOrder(), o.IsNullsFirst(), o.HasNulls());
      }
      mainOrderBy.Add(o);
    }
    return mainOrderBy;
  }

  private static SqlCollection<SqlExpression> PopulateNestedGroup2Main(SqlCollection<SqlExpression> mainGroups, SqlCollection<SqlExpression> nestedGroups) {
    foreach(SqlExpression g in nestedGroups) {
      mainGroups.Add(g);
    }
    return mainGroups;
  }

  private static SqlCollection<SqlColumn> ResolveTableAliasInColumns(SqlCollection<SqlColumn> columns, SqlCollection<SqlTable> tables) {
    SqlCollection<SqlColumn> resovled = columns;
    for (int i = 0 ; i < columns.Size(); ++i) {
      SqlColumn c = columns.Get(i);
      c = ResovleTableAliasInMainQueryColumn(c, tables);
      columns.Set(i, c);
    }
    return resovled;
  }

  private static SqlCollection<SqlExpression> ResolveTableAliasInGroupBy(SqlCollection<SqlExpression> groups, SqlCollection<SqlTable> tables) {
    SqlCollection<SqlExpression> resovled = groups;
    for (int i = 0 ; i < groups.Size(); ++i) {
      SqlExpression g = groups.Get(i);
      if (g is SqlColumn) {
        SqlColumn c = (SqlColumn)g;
        c = ResovleTableAliasInMainQueryColumn(c, tables);
        groups.Set(i, c);
      }
    }
    return resovled;
  }

  private static SqlCollection<SqlOrderSpec> ResolveTableAliasInOrderBy(SqlCollection<SqlOrderSpec> orders, SqlCollection<SqlTable> tables) {
    SqlCollection<SqlOrderSpec> resovled = orders;
    for (int i = 0 ; i < orders.Size(); ++i) {
      SqlOrderSpec o = orders.Get(i);
      if (o.GetExpr() is SqlColumn) {
        SqlColumn c = (SqlColumn)o.GetExpr();
        c = ResovleTableAliasInMainQueryColumn(c, tables);
        o = new SqlOrderSpec(c, o.GetOrder(), o.IsNullsFirst(), o.HasNulls());
        orders.Set(i, o);
      }
    }
    return resovled;
  }

  private static SqlConditionNode ResolveTableAliasInCriteria(SqlConditionNode criteria, SqlCollection<SqlTable> tables) {
    SqlConditionNode resovled = criteria;
    if (criteria is SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (l is SqlConditionNode) {
        l = (SqlExpression) ResolveTableAliasInCriteria((SqlConditionNode)l, tables);
      }
      if (r is SqlConditionNode) {
        r = (SqlExpression) ResolveTableAliasInCriteria((SqlConditionNode)r, tables);
      }
      resovled = new SqlCondition(l, c.GetLogicOp(), r);
    } else if (criteria is SqlConditionExists) {
      SqlConditionExists c = (SqlConditionExists)criteria;
      //do nothing now.
    } else if (criteria is SqlConditionNot) {
      SqlConditionNot c = (SqlConditionNot)criteria;
      SqlConditionNode r = ResolveTableAliasInCriteria((SqlConditionNode)c.GetCondition(), tables);
      resovled = new SqlConditionNot(r);
    } else if (criteria is SqlConditionInSelect) {
      SqlConditionInSelect c = (SqlConditionInSelect)criteria;
      SqlExpression l = c.GetLeft();
      if (l is SqlColumn) {
        SqlColumn r = ResovleTableAliasInMainQueryColumn((SqlColumn) l, tables);
        resovled = new SqlConditionInSelect(r, c.GetRightQuery(), c.IsAll(), c.GetOperator());
      }
    } else if (criteria is SqlCriteria) {
      SqlCriteria c = (SqlCriteria)criteria;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (l is SqlColumn) {
        l = ResovleTableAliasInMainQueryColumn((SqlColumn)l, tables);
      }
      if (r is SqlColumn) {
        r = ResovleTableAliasInMainQueryColumn((SqlColumn)r, tables);
      }
      resovled = new SqlCriteria(l, c.GetOperator(), c.GetCustomOp(), r);
    }
    return resovled;
  }

  private static SqlConditionNode ResolveColumnAliasInCriteria(SqlConditionNode criteria, SqlCollection<SqlColumn> primitiveColumns) {
    SqlConditionNode resovled = criteria;
    if (criteria is SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (l is SqlConditionNode) {
        l = (SqlExpression)ResolveColumnAliasInCriteria((SqlConditionNode) l, primitiveColumns);
      }
      if (r is SqlConditionNode) {
        r = (SqlExpression)ResolveColumnAliasInCriteria((SqlConditionNode) r, primitiveColumns);
      }
      resovled = new SqlCondition(l, c.GetLogicOp(), r);
    } else if (criteria is SqlConditionExists) {
      SqlConditionExists c = (SqlConditionExists)criteria;
      //do nothing now.
    } else if (criteria is SqlConditionNot) {
      SqlConditionNot c = (SqlConditionNot)criteria;
      SqlConditionNode r = ResolveColumnAliasInCriteria((SqlConditionNode) c.GetCondition(), primitiveColumns);
      resovled = new SqlConditionNot(r);
    } else if (criteria is SqlConditionInSelect) {
      SqlConditionInSelect c = (SqlConditionInSelect)criteria;
      //do nothing now.
    } else if (criteria is SqlCriteria) {
      SqlCriteria c = (SqlCriteria)criteria;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (l is SqlColumn) {
        l = ResolveColumnAlias((SqlColumn) l, primitiveColumns);
      }
      if (r is SqlColumn) {
        r = ResolveColumnAlias((SqlColumn) r, primitiveColumns);
      }
      resovled = new SqlCriteria(l, c.GetOperator(), c.GetCustomOp(), r);
    }
    return resovled;
  }

  private static bool NeedResolveNestedQueryAliasInMainQuery(SqlQueryStatement nestedQuery) {
    //SELECT ... FROM (SELECT ... FROM A JOIN B JOIN C) NESTED_ALIAS WHERE NESTED_ALIAS.xxxx GROUP BY NESTED_ALIAS.xxxxx HAVING NESTED_ALIAS.xxxxx ORDER BY NESTED_ALIAS.xxxxx
    return nestedQuery != null && nestedQuery.GetJoins().Size() > 0;
  }

  private static SqlCollection<SqlColumn> PopulateNestedColumns2Main(SqlTable nestedTable) {
    SqlCollection<SqlColumn> populateColumns = new SqlCollection<SqlColumn>();
    SqlCollection<SqlColumn> nestedColumns = nestedTable.GetQuery().GetColumns();
    foreach(SqlColumn nc in nestedColumns) {
      SqlColumn pcolumn = nc;
      if (!nestedTable.GetQuery().IsJoinQuery()) {
        SqlTable outerOwner = new SqlTable(nestedTable.GetQuery().GetTable().GetAlias(), nestedTable.GetAlias());
        SqlExprModifier modifier = new NestedExprPopulateModifier(outerOwner);
        modifier.Modify(nc);
        SqlExpression expr = (SqlColumn) modifier.GetModifiedExpr();
        if (expr != null) {
          pcolumn = (SqlColumn) expr;
        }
      } else {
        //do nothing now
      }
      populateColumns.Add(pcolumn);
    }
    return populateColumns;
  }

  private static SqlConditionNode PopulateNestedWhere2Main(SqlConditionNode mainWhere, SqlConditionNode nestedWhere) {
    SqlConditionNode resolved = mainWhere;
    if (mainWhere != null && nestedWhere != null) {
      resolved = new SqlCondition(mainWhere, SqlLogicalOperator.And, nestedWhere);
    } else if (mainWhere != null ){
      resolved = mainWhere;
    } else if (nestedWhere != null) {
      resolved = nestedWhere;
    }
    return resolved;
  }

  private static SqlConditionNode PopulateNestedHaving2Main(SqlConditionNode mainHaving, SqlConditionNode nestedHaving) {
    SqlConditionNode resolved = mainHaving;
    if (nestedHaving != null) {
      if (mainHaving != null) {
        resolved = new SqlCondition((SqlExpression) mainHaving.Clone(), SqlLogicalOperator.And, nestedHaving);
      } else {
        resolved = nestedHaving;
      }
    }
    return resolved;
  }

  private static SqlCollection<SqlColumn> ResolveMainQueryColumns(SqlCollection<SqlColumn> mainColumns,
                                                                  SqlTable sourceNestedTable) {
    SqlCollection<SqlColumn> newMainColumns = new SqlCollection<SqlColumn>();
    for (int i = 0; i < mainColumns.Size(); ++i) {
      SqlColumn mc = mainColumns.Get(i);
      bool match = false;
      if (mc.GetTable() != null) {
        string owner = Utilities.ToUpper(mc.GetTable().GetAlias());
        if (Utilities.EqualIgnoreCaseInvariant(owner, sourceNestedTable.GetAlias())) {
          match = true;
        }
      } else {
        match = true;
      }
      if (match) {
        if (mc is SqlWildcardColumn) {
          SqlCollection<SqlColumn> populateColumns = PopulateNestedColumns2Main(sourceNestedTable);
          newMainColumns.AddAll(populateColumns);
        } else {
          MainColumnModifier mainColumnModifier = new MainColumnModifier(sourceNestedTable);
          mainColumnModifier.Modify(mc);
          mc = (SqlColumn) mainColumnModifier.GetModifiedExpr();
          newMainColumns.Add(mc);
        }
      } else {
        newMainColumns.Add(mc);
      }
    }
    return newMainColumns;
  }

  private static bool IsSimpleTable(SqlTable table) {
    return !table.IsNestedQueryTable() && !table.IsNestedJoinTable() && !table.HasJoin();
  }

  private static SqlColumn ResovleTableAliasInMainQueryColumn(SqlColumn mc, SqlCollection<SqlTable> flatten) {
    SqlColumn resovledColumn = mc;
    if (mc is SqlGeneralColumn && mc.GetTable() != null) {
      SqlTable tableAlias = mc.GetTable();
      SqlTable ownerOfColumn = MatchOwnerOfColumn(mc, flatten);
      if (ownerOfColumn != null && ownerOfColumn != tableAlias) {
        SqlTable attachTable = ownerOfColumn;
        if (mc is SqlWildcardColumn) {
          resovledColumn = new SqlWildcardColumn(attachTable);
        } else {
          resovledColumn = mc.HasAlias() ? new SqlGeneralColumn(attachTable, mc.GetColumnName(), mc.GetAlias()) : new SqlGeneralColumn(attachTable, mc.GetColumnName());
        }
      }
    } else if (mc is SqlFormulaColumn) {
      SqlFormulaColumn fc = (SqlFormulaColumn)mc;
      for (int i = 0 ; i < fc.GetParameters().Size(); ++i) {
        SqlExpression p = fc.GetParameters().Get(i);
        if (p is SqlGeneralColumn) {
          p = ResovleTableAliasInMainQueryColumn((SqlColumn) p, flatten);
          fc.GetParameters().Set(i, p);
        }
      }
    } else if (mc is SqlOperationColumn) {
      SqlOperationColumn oc = (SqlOperationColumn)mc;
      SqlOperationExpression oe = (SqlOperationExpression)oc.GetExpr();
      SqlExpression l = oe.GetLeft();
      SqlExpression r = oe.GetRight();
      if (l is SqlColumn) {
        l = ResovleTableAliasInMainQueryColumn((SqlColumn)l, flatten);
      }
      if (r is SqlColumn) {
        r = ResovleTableAliasInMainQueryColumn((SqlColumn)r, flatten);
      }
      resovledColumn = new SqlOperationColumn(oc.GetAlias(), new SqlOperationExpression(oe.GetOperator(), l, r));
    }
    return resovledColumn;
  }

  private static bool IsStandardQuery(SqlStatement query) {
    bool isStandard = true;
    if (query.GetColumns().Size() == 0) {
      isStandard = false;
    }
    if (query.GetTable() == null) {
      isStandard = false;
    }
    return isStandard;
  }

  private static bool IsJoinQuery(SqlQueryStatement query) {
    return query != null && query.GetJoins().Size() > 0 ? true : false;
  }

  private static bool ContainsAggragation(SqlCollection<SqlColumn> columns) {
    bool containsAgg = false;
    foreach(SqlColumn column in columns) {
      bool isAgg = ContainsAggragation(column);
      if (isAgg) {
        containsAgg = true;
        break;
      }
    }
    return containsAgg;
  }

  public static bool ContainsAggragation(SqlExpression expr) {
    bool containsAggragation = false;
    if (expr is SqlFormulaColumn) {
      SqlFormulaColumn formula = (SqlFormulaColumn)expr;
      SqlCollection<SqlExpression> paras = formula.GetParameters();
      bool isAgg = SqlUtilities.IsKnownAggragation(formula.GetColumnName());
      if (!isAgg) {
        foreach(SqlExpression para in paras) {
          isAgg = ContainsAggragation(para);
          if (isAgg) {
            containsAggragation = true;
            break;
          }
        }
      } else {
        //MAX(1), SUM(1),...etc.
        bool isConstantAgg = false;
        if (1 == paras.Size() && paras.Get(0) is SqlValueExpression) {
          isConstantAgg = true;
        }
        if (!isConstantAgg) {
          containsAggragation = true;
        }
      }
    }
    return containsAggragation;
  }

  private static void ThrowNoSupportNormalizationException(string code, SqlStatement statement) {
    Dialect dialect = statement.GetDialectProcessor();
    SqlBuilder builder;
    if (null != dialect) {
      builder = SqlBuilder.CreateBuilder(statement.GetDialectProcessor());
    } else {
      builder = SqlBuilder.CreateBuilder();
    }
    throw SqlExceptions.Exception(code, SqlExceptions.NO_SUPPORT_SQL_NORMALIZATION, builder.Build(statement));
  }

  private static JavaArrayList<NormalizationOptions> BuildConfigedOptions(NormalizationOption option) {
    JavaArrayList<NormalizationOptions> configed = new JavaArrayList<NormalizationOptions>();
    if (option.NormalizeImplicitCommaJoin()) {
      configed.Add(NormalizationOptions.ImplicitCommaJoin);
    } else if (option.NormalizeImplicitNaturalJoin()) {
      configed.Add(NormalizationOptions.ImplicitNaturalJoin);
    } else if (option.NormalizeImplicitCrossJoin()) {
      configed.Add(NormalizationOptions.ImplicitCrossJoin);
    } else if (option.NormalizeImplicitInnerJoin()) {
      configed.Add(NormalizationOptions.ImplicitInnerJoin);
    }

    if (option.NormalizeNestedQueryTableName()) {
      configed.Add(NormalizationOptions.AppendNestedQueryTableName);
    }

    if (option.NormalizeRightJoin()) {
      configed.Add(NormalizationOptions.RightJoin);
    }

    if (option.NormalizeNestedJoin()) {
      configed.Add(NormalizationOptions.NestedJoin);
    }

    if (option.NormalizeEquiInnerJoin()) {
      configed.Add(NormalizationOptions.EquiInnerJoin);
    }

    if (option.NormalizeOperationExpression()) {
      configed.Add(NormalizationOptions.OperationExpression);
    }

    if (option.NormalizeRemoveDistinctIfColumnUnique()) {
      configed.Add(NormalizationOptions.RemoveDistinctIfColumnUnique);
    }

    if (option.NormalizeTableExprWithQuery()) {
      configed.Add(NormalizationOptions.TableExprWithQuery);
    }

    if (option.NormalizeConstantColumn()) {
      configed.Add(NormalizationOptions.ConstantColumn);
    }

    if (option.NormalizeDistinct()) {
      configed.Add(NormalizationOptions.Distinct);
    }

    if (option.NormalizeCountDistinct()) {
      configed.Add(NormalizationOptions.Count_Distinct);
    }

    if (option.NormalizeCriteriaInJoin()) {
      configed.Add(NormalizationOptions.CriteriaInJoin);
    }

    if (option.NormalizeCriteriaWithSubQuery()) {
      configed.Add(NormalizationOptions.CriteriaWithSubQuery);
    }

    if (option.NormalizedSemiAntiJoin()) {
      configed.Add(NormalizationOptions.SemiAntiJoin);
    }

    if (option.NormalizePredictTrue()) {
      configed.Add(NormalizationOptions.PredictTrue);
    }

    if (option.NormalizeCriteriaWithNot()) {
      configed.Add(NormalizationOptions.CriteriaWithNot);
    }

    if (option.NormalizeCriteria()) {
      configed.Add(NormalizationOptions.Criteria);
    }

    if (option.NormalizeMinimizeCriteria()) {
      configed.Add(NormalizationOptions.MinimizeCriteria);
    }

    if (option.NormalizeFormulaAlias()) {
      configed.Add(NormalizationOptions.FormulaAlias);
    }

    if (option.NormalizeFixUniqueAlias()) {
      configed.Add(NormalizationOptions.FixUniqueAlias);
    }

    if (option.NormalizedFunctionSubstitute()) {
      configed.Add(NormalizationOptions.FunctionSubstitute);
    }

    if (option.NormalizeResolveTableAlias()) {
      configed.Add(NormalizationOptions.ResolveTableAlias);
    }
    return configed;
  }

}

sealed class FunctionModifier : SqlModifier {
  private readonly JavaArrayList<IFunctionSubstitute> _functions = new JavaArrayList<IFunctionSubstitute>();
  public FunctionModifier() {
    this._functions.Add(new NULLIFSubstitue());
    this._functions.Add(new COALESCESubstitue());
    this._functions.Add(new ATATVariableSubstitue());
    this._functions.Add(new IIFSubstitue());
  }



  protected override SqlColumn Visit(SqlColumn element) {

    foreach(IFunctionSubstitute fs in this._functions) {
      if (fs.Match(element)) {
        element = (SqlColumn) fs.Substitute(element);
      }
    }
    return base.Visit(element);
  }
}

sealed class ColumnOwnerModifier : SqlModifier {
  private readonly SqlCollection<SqlTable> _tables;
  private readonly IDataMetadata _dataMeta;
  private Exception _error = null;
  public ColumnOwnerModifier(SqlQueryStatement query, IDataMetadata dataMeta) {
    this._tables = SqlUtilities.GetTables(query, new SourceTableMatcher());
    this._dataMeta = dataMeta;
  }



  protected override SqlTable Visit(SqlTable element) {

    return element;
  }



  protected override SqlColumn Visit(SqlColumn element) {

    if (element.GetTable() != null) return element;

    if (!(element is SqlGeneralColumn)) return base.Visit(element);

    if (element is SqlWildcardColumn) {
      return element;
    }

    SqlGeneralColumn issuedColumn = (SqlGeneralColumn) element;

    if (null == this._dataMeta) {
      return issuedColumn;
    }

    SqlCollection<SqlTable> matched = new SqlCollection<SqlTable>();
    foreach(SqlTable t in this._tables) {
      ColumnInfo[] columns = this._dataMeta.GetTableMetadata(t.GetCatalog(), t.GetSchema(), t.GetName());
      if (columns == null || columns.Length == 0) continue;
      foreach(ColumnInfo c in columns) {
        if (Utilities.EqualIgnoreCase(c.GetColumnName(), issuedColumn.GetColumnName())) {
          matched.Add(t);
          break;
        }
      }
    }

    if (matched.Size() > 1) {
      if (this._error == null) {
        this._error = SqlExceptions.Exception(null, SqlExceptions.AMBIGUOUS_COLUMN_IN_FIELDS_LIST, element.GetColumnName());
      }
      return element;
    } else if (matched.Size() == 0) {
      if (this._error == null) {
        this._error = SqlExceptions.Exception(null, SqlExceptions.UNKNOWN_COLUMN_IN_FIELDS_LIST, element.GetColumnName());
      }
      return element;
    }

    SqlTable owner = matched.Get(0);

    if (issuedColumn.HasAlias()) {
      issuedColumn = new SqlGeneralColumn(new SqlTable(owner.GetAlias()), issuedColumn.GetColumnName(), issuedColumn.GetAlias());
    } else {
      issuedColumn = new SqlGeneralColumn(new SqlTable(owner.GetAlias()), issuedColumn.GetColumnName());
    }
    return issuedColumn;
  }

  public Exception GetError() {
    return this._error;
  }

  internal class SourceTableMatcher : ITableMatch {

    public SqlTable Create(SqlTable t) {
      return new SqlTable(t.GetCatalog(),
          t.GetSchema(),
          t.GetName(),
          t.GetAlias());
    }

    public bool Accept(SqlTable t, TablePartType type) {
      if (TablePartType.SimpleTable == type) return true;

      return (TablePartType.JoinPart == type &&
          (null == t.GetQuery() && null == t.GetNestedJoin()));
    }

    public bool Unwind(SqlTable t, TablePartType type) {
      return false;
    }
  }
}

class OperationColumnModifier : SqlModifier {
  private static bool IsOperationExpression(SqlExpression expr) {
    bool isOperation = false;
    if (expr is SqlOperationColumn) {
      isOperation = true;
    } else if (expr is SqlOperationExpression) {
      isOperation = true;
    } else if (expr is SqlFormulaColumn) {
      SqlFormulaColumn formula = (SqlFormulaColumn) expr;
      foreach(SqlExpression para in formula.GetParameters()) {
        if (para == null) continue;
        isOperation = IsOperationExpression(para);
        if (isOperation) break;
      }
    } else if (expr is SqlConditionNode) {
      isOperation = ContainOperationExpr((SqlConditionNode) expr);
    }
    return isOperation;
  }

  private static bool ContainOperationExpr(SqlConditionNode selector) {
    bool contains = false;
    if (selector is SqlCriteria) {
      SqlCriteria c = (SqlCriteria)selector;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (IsOperationExpression(l) || IsOperationExpression(r)) {
        contains = true;
      }
    } else if (selector is SqlCondition) {
      SqlCondition c = (SqlCondition)selector;
      if (c.GetLeft() is SqlConditionNode) {
        contains = ContainOperationExpr((SqlConditionNode)c.GetLeft());
      }
      if (!contains && c.GetRight() is SqlConditionNode) {
        contains = ContainOperationExpr((SqlConditionNode)c.GetRight());
      }
    }
    return contains;
  }

  private static SqlConditionNode ResolveOperationSelector(SqlConditionNode selector) {
    SqlConditionNode resolved = selector;
    if (selector is SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)selector;
      SqlExpression l = criteria.GetLeft();
      if (IsOperationExpression(l)) {
        OperationExprModifier modifier = new OperationExprModifier();
        modifier.Modify(l);
        l = modifier.GetModifiedExpr();
      }
      SqlExpression r = criteria.GetRight();
      if (IsOperationExpression(r)) {
        OperationExprModifier modifier = new OperationExprModifier();
        modifier.Modify(r);
        r = modifier.GetModifiedExpr();
      }
      resolved = new SqlCriteria(l, criteria.GetOperator(), criteria.GetCustomOp(), r, criteria.GetEscape());
    } else if (selector is SqlCondition) {
      SqlCondition condition = (SqlCondition) selector;
      SqlExpression l = condition.GetLeft();
      if (l is SqlConditionNode) {
        if (ContainOperationExpr((SqlConditionNode)l)) {
          l = ResolveOperationSelector((SqlConditionNode)l);
        }
      }
      SqlExpression r = condition.GetRight();
      if (r is SqlConditionNode) {
        if (ContainOperationExpr((SqlConditionNode)r)) {
          r = ResolveOperationSelector((SqlConditionNode)r);
        }
      }
      resolved = new SqlCondition(l, condition.GetLogicOp(), r);
    }
    return resolved;
  }


  protected override SqlConditionNode Visit(SqlConditionNode element) {

    if (ContainOperationExpr(element)) {
      element = ResolveOperationSelector(element);
    }
    return element;
  }


  protected override SqlTable Visit(SqlTable element) {

    SqlQueryStatement nestedQuery = element.GetQuery();
    SqlTable nestedJoin = element.GetNestedJoin();
    SqlJoin join = element.GetJoin();
    bool modified = false;
    if (nestedQuery != null) {
      SqlModifier modifier = new OperationColumnModifier();
      modified = modifier.Modify(new SqlParser(nestedQuery));
    }

    if (nestedJoin != null) {
      SqlTable modifiedNJ = Visit(nestedJoin);
      if (modifiedNJ != nestedJoin) {
        nestedJoin = modifiedNJ;
        modified = true;
      }
    }
    if (join != null) {
      SqlTable modifiedR = Visit(join.GetTable());
      SqlConditionNode modifiedOn = Visit(join.GetCondition());
      if (modifiedR != join.GetTable() || modifiedOn != join.GetCondition()) {
        join = new SqlJoin(join.GetJoinType(),
                modifiedR,
                modifiedOn,
                join.IsEach(),
                join.HasOuter());
        modified = true;
      }
    }
    if (modified) {
      element = new SqlTable(element.GetCatalog(),
              element.GetSchema(),
              element.GetName(),
              element.GetAlias(),
              join,
              nestedJoin,
              nestedQuery,
              element.GetTableValueFunction(),
              element.GetCrossApply());
    }
    return element;
  }


  protected override SqlColumn Visit(SqlColumn element) {

    if (IsOperationExpression(element)) {
      OperationExprModifier modifier = new OperationExprModifier();
      modifier.Modify(element);
      return (SqlColumn) modifier.GetModifiedExpr();
    } else if (element is SqlFormulaColumn && Utilities.EqualIgnoreCase("IF", element.GetColumnName())) {
      SqlFormulaColumn IF = (SqlFormulaColumn) element;
      SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
      paras.Add(null);
      foreach(SqlExpression p in IF.GetParameters()) {
        paras.Add(p);
      }
      SqlFormulaColumn caseWhenF = new SqlFormulaColumn("CASE", IF.GetAlias(), paras);
      return caseWhenF;
    } else if (SqlNormalization.ContainsAggragation(element)) {
      SqlFormulaColumn agg = (SqlFormulaColumn) element;
      if (1 == agg.GetParameters().Size()
              && agg.GetParameters().Get(0) is SqlFormulaColumn) {
        SqlFormulaColumn negate = (SqlFormulaColumn) agg.GetParameters().Get(0);
        if (Utilities.EqualIgnoreCase("NEGATE", negate.GetColumnName())) {
          SqlExpression expr = negate.GetParameters().Get(0);
          if (Utilities.EqualIgnoreCase("MIN", agg.GetColumnName())) {
            agg = new SqlFormulaColumn("MAX", agg.GetAlias(), agg.GetParameters());
            agg.GetParameters().Set(0, expr);
          } else if (Utilities.EqualIgnoreCase("MAX", agg.GetColumnName())) {
            agg = new SqlFormulaColumn("MIN", agg.GetAlias(), agg.GetParameters());
            agg.GetParameters().Set(0, expr);
          } else {
            agg.GetParameters().Set(0, expr);
          }
          negate.GetParameters().Set(0, agg);
          negate = new SqlFormulaColumn("NEGATE", agg.GetAlias(), negate.GetParameters());
          return negate;
        }
        return base.Visit(element);
      }
    }
    return element;
  }



  protected override SqlExpression Visit(SqlExpression element) {

    if (IsOperationExpression(element)) {
      OperationExprModifier modifier = new OperationExprModifier();
      modifier.Modify(element);
      return modifier.GetModifiedExpr();
    }
    return base.Visit(element);
  }
}

class MainOwnerColumnsModifier : SqlModifier {
  private JavaHashtable<string, SqlCollection<SqlColumn>> _tAlias2NestedColumnsMap;
  private SqlCollection<SqlColumn> _originalMainColumns;
  public MainOwnerColumnsModifier(SqlCollection<SqlColumn> originalMainColumns, JavaHashtable<string, SqlCollection<SqlColumn>> tAlias2NestedColumnsMap) {
    this._originalMainColumns = originalMainColumns;
    this._tAlias2NestedColumnsMap = tAlias2NestedColumnsMap;
  }



  protected override SqlTable Visit(SqlTable element) {

    if (element == null) return element;

    if (element.HasJoin()) {
      SqlJoin j = element.GetJoin();
      SqlJoin tempJ = this.Visit(j);
      if (j != tempJ) {
        element = new SqlTable(element.GetCatalog(),
            element.GetSchema(),
            element.GetName(),
            element.GetAlias(),
            tempJ,
            element.GetNestedJoin(),
            element.GetQuery(),
            element.GetTableValueFunction(),
            element.GetCrossApply());
      }
    }
    return element;
  }

  

  protected override SqlColumn Visit(SqlColumn element) {

    if (!(element is SqlGeneralColumn)){
      return base.Visit(element);
    }
    SqlGeneralColumn gc = (SqlGeneralColumn) element;
    SqlGeneralColumn omc = (SqlGeneralColumn) element;
    foreach(SqlColumn om in this._originalMainColumns) {
      if (om is SqlFormulaColumn) continue;
      if (Utilities.EqualIgnoreCase(om.GetAlias(), element.GetAlias())) {
        omc = (SqlGeneralColumn) om;
        break;
      }
    }
    if (gc.GetTable() != null) {
      string alias = gc.GetTable().GetAlias();
      string KEY = Utilities.ToUpper(alias);
      if (this._tAlias2NestedColumnsMap.ContainsKey(KEY)) {
        SqlColumn innerColumn = GetMappingNestedColumn(omc, this._tAlias2NestedColumnsMap.Get(KEY));
        if (innerColumn != null && innerColumn.GetTable() != null) {
          if (this._clause == CLAUSE_TYPE.COLUMNS) {
            element = gc.HasAlias() ? new SqlGeneralColumn(innerColumn.GetTable(), innerColumn.GetColumnName(), gc.GetAlias()) : new SqlGeneralColumn(innerColumn.GetTable(), innerColumn.GetColumnName());
          } else if (this._clause ==CLAUSE_TYPE.CRITERIA
            || this._clause ==CLAUSE_TYPE.HAVING){
            element = new SqlGeneralColumn(innerColumn.GetTable(), innerColumn.GetColumnName());
          } else {
            element = new SqlGeneralColumn(gc.GetAlias());
          }
        }
      }
    }
    return element;
  }



  protected override SqlOrderSpec Visit(SqlOrderSpec element) {

    if (this._clause == CLAUSE_TYPE.ORDERBY) {
      if (element.GetExpr() is SqlColumn) {
        SqlColumn column = Visit((SqlColumn)element.GetExpr());
        element = new SqlOrderSpec(column, element.GetOrder(), element.IsNullsFirst(), element.HasNulls());
      }
    }
    return element;
  }

  private SqlColumn GetMappingNestedColumn(SqlColumn mc, SqlCollection<SqlColumn> nestedColumns) {
    SqlColumn mappingColumn = null;
    foreach(SqlColumn nc in nestedColumns) {
      if (Utilities.EqualIgnoreCaseInvariant(mc.GetColumnName(), nc.GetAlias())) {
        mappingColumn = nc;
        break;
      }
    }
    return mappingColumn;
  }
}

abstract class SqlExprModifier {
  internal protected SqlExpression _originalExpr;
  internal protected EXPR_BELONGS _exprBelongs;
  private SqlExpression _modifiedExpr;
  public enum EXPR_BELONGS
{
NONE,
COLUMNS,
FORMULA,
CRITERIA}

  internal protected SqlExprModifier() {
    this._exprBelongs = EXPR_BELONGS.NONE;
  }

  internal protected bool Modify(SqlExpression expr) {
    this._originalExpr = expr;
    this._modifiedExpr = Visit(this._originalExpr);
    return (this._modifiedExpr != this._originalExpr);
  }

  internal protected SqlExpression GetModifiedExpr() {
    return this._modifiedExpr;
  }

  internal protected virtual SqlExpression Visit(SqlExpression element) {
    SqlExpression modified = element;
    if (element is SqlColumn) {
      if (this._exprBelongs == EXPR_BELONGS.NONE) {
        this._exprBelongs = EXPR_BELONGS.COLUMNS;
      }
      modified = Visit((SqlColumn) element);
    } else if (element is SqlConditionNode) {
      if (this._exprBelongs == EXPR_BELONGS.NONE) {
        this._exprBelongs = EXPR_BELONGS.CRITERIA;
      }
      modified = Visit((SqlConditionNode) element);
    }
    return modified;
  }

  internal protected virtual SqlColumn Visit(SqlColumn element) {
    SqlColumn modified = element;
    if (element is SqlGeneralColumn) {
      modified = Visit((SqlGeneralColumn) element);
    } else if (element is SqlFormulaColumn) {
      modified = Visit((SqlFormulaColumn) element);
    } else if (element is SqlOperationColumn) {
      modified = Visit((SqlOperationColumn) element);
    } else if (element is SqlConstantColumn) {
      modified = Visit((SqlConstantColumn) element);
    } else if (element is SqlSubQueryColumn) {
      modified = Visit((SqlSubQueryColumn)element);
    }
    return modified;
  }

  internal protected virtual SqlColumn Visit(SqlGeneralColumn element) {
    return element;
  }

  internal protected virtual SqlColumn Visit(SqlFormulaColumn element) {
    SqlCollection<SqlExpression> modifiedParas = new SqlCollection<SqlExpression>();
    EXPR_BELONGS belonged = this._exprBelongs;
    this._exprBelongs = EXPR_BELONGS.FORMULA;
    foreach(SqlExpression para in element.GetParameters()) {
      SqlExpression modifiedPara = Visit(para);
      modifiedParas.Add(modifiedPara);
    }
    this._exprBelongs = belonged;
    SqlColumn modified = new SqlFormulaColumn(element.GetColumnName(), element.GetAlias(), modifiedParas);
    return modified;
  }

  internal protected virtual SqlColumn Visit(SqlOperationColumn element) {
    SqlOperationExpression opExpr = (SqlOperationExpression) element.GetExpr();
    SqlOperationExpression modifiedExpr = opExpr;
    SqlExpression left = opExpr.GetLeft();
    SqlExpression right = opExpr.GetRight();
    SqlExpression modifiedLeft = Visit(left);
    SqlExpression modifiedRight = Visit(right);
    if (modifiedLeft != left || modifiedRight != right) {
      modifiedExpr = new SqlOperationExpression(opExpr.GetOperator(), modifiedLeft, modifiedRight);
    }
    if (modifiedExpr != opExpr) {
      return new SqlOperationColumn(element.GetAlias(), modifiedExpr);
    } else {
      return element;
    }
  }

  internal protected virtual SqlColumn Visit(SqlSubQueryColumn element) {
    return element;
  }

  internal protected virtual SqlColumn Visit(SqlConstantColumn element) {
    return element;
  }

  internal protected virtual SqlConditionNode Visit(SqlConditionNode element) {
    if (element is SqlCondition) {
      SqlCondition condition = (SqlCondition) element;
      SqlExpression left = condition.GetLeft();
      SqlExpression ml = Visit(left);
      SqlExpression right = condition.GetRight();
      SqlExpression mr = Visit(right);
      if (ml != left || mr != right) {
        element = new SqlCondition(ml, condition.GetLogicOp(), mr);
        return element;
      }
    } else if (element is SqlCriteria) {
      SqlCriteria c = (SqlCriteria) element;
      SqlExpression left = c.GetLeft();
      SqlExpression ml = Visit(left);
      SqlExpression right = c.GetRight();
      SqlExpression mr = Visit(right);
      if (ml != left || mr != right) {
        element = new SqlCriteria(ml, c.GetOperator(), c.GetCustomOp(), mr, c.GetEscape());
        return element;
      }
    } else if (element is SqlConditionNot) {
      SqlConditionNot Not = (SqlConditionNot) element;
      SqlExpression expr = Visit(Not.GetCondition());
      if (expr != Not.GetCondition()) {
        return new SqlConditionNot(expr);
      }
    }
    return element;
  }
}

class PredictTrueModifier : SqlExprModifier {
  internal protected override SqlConditionNode Visit(SqlConditionNode element) {
    if (element is SqlCondition) {
      SqlCondition condition = (SqlCondition) element;
      SqlExpression left = condition.GetLeft();
      if (left is SqlConditionNode) {
        left = Visit((SqlConditionNode) left);
      } else {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        left = new SqlCriteria(left, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
      }
      SqlExpression right = condition.GetRight();
      if (right is SqlConditionNode) {
        right = Visit((SqlConditionNode) right);
      } else {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        right = new SqlCriteria(right, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
      }
      if (left != condition.GetLeft() || right != condition.GetRight()) {
        element = new SqlCondition(left, condition.GetLogicOp(), right);
        return element;
      }
    } else if (element is SqlConditionNot) {
      SqlConditionNot not = (SqlConditionNot) element;
      SqlExpression expr = not.GetCondition();
      if (!(expr is SqlConditionNode)) {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        return new SqlConditionNot(new SqlCriteria(expr,
                ComparisonType.IS,
                SqlCriteria.CUSTOM_OP_PREDICT_TRUE,
                new SqlValueExpression(trueValue)));
      }
    } else {
      return base.Visit(element);
    }
    return element;
  }
}

class MainColumnModifier : SqlExprModifier {
  internal SqlTable _sourceNestedTable;
  internal protected MainColumnModifier(SqlTable sourceNestedTable) : base() {
    this._sourceNestedTable = sourceNestedTable;
  }

  internal protected override SqlColumn Visit(SqlGeneralColumn element) {
    SqlColumn modified = element;
    SqlColumn mappingColumn = SqlNormalizationHelper.GetMappingNestedColumn(element, this._sourceNestedTable.GetQuery().GetColumns());
    if (element is SqlWildcardColumn) {
      if (this._exprBelongs == EXPR_BELONGS.FORMULA) {
        //do nothing.
      } else {
        modified = Populate2MainColumn(mappingColumn,
            element,
            this._sourceNestedTable.GetQuery().GetTable().HasJoin() ? null : element.GetTable());
      }
    } else if (mappingColumn != null) {
      modified = Populate2MainColumn(mappingColumn,
          element,
          this._sourceNestedTable.GetQuery().GetTable().HasJoin() ? null : element.GetTable());
      if (this._exprBelongs == EXPR_BELONGS.FORMULA) {
        if (modified.HasAlias()) {
          if (modified is SqlGeneralColumn) {
            // There are 2 cases, those columns are as following,
            // 1. SELECT q.c1, sum(q.c1) FROM (SELECT c c1 FROM ...) q
            //    q.c1 -> q.c c1
            //    sum(q.c1) -> sum(q.c c1) -> sum(q.c)
            // 2. SELECT q.c1, sum(q.c1) FROM (SELECT t2.c c1,t1.c c2 FROM t1 JOIN t2 ...) q
            //    q.c1 -> q.c1 c1 -> t2.c c1
            //    sum(q.c1) -> sum(q.c c1) -> sum(q.c1 c1) -> sum(t2.c)
            SqlQueryStatement nq = this._sourceNestedTable.GetQuery();
            if (!nq.IsJoinQuery()) {
              modified = new SqlGeneralColumn(modified.GetTable(),
                modified.GetColumnName());
            } else {
              modified = new SqlGeneralColumn(modified.GetTable(),
                modified.GetAlias());
            }
          } else if(modified is SqlFormulaColumn) {
            modified = new SqlFormulaColumn(modified.GetColumnName(),
                ((SqlFormulaColumn) modified).GetParameters(),
                ((SqlFormulaColumn) modified).GetOverClause());
          }
        }
      }
    }
    return modified;
  }

  internal protected override SqlConditionNode Visit(SqlConditionNode element) {
    SqlConditionNode modified = element;
    if (element is SqlCondition) {
      SqlCondition mc = (SqlCondition) element;
      SqlExpression left = Visit(mc.GetLeft());
      SqlExpression right = Visit(mc.GetRight());
      modified = new SqlCondition(left, mc.GetLogicOp(), right);
    } else if (element is SqlCriteria) {
      SqlCriteria mc = (SqlCriteria) element;
      SqlExpression left = Visit(mc.GetLeft());
      SqlExpression right = Visit(mc.GetRight());
      modified = new SqlCriteria(left, mc.GetOperator(), mc.GetCustomOp(), right, mc.GetEscape());
    } else if (element is SqlConditionNot) {
      SqlConditionNot  cNot = (SqlConditionNot) element;
      modified = new SqlConditionNot(Visit(cNot.GetCondition()));
    } else if (element is SqlConditionExists) {
      //do nothing.
    } else if (element is SqlConditionInSelect) {
      //do nothing.
    }
    return modified;
  }

  private static SqlColumn Populate2MainColumn(SqlColumn c, SqlColumn m, SqlTable ow) {
    SqlColumn resovled = c;
    if (c is SqlGeneralColumn) {
      if (m != null) {
        if (m is SqlWildcardColumn) {
          if (c is SqlWildcardColumn) {
            //do nothing.
          } else {
            if (m.GetTable() != null) {
              SqlTable owner =  m.GetTable();
              if (c.GetTable() != null) {
                SqlTable t1 = c.GetTable();
                SqlTable t2 = m.GetTable();
                if (!Utilities.EqualIgnoreCase(t1.GetValidName(), t2.GetValidName())) {
                  owner = c.GetTable();
                }
              }
              if (c.HasAlias()) {
                resovled = new SqlGeneralColumn(owner, c.GetColumnName(), c.GetAlias());
              } else {
                resovled = new SqlGeneralColumn(owner, c.GetColumnName());
              }
            } else {
              resovled = c;
            }
          }
        } else {
          SqlTable t = null;
          if (c.GetTable() == null) {
            t = m.GetTable();
          } else {
            t = m.GetTable();
          }
          if (m.HasAlias()) {
            resovled = new SqlGeneralColumn(t, c.GetColumnName(), m.GetAlias());
          } else {
            bool hasAlias = true;
            string n1 = c.GetColumnName();
            string n2 = m.GetColumnName();
            if (n1.Equals(n2)) {
              hasAlias = false;
            }
            if (hasAlias) {
              resovled = new SqlGeneralColumn(t, n1, n2);
            } else {
              resovled = new SqlGeneralColumn(t, n1);
            }
          }
        }
      } else {
        resovled = (SqlColumn) c.Clone();
      }
    } else if (c is SqlFormulaColumn) {
      SqlFormulaColumn fc = (SqlFormulaColumn)c;
      if (m != null) {
        SqlCollection<SqlExpression> popluateParas = new SqlCollection<SqlExpression>();
        NestedExprPopulateModifier populateModifier = new NestedExprPopulateModifier(ow);
        foreach(SqlExpression p in fc.GetParameters()) {
          populateModifier.Modify(p);
          SqlExpression populatePara = populateModifier.GetModifiedExpr();
          popluateParas.Add(populatePara);
        }
        resovled = new SqlFormulaColumn(c.GetColumnName(), m.GetAlias(), popluateParas);
      } else {
        resovled = (SqlColumn)c.Clone();
      }
    } else if (c is SqlConstantColumn) {
      if (m != null) {
        resovled = new SqlConstantColumn(m.GetAlias(), (SqlValueExpression)c.GetExpr());
      } else {
        resovled = (SqlColumn)c.Clone();
      }
    } else if (c is SqlOperationColumn) {
      if (m != null) {
        NestedExprPopulateModifier populateModifier = new NestedExprPopulateModifier(m.GetTable());
        populateModifier.Modify(c);
        SqlOperationColumn modified = (SqlOperationColumn) populateModifier.GetModifiedExpr();
        resovled = new SqlOperationColumn(m.GetAlias(), (SqlOperationExpression) modified.GetExpr());
      } else {
        resovled = (SqlColumn)c.Clone();
      }
    } else if (c is SqlSubQueryColumn) {
      if (m != null) {
        resovled = new SqlSubQueryColumn(m.GetAlias(), (SqlSubQueryExpression)c.GetExpr());
      } else {
        resovled = (SqlColumn)c.Clone();
      }
    }
    return resovled;
  }
}

class NestedExprPopulateModifier : SqlExprModifier {
  private SqlTable _outerOwner;
  internal protected NestedExprPopulateModifier(SqlTable outerOwner) {
    this._outerOwner = outerOwner;
  }

  internal protected override SqlColumn Visit(SqlGeneralColumn element) {
    SqlColumn modified = element;
    SqlTable outerOwner = this._outerOwner != null ? new SqlTable(this._outerOwner.GetAlias()) : element.GetTable();

    if (element is SqlWildcardColumn) {
      modified = new SqlWildcardColumn(outerOwner);
      return modified;
    }

    if (element.HasAlias()) {
      modified = new SqlGeneralColumn(outerOwner, element.GetColumnName(), element.GetAlias());
    } else {
      modified = new SqlGeneralColumn(outerOwner, element.GetColumnName());
    }
    return modified;
  }
}

class OperationExprModifier : SqlExprModifier {
  internal protected override SqlExpression Visit(SqlExpression element) {
    if (element is SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) element;
      SqlExpression l = opExpr.GetLeft();
      SqlExpression r = opExpr.GetRight();
      SqlCollection<SqlExpression> PARAS = new SqlCollection<SqlExpression>();
      PARAS.Add(new SqlValueExpression(SqlValueType.STRING, opExpr.GetOperatorAsString()));
      PARAS.Add(this.Visit(l));
      PARAS.Add(this.Visit(r));
      SqlFormulaColumn EXPR = new SqlFormulaColumn("EXPR", PARAS);
      return EXPR;
    }
    return base.Visit(element);
  }

  internal protected override SqlColumn Visit(SqlOperationColumn element) {
    SqlOperationExpression opExpr = (SqlOperationExpression) element.GetExpr();
    SqlFormulaColumn EXPR = (SqlFormulaColumn) this.Visit(opExpr);
    if (element.HasAlias()) {
      return new SqlFormulaColumn(EXPR.GetColumnName(),
          element.GetAlias(),
          EXPR.GetParameters(),
          EXPR.GetOverClause());
    } else {
      return new SqlFormulaColumn(EXPR.GetColumnName(),
          EXPR.GetParameters(),
          EXPR.GetOverClause());
    }
  }

  internal protected override SqlColumn Visit(SqlFormulaColumn element) {
    if (Utilities.EqualIgnoreCase("EXPR", element.GetColumnName())) {
      if (element.GetParameters().Size() == 1 && element.GetParameters().Get(0) is SqlOperationExpression) {
        SqlFormulaColumn modified = (SqlFormulaColumn)this.Visit(element.GetParameters().Get(0));
        return new SqlFormulaColumn(element.GetColumnName(),
                element.GetAlias(),
                modified.GetParameters(),
                element.GetOverClause());
      }
    }
    return base.Visit(element);
  }
}

class NestedQueryModifier : SqlModifier {
  private SqlTable _outerOwner;
  public NestedQueryModifier(SqlTable outerOwner) {
   this._outerOwner = outerOwner;
  }



  protected override SqlColumn Visit(SqlColumn element) {

    if (element is SqlGeneralColumn) {
      SqlColumn c = (SqlColumn) element;
      if (this._clause == CLAUSE_TYPE.CRITERIA ||
              this._clause == CLAUSE_TYPE.HAVING) {
        element = new SqlGeneralColumn(this._outerOwner, c.GetColumnName());
      } else if (this._clause == CLAUSE_TYPE.ORDERBY) {
        element = SqlNormalization.ResolveColumnAlias(c, this._statement.GetColumns());
        element = new SqlGeneralColumn(this._outerOwner, element.GetColumnName());
      } else if (this._clause == CLAUSE_TYPE.GROUPBY) {
        element = SqlNormalization.ResolveColumnAlias(c, this._statement.GetColumns());
        element = new SqlGeneralColumn(this._outerOwner, element.GetColumnName());
      }
    } else if (element is SqlOperationColumn) {
      //do nothing now.
    } else if (element is SqlConstantColumn) {
      //do nothing now.
    } else if (element is SqlFormulaColumn) {
      if (this._clause == CLAUSE_TYPE.CRITERIA
          || this._clause == CLAUSE_TYPE.GROUPBY
          || this._clause == CLAUSE_TYPE.ORDERBY) {
        SqlFormulaColumn fc = (SqlFormulaColumn) element;
        SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
        foreach(SqlExpression p in fc.GetParameters()) {
          paras.Add(Visit(p));
        }
        element = new SqlFormulaColumn(fc.GetColumnName(), paras);
      }
    } else if (element is SqlSubQueryColumn) {
      //do nothing now.
    }
    return element;
  }



  protected override SqlOrderSpec Visit(SqlOrderSpec element) {

    if (this._clause == CLAUSE_TYPE.ORDERBY) {
      if (element.GetExpr() is SqlColumn) {
        SqlColumn column = Visit((SqlColumn)element.GetExpr());
        element = new SqlOrderSpec(column, element.GetOrder(), element.IsNullsFirst(), element.HasNulls());
      }
    }
    return element;
  }


  protected override SqlConditionNode Visit(SqlConditionNode element) {

    if (element is SqlConditionExists) {
      return element;
    } else if (element is SqlConditionInSelect) {
      return element;
    } else {
      return base.Visit(element);
    }
  }



  protected override SqlTable Visit(SqlTable element) {

    return element;
  }
}

class SqlNormalizationException : RSBException {
  private static string GetDetails(IList<Exception> exceptions) {
    ByteBuffer builder = new ByteBuffer();
    foreach(Exception ex in exceptions) {
      if (builder.Length > 0) {
        builder.Append(Utilities.SYS_LINESEPARATOR);
      }
      builder.Append(Utilities.GetExceptionMessage(ex));
    }
    return builder.ToString();
  }
  public SqlNormalizationException(string code, IList<Exception> exceptions) : base(code, GetDetails(exceptions)) {
  }
}

class SqlNormalizationHelper {
  internal protected const string NO_TABLE_QUERY_TABLE_NAME_PREFIX = "NO_TABLE_QUERY_TABLE";
  internal protected static SqlColumn GetMappingNestedColumn(SqlColumn mc, SqlCollection<SqlColumn> nestedColumns) {
    SqlColumn mappingColumn = null;
    foreach(SqlColumn nc in nestedColumns) {
      if (mc is SqlWildcardColumn) {
        mappingColumn = nc;
        break;
      }
      if (Utilities.EqualIgnoreCaseInvariant(mc.GetColumnName(), nc.GetAlias())) {
        mappingColumn = nc;
        break;
      }
    }
    return mappingColumn;
  }

  internal protected static void FlattenTables(SqlCollection<SqlTable> flattens, SqlTable main) {
    if (main == null) return;
    if (main.IsNestedQueryTable()) {
      flattens.Add(main);
    } else if (main.IsNestedJoinTable()) {
      if (main.HasAlias()) {
        flattens.Add(main);
      }
      FlattenTables(flattens, main.GetNestedJoin());
    } else {
      if (main.HasAlias()) {
        flattens.Add(new SqlTable(main.GetName(), main.GetAlias()));
      } else {
        flattens.Add(new SqlTable(main.GetName()));
      }
    }
    if (main.HasJoin()) {
      FlattenTables(flattens, main.GetJoin().GetTable());
    }
    // THIS is a hack for Cross Apply.
    // We add each one as a pseudo table so that
    // we correctly resolve prefixes during normalization
    SqlCrossApply ca = main.GetCrossApply();
    while ( ca != null ) {
      flattens.Add(ca.GetPseudoTable());
      ca = ca.GetCrossApply();
    }
  }

  internal protected static SqlCollection<SqlTable> FlattenTables(SqlQueryStatement query) {
    SqlCollection<SqlTable> flattens = new SqlCollection<SqlTable>();
    if (IsImplicitJoin(query)) {
      SqlCollection<SqlTable> implicitTables = ((SqlSelectStatement)query).GetTables();
      foreach(SqlTable iTable in implicitTables) {
        SqlNormalizationHelper.FlattenTables(flattens, iTable);
      }
    } else {
      SqlNormalizationHelper.FlattenTables(flattens, query.GetTable());
    }
    return flattens;
  }

  internal protected static bool IsImplicitJoin(SqlQueryStatement query) {
    bool implicitJoin = false;
    if (query is SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement)query;
      if (select.GetTables().Size() > 1) {
        implicitJoin = true;
      }
    }
    return implicitJoin;
  }

  internal protected static bool IsConstantQuery(SqlQueryStatement query) {
    if (query.GetGroupBy().Size() > 0) return false;

    bool containsAggConstant = false;
    bool containsGeneralColumn = false;
    SqlCollection<SqlColumn> columns = query.GetColumns();
    foreach(SqlColumn c in columns) {
      if (c is SqlFormulaColumn) {
        SqlFormulaColumn function = (SqlFormulaColumn)c;
        string aggFun =  function.GetColumnName();
        if (Utilities.EqualIgnoreCase("SUM", Utilities.ToUpper(aggFun)) || Utilities.EqualIgnoreCase("COUNT", Utilities.ToUpper(aggFun)) || Utilities.EqualIgnoreCase("COUNT_BIG", Utilities.ToUpper(aggFun)))
          return false;

        if (SqlUtilities.IsKnownAggragation(aggFun)) {
          SqlCollection<SqlExpression> paras = function.GetParameters();
          if (paras.Get(0) is SqlValueExpression) {
            containsAggConstant = true;
          } else {
            containsGeneralColumn = true;
          }
        }
      } else if (c is SqlConstantColumn) {
        if (null == query.GetTable() || Utilities.EqualIgnoreCase(NO_TABLE_QUERY_TABLE_NAME_PREFIX, query.GetTableName())) {
          containsAggConstant = true;
        }
      } else {
        containsGeneralColumn = true;
      }
    }
    return !containsGeneralColumn && containsAggConstant;
  }

  internal protected static bool ContainsNestedJoin(SqlTable table) {
    bool contain = false;
    if (table.IsNestedJoinTable()) {
      contain = true;
    } else {
      if (table.HasJoin()) {
        SqlJoin j = table.GetJoin();
        contain = ContainsNestedJoin(j.GetTable());
      }
    }
    return contain;
  }

  internal protected static bool IsJoinsAssociative(SqlTable left) {
    bool isAssociative = false;
    if (left.IsNestedJoinTable()) {
      isAssociative = IsJoinsAssociative(left.GetNestedJoin());
    } else {
      isAssociative = true;
    }

    if (isAssociative) {
      if (left.HasJoin()) {
        SqlJoin j = left.GetJoin();
        SqlTable lrm = left;
        if (left.IsNestedJoinTable()) {
          lrm = GetRightMostTable(left.GetNestedJoin());
        }
        SqlTable rlm = GetLeftMostTable(j.GetTable());
        bool isOnAdjacent = IsOnAdjacent(j.GetCondition(), lrm, rlm) || null == j.GetCondition();
        bool NotRightJoin = j.GetJoinType() != JoinType.RIGHT;
        if (isOnAdjacent) {
          isAssociative = true;
        } else {
          isAssociative = false;
        }
        if (isAssociative) {
          isAssociative = IsJoinsAssociative(rlm);
        }
      } else {
        isAssociative = true;
      }
    }
    return isAssociative;
  }

  internal protected static bool IsOnAdjacent(SqlConditionNode on, SqlTable lm, SqlTable rm) {
    bool isAdjacent = false;
    if (on is SqlCriteria) {
      if (IsStandardOnCriteria(on)) {
        SqlGeneralColumn lc = (SqlGeneralColumn) ((SqlCriteria)on).GetLeft();
        SqlGeneralColumn rc = (SqlGeneralColumn) ((SqlCriteria)on).GetRight();
        bool sourceNameMatch1 = Utilities.EqualIgnoreCase(lm.GetValidName(), lc.GetTableName()) && Utilities.EqualIgnoreCase(rm.GetValidName(), rc.GetTableName());
        bool sourceNameMatch2 = Utilities.EqualIgnoreCase(lm.GetValidName(), rc.GetTableName()) && Utilities.EqualIgnoreCase(rm.GetValidName(), lc.GetTableName());
        bool aliasNameMatch1 = Utilities.EqualIgnoreCase(lm.GetAlias(), lc.GetTable().GetAlias()) && Utilities.EqualIgnoreCase(rm.GetAlias(), rc.GetTable().GetAlias());
        bool aliasNameMatch2 = Utilities.EqualIgnoreCase(lm.GetAlias(), rc.GetTable().GetAlias()) && Utilities.EqualIgnoreCase(rm.GetAlias(), lc.GetTable().GetAlias());
        if (sourceNameMatch1 || sourceNameMatch2 || aliasNameMatch1 || aliasNameMatch2) {
          isAdjacent = true;
        }
      }
    }
    return isAdjacent;
  }

  internal protected static bool IsStandardCriteriaInJoin(SqlJoin topJoin) {
    bool isStandard = true;
    if (topJoin != null) {
      isStandard = IsStandardOnCriteria(topJoin.GetCondition());
      if (isStandard) {
        isStandard = IsStandardCriteriaInJoin(topJoin.GetTable().GetJoin());
      }
    }
    return isStandard;
  }

  internal protected static bool IsStandardOnCriteria(SqlConditionNode condition) {
    bool isStandard = true;
    if (condition is SqlCriteria) {
      SqlCriteria c = (SqlCriteria)condition;
      SqlExpression l = c.GetLeft();
      SqlExpression r = c.GetRight();
      if (!(l is SqlGeneralColumn && r is SqlGeneralColumn)) {
        isStandard = false;
      }
    } else if (condition is SqlCondition) {
      SqlCondition c = (SqlCondition)condition;
      if (SqlLogicalOperator.Or == c.GetLogicOp()) {
        isStandard = true;
      } else {
        isStandard = IsStandardOnCriteria((SqlConditionNode)c.GetLeft());
        if (isStandard) {
          isStandard = IsStandardOnCriteria((SqlConditionNode)c.GetRight());
        }
      }
    }
    return isStandard;
  }

  internal protected static bool IsEquiJoinCriteria(SqlConditionNode where, SqlCollection<SqlTable> tables) {
    int tableIndex = 0;
    bool isEquiJoin = true;
    for( ; (tableIndex + 1)< tables.Size(); ) {
      SqlTable l = tables.Get(tableIndex);
      SqlTable r = tables.Get(++tableIndex);
      isEquiJoin = IsEquiJoinCriteria(where, l, r);
      if(!isEquiJoin) {
        break;
      }
    }

    return isEquiJoin;
  }

  internal protected static bool IsEquiJoinCriteria(SqlConditionNode where, SqlTable left, SqlTable right) {
    bool isEquiJoin = false;
    if (where is SqlCondition) {
      SqlCondition andCond = (SqlCondition) where;
      if (andCond.GetLogicOp() == SqlLogicalOperator.And) {
        SqlExpression l = andCond.GetLeft();
        SqlExpression r = andCond.GetRight();
        if (l is SqlConditionNode && IsStandardOnCriteria((SqlConditionNode) l)) {
          isEquiJoin = IsEquiJoinCriteria((SqlConditionNode) l, left, right);
        }
        if (!isEquiJoin && r is SqlConditionNode && IsStandardOnCriteria((SqlConditionNode) r)) {
          isEquiJoin = IsEquiJoinCriteria((SqlConditionNode) r, left, right);
        }
      }
    } else if (where is SqlCriteria && IsStandardOnCriteria(where)) {
      if (SqlUtilities.IsSimpleTable(left) && SqlUtilities.IsSimpleTable(right)) {
        isEquiJoin = IsEquiJoinCriteria((SqlCriteria)where,
                left.GetAlias(),
                right.GetAlias());
      } else if (SqlUtilities.IsSimpleTable(left)) {
        if (right.IsNestedQueryTable()) {
          isEquiJoin =  true;
        }
      } else if (SqlUtilities.IsSimpleTable(right)) {
        if (left.IsNestedQueryTable()) {
          isEquiJoin = true;
        }
      } else {
        if (left.IsNestedQueryTable() && right.IsNestedQueryTable()) {
          isEquiJoin = true;
        }
      }
    }
    return isEquiJoin;
  }

  internal protected static bool IsEquiJoinCriteria(SqlCriteria criteria, string t1, string t2) {
    bool isEquiJoin = false;
    SqlColumn lc = (SqlColumn)criteria.GetLeft();
    SqlColumn rc = (SqlColumn)criteria.GetRight();
    if (Utilities.EqualIgnoreCaseInvariant(t1, lc.GetTableName())
            && Utilities.EqualIgnoreCaseInvariant(t2, rc.GetTableName())) {
      isEquiJoin = true;
    } else if (Utilities.EqualIgnoreCaseInvariant(t1, rc.GetTableName())
            && Utilities.EqualIgnoreCaseInvariant(t2, lc.GetTableName())) {
      isEquiJoin = true;
    }
    return isEquiJoin;
  }

  private static SqlTable GetRightMostTable(SqlTable t) {
    SqlTable rm = t;
    if (t.HasJoin()) {
      SqlJoin join = t.GetJoin();
      SqlTable right = join.GetTable();
      rm = GetRightMostTable(right);
    }
    return rm;
  }

  private static SqlTable GetLeftMostTable(SqlTable t) {
    SqlTable lm = t;
    if (t.IsNestedJoinTable()) {
      SqlTable nestedJoin = t.GetNestedJoin();
      lm = GetLeftMostTable(nestedJoin);
    }
    return lm;
  }

}

interface IColumnCompare {
   bool Compare(SqlColumn c1, SqlColumn c2);
}

interface ITableCompare {
   bool Compare(SqlTable t1, SqlTable t2);
}

interface IFunctionSubstitute {
   SqlExpression Substitute(SqlColumn f) ;
   bool Match(SqlColumn f);
}

sealed class SourceColumnComparer : IColumnCompare {
   public  bool Compare(SqlColumn c1, SqlColumn c2) {
    SimpleTableComparer tc = new SimpleTableComparer();
    if (!tc.Compare(c1.GetTable(), c2.GetTable())) return false;

    bool r1 = Utilities.EqualIgnoreCase(c1.GetColumnName(), c2.GetAlias());
    bool r2 = Utilities.EqualIgnoreCase(c2.GetColumnName(), c1.GetAlias());
    return r1 || r2;
  }
}

sealed class SimpleTableComparer : ITableCompare {
   public bool Compare(SqlTable t1, SqlTable t2) {
    if (t1 == null || t2 == null) {
      return true;
    }
    bool c1 = Utilities.EqualIgnoreCase(t1.GetName(), t2.GetAlias());
    bool c2 = Utilities.EqualIgnoreCase(t2.GetName(), t1.GetAlias());
    return c1 || c2;
  }
}

sealed class NULLIFSubstitue : IFunctionSubstitute {
  private const string Name = "NULLIF";
   public SqlExpression Substitute(SqlColumn f) {
    if (!Utilities.EqualIgnoreCase(Name, f.GetColumnName())) {
      return f;
    }
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    SqlCollection<SqlExpression> parameters = new SqlCollection<SqlExpression>();
    parameters.Add(null);
    SqlCriteria c = new SqlCriteria(func.GetParameters().Get(0), ComparisonType.NOT_EQUAL, func.GetParameters().Get(1));
    parameters.Add(c);
    parameters.Add(func.GetParameters().Get(0));
    parameters.Add(new SqlValueExpression(SqlValue.GetNullValueInstance()));
    if (f.HasAlias()) {
      return new SqlFormulaColumn("CASE", func.GetAlias(), parameters);
    } else {
      return new SqlFormulaColumn("CASE", parameters);
    }
  }

   public bool Match(SqlColumn f) {
    if (!(f is SqlFormulaColumn)) return false;
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    return Utilities.EqualIgnoreCase(Name, func.GetColumnName()) && 2 == func.GetParameters().Size();
  }
}

sealed class IIFSubstitue : IFunctionSubstitute {
  private const string Name = "IIF";
   public  SqlExpression Substitute(SqlColumn f) {
    //https://docs.microsoft.com/en-us/sql/t-sql/functions/logical-functions-iif-transact-sql?view=sql-server-2017
    if (!Utilities.EqualIgnoreCase(Name, f.GetColumnName())) {
      return f;
    }

    SqlFormulaColumn func = (SqlFormulaColumn) f;
    SqlCollection<SqlExpression> parameters = new SqlCollection<SqlExpression>();
    parameters.Add(null);
    parameters.Add(func.GetParameters().Get(0));
    parameters.Add(func.GetParameters().Get(1));
    parameters.Add(func.GetParameters().Get(2));
    if (f.HasAlias()) {
      return new SqlFormulaColumn("CASE", func.GetAlias(), parameters);
    } else {
      return new SqlFormulaColumn("CASE", parameters);
    }
  }

   public  bool Match(SqlColumn f) {
    if (!(f is SqlFormulaColumn)) return false;
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    return Utilities.EqualIgnoreCase(Name, func.GetColumnName()) && 3 == func.GetParameters().Size();
  }
}

sealed class COALESCESubstitue : IFunctionSubstitute {
  private const string Name = "COALESCE";
   public SqlExpression Substitute(SqlColumn f) {
    if (!Utilities.EqualIgnoreCase(Name, f.GetColumnName())) {
      return f;
    }
    SqlCollection<SqlExpression> parameters = new SqlCollection<SqlExpression>();
    parameters.Add(null);

    SqlFormulaColumn func = (SqlFormulaColumn) f;
    for (int i = 0 ; i < func.GetParameters().Size() - 1; ++i) {
      SqlExpression p = func.GetParameters().Get(i);
      SqlCriteria c = new SqlCriteria(p, ComparisonType.IS_NOT, new SqlValueExpression(SqlValue.GetNullValueInstance()));
      parameters.Add(c);
      parameters.Add(p);
    }

    parameters.Add(func.GetParameters().Get(func.GetParameters().Size() - 1));

    if (func.HasAlias()) {
      return new SqlFormulaColumn("CASE", func.GetAlias(), parameters);
    } else {
      return new SqlFormulaColumn("CASE", parameters);
    }
  }

   public bool Match(SqlColumn f) {
    if (!(f is SqlFormulaColumn)) return false;
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    return Utilities.EqualIgnoreCase(Name, func.GetColumnName()) && func.GetParameters().Size() > 1;
  }
}

sealed class ATATVariableSubstitue : IFunctionSubstitute {

   public  SqlExpression Substitute(SqlColumn f) {
    SqlValueExpression p = (SqlValueExpression) f.GetExpr();
    SqlCollection<SqlExpression> paras =new SqlCollection<SqlExpression>();
    paras.Add(new SqlValueExpression(SqlValueType.STRING, p.GetParameterName()));
    if (f.HasAlias()) {
      return new SqlFormulaColumn("SYSTEM_VARIABLE", f.GetAlias(), paras);
    } else {
      return new SqlFormulaColumn("SYSTEM_VARIABLE", paras);
    }
  }

   public  bool Match(SqlColumn f) {
    if (!(f is SqlConstantColumn)) return false;
    SqlConstantColumn cc = (SqlConstantColumn) f;
    if (!ParserCore.IsParameterExpression(cc.GetExpr())) {
      return false;
    }

    SqlValueExpression p = (SqlValueExpression) cc.GetExpr();

    string pName = p.GetParameterName();
    if (pName != null && pName.StartsWith("@@")) {
      return true;
    }

    return false;
  }
}
}

