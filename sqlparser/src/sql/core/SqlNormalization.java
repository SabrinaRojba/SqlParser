package core;
/*#
using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Collections.Generic;
#*/
import cdata.sql.*;
import rssbus.RSBException;
import rssbus.oputils.common.Utilities;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

public final class SqlNormalization {
  private static final String CRITERIA_SUBQUERY_EXCEPTION_CODE = "normalizeCriteriaSubQuery";
  private static final String TABLE_EXPRESSION_EXCEPTION_CODE = "normalizeTableExprWithQuery";
  private static final String NESTED_QUERY_TABLE_NAME_EXCEPTION_CODE = "normalizeNestedQueryTableName";
  private static final String IMPLICIT_JOIN_EXCEPTION_CODE = "normalizeImplicitJoin";
  private static final String NESTED_JOIN_EXCEPTION_CODE = "normalizeNestedJoin";
  private static final String NORMALIZATION_EXCEPTION_CODE = "normalize";
  private SqlNormalizationException _throwEx;

  public SqlStatement normalizeStatement(NormalizationOption option, SqlStatement stmt, IDataMetadata dataMetadata) {
    ArrayList<Exception> exceptions = new ArrayList<Exception>();
    SqlStatement normalized = normalize(option, stmt, dataMetadata, exceptions);
    if (exceptions.size() > 0) {
      this._throwEx = new SqlNormalizationException(NORMALIZATION_EXCEPTION_CODE, exceptions);
    }
    return normalized;
  }

  public Exception getException() {
    return this._throwEx;
  }

  public static SqlStatement normalize(NormalizationOption option, SqlStatement stmt, IDataMetadata dataMetaData) throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<Exception>();
    SqlStatement normalized = normalize(option, stmt, dataMetaData, exceptions);
    if (exceptions.size() > 0) {
      SqlNormalizationException throwEx = new SqlNormalizationException(NORMALIZATION_EXCEPTION_CODE, exceptions);
      throw throwEx;
    }

    return normalized;
  }

  private static SqlStatement normalize(NormalizationOption option, SqlStatement stmt, IDataMetadata dataMetaData, List<Exception> exceptions) {
    SqlStatement simplified = stmt;
    ArrayList<NormalizationOptions> configs = buildConfigedOptions(option);
    boolean support = stmt instanceof SqlQueryStatement;
    if (support) {
      for (int i = 0 ; i < configs.size(); ++i) {
        try {
          simplified = normalize(configs.get(i), simplified, dataMetaData);
        } catch (Exception ex) {
          exceptions.add(ex);
        }
      }
    }
    return simplified;
  }

  private static SqlStatement normalize(NormalizationOptions options, SqlStatement stmt, IDataMetadata dataMetaData) throws Exception {
    SqlStatement simplified = stmt;
    if (!isStandardQuery(simplified)) {
      if (NormalizationOptions.FunctionSubstitute == options) {
        simplified = normalizedFunctionSubstitute((SqlQueryStatement) simplified);
      } else if (NormalizationOptions.OperationExpression == options) {
        simplified = normalizeOperationExpression((SqlQueryStatement)simplified);
      }
      return simplified;
    }
    if (NormalizationOptions.ImplicitCrossJoin == options) {
      simplified = normalizeImplicitJoin((SqlQueryStatement) simplified, JoinType.CROSS);
    } else if (NormalizationOptions.ImplicitNaturalJoin == options) {
      simplified = normalizeImplicitJoin((SqlQueryStatement)simplified, JoinType.NATURAL);
    } else if (NormalizationOptions.ImplicitCommaJoin == options) {
      simplified = normalizeImplicitJoin((SqlQueryStatement)simplified, JoinType.COMMA);
    } else if (NormalizationOptions.ImplicitInnerJoin == options) {
      simplified = normalizeImplicitJoin((SqlQueryStatement)simplified, JoinType.INNER);
    } else if (NormalizationOptions.EquiInnerJoin == options) {
      simplified = normalizeEquiInnerJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.TableExprWithQuery == options && containsNestedQuery(stmt.getTable())) {
      simplified = normalizeTableExprWithQuery((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.CriteriaInJoin == options) {
      simplified = normalizeCriteriaInJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.ResolveTableAlias == options) {
      simplified = normalizeTableAlias((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.CriteriaWithSubQuery == options) {
      simplified = normalizeCriteriaWithSubQuery((SqlQueryStatement)simplified, JoinType.INNER);
    } else if (NormalizationOptions.CriteriaWithNot == options) {
      simplified = normalizeCriteriaWithNot((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.Criteria == options) {
      simplified = normalizeCriteria((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.FixUniqueAlias == options) {
      simplified = normalizeFixUniqueAlias((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.OperationExpression == options) {
      simplified = normalizeOperationExpression((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.AppendNestedQueryTableName == options) {
      simplified = normalizeNestedQueryTableName((SqlQueryStatement)simplified, dataMetaData);
    } else if (NormalizationOptions.RightJoin == options) {
      simplified = normalizeRightJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.MinimizeCriteria == options) {
      simplified = normalizeMinimizeCriteria((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.ConstantColumn == options) {
      simplified = normalizeConstantColumn((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.FormulaAlias == options) {
      simplified = normalizeUnnamedColumn((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.NestedJoin == options) {
      simplified = normalizeNestedJoin((SqlQueryStatement)simplified);
    } else if (NormalizationOptions.Distinct == options) {
      simplified = normalizeDistinct((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.Count_Distinct == options) {
      simplified = normalizeCountDistinct((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.PredictTrue == options) {
      simplified = normalizePredictTrue((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.SemiAntiJoin == options) {
      simplified = normalizeCriteriaWithSubQuery((SqlQueryStatement)simplified, JoinType.LEFT_SEMI);
    } else if (NormalizationOptions.FunctionSubstitute == options) {
      simplified = normalizedFunctionSubstitute((SqlQueryStatement) simplified);
    } else if (NormalizationOptions.RemoveDistinctIfColumnUnique == options) {
      simplified = normalizeRemoveDistinctIfColumnUnique((SqlQueryStatement) simplified, dataMetaData);
    }
    return simplified;
  }

  private static SqlQueryStatement normalizedFunctionSubstitute(SqlQueryStatement statement) throws Exception {
    if (statement instanceof SqlSelectUnionStatement) {
      SqlSelectUnionStatement union = (SqlSelectUnionStatement) statement;
      SqlQueryStatement left = union.getLeft();
      SqlQueryStatement right = union.getRight();
      normalizedFunctionSubstitute(left);
      normalizedFunctionSubstitute(right);
      return statement;
    }
    FunctionModifier funcModifier = new FunctionModifier();
    SqlParser parser = new SqlParser(statement);
    funcModifier.modify(parser);
    SqlSelectStatement select = (SqlSelectStatement) statement;
    return select;
  }

  private static SqlQueryStatement normalizeRemoveDistinctIfColumnUnique(SqlQueryStatement statement, IDataMetadata dataMetadata) throws Exception {
    RemoveDistinctVisitor removeDistinctVisitor = new RemoveDistinctVisitor(dataMetadata);
    statement.accept(removeDistinctVisitor);
    return statement;
  }

  private static SqlStatement normalizeUnnamedColumn(SqlQueryStatement statement) throws Exception {
    if (statement instanceof SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) statement;
      SqlCollection<SqlColumn> columns = select.getColumns();
      fixUnnamedColumnAlias(columns);
    } else if (statement instanceof SqlSelectUnionStatement){
      SqlSelectUnionStatement unionStatement = (SqlSelectUnionStatement) statement;
      SqlQueryStatement left = unionStatement.getLeft();
      normalizeUnnamedColumn(left);
      SqlQueryStatement right = unionStatement.getRight();
      normalizeUnnamedColumn(right);
    }
    return statement;
  }

  public static void fixUnnamedColumnAlias(SqlCollection<SqlColumn> columns) {
    int incrementNum = 0;

    for (int i = 0 ; i < columns.size(); ++i) {
      SqlColumn col = columns.get(i);
      if (col instanceof SqlConstantColumn) {
        ATATVariableSubstitue ATAT = new ATATVariableSubstitue();
        String alias = null;
        if (null == col.getAlias()) {
          if (ATAT.match(col)) {
            SqlValueExpression p = (SqlValueExpression) col.getExpr();
            String pn = p.getParameterName();
            alias = pn.substring(2);
          } else {
            alias = "Column" + (incrementNum++);
          }
          SqlConstantColumn c = (SqlConstantColumn)col;
          col = new SqlConstantColumn(alias, (SqlValueExpression)c.getExpr());
          columns.set(i, col);
        }
      } else if (col instanceof SqlOperationColumn) {
        if (null == col.getAlias()) {
          String alias = "Column" + (incrementNum++);
          SqlOperationColumn c = (SqlOperationColumn)col;
          col = new SqlOperationColumn(alias, (SqlOperationExpression)c.getExpr());
          columns.set(i, col);
        }
      } else if (col instanceof SqlSubQueryColumn) {
        if (null == col.getAlias()) {
          String alias = "Column" + (incrementNum++);
          SqlSubQueryColumn c = (SqlSubQueryColumn)col;
          col = new SqlSubQueryColumn(alias, (SqlSubQueryExpression)c.getExpr());
          columns.set(i, col);
        }
      } else {
        continue;
      }
    }
  }

  public static void normalizeFormulaAlias(SqlCollection<SqlColumn> columns) throws Exception {
    ArrayList<SqlFormulaColumn> functions = new ArrayList<SqlFormulaColumn>();
    for (int i = 0 ; i < columns.size(); ++i) {
      SqlColumn column = columns.get(i);
      if (column instanceof SqlFormulaColumn) {
        functions.add((SqlFormulaColumn) column);
      }
    }

    for (int i = 0 ; i < columns.size(); ++i) {
      SqlColumn column = columns.get(i);
      if (column instanceof SqlFormulaColumn) {
        SqlFormulaColumn formulaColumn = (SqlFormulaColumn) column;
        if (!formulaColumn.hasAlias()) {
          int matched = 0;
          for (int j = 0 ; j < functions.size(); ++j) {
            SqlFormulaColumn f = functions.get(j);
            if (Utilities.equalIgnoreCase(f.getColumnName(), formulaColumn.getColumnName())) {
              ++matched;
            }
          }
          if (1 == matched) {
            SqlFormulaColumn tempFormula = new SqlFormulaColumn(formulaColumn.getColumnName(),
                    formulaColumn.getColumnName(),
                    formulaColumn.getParameters(),
                    formulaColumn.getOverClause());
            columns.set(i, tempFormula);
          }
        }
      } else {
        continue;
      }
    }
  }

  private static void fixUniqueColumnAlias(SqlCollection<SqlColumn> columns, SqlTable mainTable) throws Exception {
    if (columns == null) return;
    for (int i = columns.size() - 1 ; i >= 0; --i) {
      SqlColumn col = columns.get(i);
      if ((col instanceof SqlGeneralColumn && !(col instanceof SqlWildcardColumn)) || col instanceof SqlFormulaColumn) {
        String alias = col.getAlias();
        int match = 0;
        if (alias != null) {
          for (int j = 0 ; j < columns.size(); ++j) {
            SqlColumn c = columns.get(j);
            if (j == i) continue;
            if (c.getAlias() != null) {
              boolean matched = false;
              if (alias.equals(c.getAlias())) {
                if (c.getTable() != null && col.getTable() != null) {
                  String t1 = c.getTable().getAlias();
                  String t2 = col.getTable().getAlias();
                  if (t1.equalsIgnoreCase(t2)) {
                    matched = true;
                  }
                } else if (c.getTable() != null) {
                  String t1 = c.getTable().getAlias();
                  String t2 = mainTable.getAlias();
                  if (t1.equalsIgnoreCase(t2)) {
                    matched = true;
                  }
                } else if (col.getTable() != null) {
                  String t1 = col.getTable().getAlias();
                  String t2 = mainTable.getAlias();
                  if (t1.equalsIgnoreCase(t2)) {
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
          String aliasName = alias + match;
          if (col instanceof SqlGeneralColumn) {
            col = new SqlGeneralColumn(col.getTable(), col.getColumnName(), aliasName, col.getValueExpr());
          } else if (col instanceof SqlFormulaColumn) {
            col = new SqlFormulaColumn(col.getColumnName(),
                    aliasName,
                    ((SqlFormulaColumn)col).getParameters(),
                    ((SqlFormulaColumn) col).getOverClause());
          }
          columns.set(i, col);
        }
      } else {
        continue;
      }
    }
  }

  public static SqlCollection<SqlColumn> getPrimitiveColumns(SqlCollection<SqlColumn> columns) {
    SqlCollection<SqlColumn> primitiveColumns = new SqlCollection<SqlColumn>();
    for (SqlColumn c : columns) {
      if (c instanceof SqlGeneralColumn) {
        primitiveColumns.add(c);
      }
    }
    return primitiveColumns;
  }

  public static SqlColumn resolveColumnAlias(SqlColumn c, SqlCollection<SqlColumn> primitiveColumns) throws Exception {
    SqlColumn resolved = c;
    if (c instanceof SqlFormulaColumn) {
      SqlFormulaColumn fcol = (SqlFormulaColumn)c;
      SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
      boolean modified = false;
      for (SqlExpression para : fcol.getParameters()) {
        if (para instanceof SqlColumn) {
          SqlColumn resolvedPara = resolveColumnAlias((SqlColumn) para, primitiveColumns);
          paras.add(resolvedPara);
          if (resolvedPara != para) {
            modified = true;
          }
        } else if (para instanceof SqlConditionNode) {
          SqlConditionNode resolvedC = resolveColumnAliasInCriteria((SqlConditionNode) para, primitiveColumns);
          paras.add(resolvedC);
          if (resolvedC != para) {
            modified = true;
          }
        } else {
          paras.add(para);
        }
      }
      if (modified) {
        resolved = new SqlFormulaColumn(fcol.getColumnName(),
                fcol.getAlias(),
                paras,
                fcol.getOverClause());
      } else {
        return fcol;
      }
    } else if (c instanceof SqlOperationColumn) {
      SqlOperationColumn opcol = (SqlOperationColumn)c;
      SqlOperationExpression opExpr = (SqlOperationExpression)opcol.getExpr();
      SqlExpression left = opExpr.getLeft();
      SqlExpression right = opExpr.getRight();
      if (left instanceof SqlColumn) {
        left = resolveColumnAlias((SqlColumn)left, primitiveColumns);
      }
      if (right instanceof SqlColumn) {
        right = resolveColumnAlias((SqlColumn)right, primitiveColumns);
      }
      resolved = new SqlOperationColumn(opcol.getAlias(), new SqlOperationExpression(opExpr.getOperator(), left, right));
    } else if (c instanceof SqlWildcardColumn) {
      //do nothing.
    } else if (c instanceof SqlGeneralColumn) {
      SqlGeneralColumn column = (SqlGeneralColumn)c;
      if (column.hasAlias()) {
        resolved = new SqlGeneralColumn(column.getTable(), column.getColumnName());
      } else {
        SourceColumnComparer cc = new SourceColumnComparer();
        SqlColumn matched = null;
        for (SqlColumn sc : primitiveColumns) {
          if (cc.compare(column, sc)) {
            matched = sc;
            break;
          }
        }

        if (matched instanceof SqlGeneralColumn) {
          if (column.getTable() != null) {
            resolved = new SqlGeneralColumn(column.getTable(), matched.getColumnName());
          } else {
            resolved = new SqlGeneralColumn(matched.getTable(), matched.getColumnName());
          }
        } else if (matched instanceof SqlFormulaColumn && matched.hasAlias()) {
          resolved = new SqlFormulaColumn(matched.getColumnName(),
              ((SqlFormulaColumn) matched).getParameters(),
              ((SqlFormulaColumn) matched).getOverClause());
        } else if (matched != null) {
          resolved = matched;
        }
      }
    } else if (c instanceof SqlConstantColumn) {
      //do nothing.
    } else if (c instanceof SqlSubQueryColumn) {
      //do nothing.
    }
    return resolved;
  }

  private static SqlQueryStatement normalizePredictTrue(SqlQueryStatement query) throws Exception {
    SqlQueryStatement resolved = query;
    SqlConditionNode where = query.getCriteria();
    if (where == null) return resolved;
    PredictTrueModifier modifier = new PredictTrueModifier();
    modifier.modify(where);
    resolved.setCriteria((SqlConditionNode) modifier.getModifiedExpr());
    return resolved;
  }

  private static SqlQueryStatement normalizeDistinct(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) query;

      if (select.isDistinct() && containsAggragation(select.getColumns())) {
        SqlCollection<SqlColumn> outerColumns = new SqlCollection<SqlColumn>();
        SqlCollection<SqlColumn> innerColumns = new SqlCollection<SqlColumn>();
        for (SqlColumn c : select.getColumns()) {
          if (c instanceof SqlFormulaColumn) {
            innerColumns.add(c);
            outerColumns.add(new SqlGeneralColumn(c.getAlias()));
            continue;
          }
          innerColumns.add(c);
          outerColumns.add(c);
        }
        SqlCollection<SqlExpression> outerGroupBy = new SqlCollection<SqlExpression>();
        for (SqlColumn c : outerColumns) {
          outerGroupBy.add(c);
        }
        SqlSelectStatement nestedQuery = new SqlSelectStatement(innerColumns,
                select.getHavingClause(),
                select.getCriteria(),
                select.getOrderBy(),
                select.getGroupBy(),
                select.getEachGroupBy(),
                select.getTable(),
                select.getParameterList(),
                select.getFromLast(),
                select.getLimitExpr(),
                select.getOffsetExpr(),
                false,
                select.getDialectProcessor());
        SqlTable nestTable = new SqlTable(nestedQuery, select.getTable().getAlias());

        select.setColumns(outerColumns);
        select.setGroupByClause(outerGroupBy, select.getEachGroupBy());
        select.setTable(nestTable);
        select.setDistinct(false);
        select.setCriteria(null);
        return select;
      }

      if (select.isDistinct()) {
        SqlCollection<SqlExpression> groupBy = select.getGroupBy();
        for (SqlColumn c : select.getColumns()) {
          if (c.hasAlias()) {
            groupBy.add(new SqlGeneralColumn(c.getAlias()));
          } else {
            groupBy.add(c);
          }
        }
        select.setDistinct(false);
        select.setGroupByClause(groupBy, select.getEachGroupBy());
      }

      SqlTable t = select.getTable();

      do {
        if (t.isNestedQueryTable()) {
          SqlQueryStatement nq = t.getQuery();
          normalizeDistinct(nq);
        } else if (t.isNestedJoinTable()) {
          SqlTable nt = t.getNestedJoin();
          SqlSelectStatement temp = new SqlSelectStatement(null);
          temp.setTable(nt);
          SqlCollection<SqlColumn> tempC = new SqlCollection<SqlColumn>();
          tempC.add(new SqlWildcardColumn());
          temp.setColumns(tempC);
          normalizeDistinct(temp);
        }

        if (t.hasJoin()) {
          SqlJoin j = t.getJoin();
          t = j.getTable();
        } else {
          break;
        }
      } while(true);

      return select;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeCountDistinct(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement) query;

      if (containsAggragation(select.getColumns())) {
        for (int i = 0 ; i < select.getColumns().size(); ++i) {
          SqlColumn c = select.getColumns().get(i);
          if (!(c instanceof SqlFormulaColumn)) {
            continue;
          }

          if (!Utilities.equalIgnoreCase("COUNT", c.getColumnName())) {
            continue;
          }

          SqlFormulaColumn COUNT_FUNC = (SqlFormulaColumn) c;
          SqlExpression P1 = COUNT_FUNC.getParameters().get(0);

          if (!(P1 instanceof SqlFormulaColumn)) {
            continue;
          }

          SqlFormulaColumn DISTINCT_FUNC = (SqlFormulaColumn) P1;
          if (Utilities.equalIgnoreCase("DISTINCT", DISTINCT_FUNC.getColumnName())) {
            SqlFormulaColumn COUNT_DISTINCT_FUNC = new SqlFormulaColumn("COUNT_DISTINCT", COUNT_FUNC.getAlias(), DISTINCT_FUNC.getParameters());
            select.getColumns().set(i, COUNT_DISTINCT_FUNC);
          }
        }
      }

      SqlTable t = select.getTable();

      do {
        if (t.isNestedQueryTable()) {
          SqlQueryStatement nq = t.getQuery();
          normalizeDistinct(nq);
        } else if (t.isNestedJoinTable()) {
          SqlTable nt = t.getNestedJoin();
          SqlSelectStatement temp = new SqlSelectStatement(null);
          temp.setTable(nt);
          SqlCollection<SqlColumn> tempC = new SqlCollection<SqlColumn>();
          tempC.add(new SqlWildcardColumn());
          temp.setColumns(tempC);
          normalizeCountDistinct(temp);
        }

        if (t.hasJoin()) {
          SqlJoin j = t.getJoin();
          t = j.getTable();
        } else {
          break;
        }
      } while(true);

      return select;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeFixUniqueAlias(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();
      fixUniqueColumnAlias(columns, stmt.getTable());
      stmt.setColumns(columns);
      return stmt;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeOperationExpression(SqlQueryStatement query) throws Exception {
    OperationColumnModifier modifier = new OperationColumnModifier();
    SqlParser parser = new SqlParser(query);
    modifier.modify(parser);
    return parser.getSelect();
  }

  private static SqlTable concatJoinTable(SqlTable l, SqlTable r, JoinType type, SqlConditionNode criteria) throws Exception {
    SqlTable newTop = l;
    if (l.getJoin() != null) {
      SqlJoin j = l.getJoin();
      SqlTable nr = concatJoinTable(j.getTable(), r, type, criteria);
      SqlJoin nj = new SqlJoin(j.getJoinType(), nr, j.getCondition(), j.isEach(), j.hasOuter());
      newTop = new SqlTable(newTop.getCatalog(),
              newTop.getSchema(),
              newTop.getName(),
              newTop.getAlias(),
              nj,
              newTop.getNestedJoin(),
              newTop.getQuery(),
              newTop.getTableValueFunction(),
              newTop.getCrossApply());

    } else {
      SqlJoin j = new SqlJoin(type, r, criteria);
      newTop = new SqlTable(newTop.getCatalog(),
              newTop.getSchema(),
              newTop.getName(),
              newTop.getAlias(),
              j,
              newTop.getNestedJoin(),
              newTop.getQuery(),
              newTop.getTableValueFunction(),
              newTop.getCrossApply());
    }
    return newTop;
  }

  private static SqlCriteria[] resolveEquiJoinCriteria(SqlConditionNode[] inOutWherem, SqlCollection<SqlTable> tables) {
    SqlCriteria[] criterias = new SqlCriteria[tables.size() - 1];
    for(int index = 0; (index + 1) < tables.size(); ) {
      criterias[index] = resolveEquiJoinCriteria(inOutWherem, tables.get(index), tables.get(++index));
    }
    return criterias;
  }

  private static SqlCriteria resolveEquiJoinCriteria(SqlConditionNode [] inOutWhere, SqlTable left, SqlTable right) {
    SqlCriteria equiOn = null;
    boolean isEquiJoin = false;
    SqlConditionNode where = inOutWhere[0];
    if (where instanceof SqlCondition) {
      SqlCondition andCond = (SqlCondition) where;
      if (andCond.getLogicOp() == SqlLogicalOperator.And) {
        SqlExpression l = andCond.getLeft();
        SqlExpression r = andCond.getRight();
        if (l instanceof SqlConditionNode) {
          isEquiJoin = SqlNormalizationHelper.isEquiJoinCriteria((SqlConditionNode) l, left, right);
        }
        if (isEquiJoin) {
          inOutWhere[0] = (SqlConditionNode) l;
          equiOn = resolveEquiJoinCriteria(inOutWhere, left, right);
          if (inOutWhere[0] != null) {
            inOutWhere[0] = new SqlCondition(inOutWhere[0], SqlLogicalOperator.And, r);
          } else {
            inOutWhere[0] = (SqlConditionNode) r;
          }

          return equiOn;
        }

        if (r instanceof SqlConditionNode) {
          isEquiJoin = SqlNormalizationHelper.isEquiJoinCriteria((SqlConditionNode) r, left, right);
        }
        if (isEquiJoin) {
          inOutWhere[0] = (SqlConditionNode) r;
          equiOn = resolveEquiJoinCriteria(inOutWhere, left, right);
          if (inOutWhere[0] != null) {
            inOutWhere[0] = new SqlCondition(l, SqlLogicalOperator.And, inOutWhere[0]);
          } else {
            inOutWhere[0] = (SqlConditionNode) l;
          }
          return equiOn;
        }
        inOutWhere[0] = where;
      }
    } else if (where instanceof SqlCriteria) {
      isEquiJoin = SqlNormalizationHelper.isEquiJoinCriteria((SqlCriteria)where, left, right);
      if (isEquiJoin) {
        equiOn = (SqlCriteria)where;
        inOutWhere[0] = null;
      }
    }
    return equiOn;
  }

  private static SqlQueryStatement normalizeCriteriaInJoin(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlTable mainTable = stmt.getTable();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();

      SqlCollection<SqlJoin> joins = stmt.getJoins();
      boolean onlyInnerJoin = true;
      if (joins != null) {
        for (SqlJoin join : joins) {
          if (JoinType.INNER != join.getJoinType()) {
            onlyInnerJoin = false;
            break;
          }
        }
      }

      if (!onlyInnerJoin) {
        return query;
      }

      if (SqlNormalizationHelper.isStandardCriteriaInJoin(mainTable.getJoin())) {
        return query;
      }

      SqlTable [] mainTableInOut = {mainTable};
      SqlConditionNode [] whereInOut = {condition};
      standardJoin(mainTableInOut, whereInOut);

      SqlTable aliasMainTable = new SqlTable(mainTable.getAlias());

      SqlCollection<SqlTable> OWNERS = new SqlCollection<SqlTable>();
      OWNERS.add(aliasMainTable);

      attachOwnerTableForColumns(OWNERS, columns);

      orderBy = attachOwnerTableForOrderBy(orderBy, OWNERS);

      groupBy = attachOwnerTableForGroupBy(groupBy, OWNERS);

      havingClause = attachTableForCriteria(OWNERS, havingClause);

      condition = attachTableForCriteria(OWNERS, whereInOut[0]);

      stmt.setColumns(columns);
      stmt.setHavingClause(havingClause);
      stmt.setCriteria(condition);
      stmt.setOrderBy(orderBy);
      stmt.setGroupByClause(groupBy, stmt.getEachGroupBy());
      stmt.setTable(mainTableInOut[0]);
      return stmt;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeRightJoin(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {

      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();
      SqlCollection<SqlTable> tables = stmt.getTables();
      boolean normalized = false;

      for (int i = 0 ; i < tables.size(); ++i) {
        SqlTable t = tables.get(i);
        if (!hasRightJoin(t)) {
          continue;
        }
        normalized = true;
        SqlTable wrapped = wrapToNestedJoin(t);
        SqlTable tt = swapRightJoin(wrapped);
        tables.set(i, tt);
      }

      if (normalized) {
        stmt.setTables(tables);
        return stmt;
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeNestedJoin(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();
      SqlCollection<SqlTable> tables = stmt.getTables();
      boolean normalized = false;

      for (int i = 0 ; i < tables.size(); ++i) {
        SqlTable t = tables.get(i);
        if (t.isNestedJoinTable()) {
          SqlTable unWrapped = unWrapNestedJoin(t);
          tables.set(i, unWrapped);
          normalized = true;
          t = unWrapped;
        }

        if (SqlNormalizationHelper.containsNestedJoin(t)) {
          boolean associative = SqlNormalizationHelper.isJoinsAssociative(t);
          if (associative) {
            SqlTable unWrapped = unWrapNestedJoin(t);
            tables.set(i, unWrapped);
            normalized = true;
          } else {
            throwNoSupportNormalizationException(NESTED_JOIN_EXCEPTION_CODE, query);
          }
        }
      }

      if (normalized) {
        stmt.setTables(tables);
        return stmt;
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private static SqlTable wrapToNestedJoin(SqlTable top) throws Exception {
    if (top == null || top.getJoin() == null) {
      return top;
    }

    SqlTable nestedTableTop;
    if (top.isNestedJoinTable()) {
      nestedTableTop = top.getNestedJoin();
    } else if (top.isNestedQueryTable()) {
      nestedTableTop = new SqlTable(top.getQuery(), top.getAlias());
    } else {
      nestedTableTop = new SqlTable(top.getCatalog(),
              top.getSchema(),
              top.getName(),
              top.getAlias());
    }

    SqlJoin join = top.getJoin();
    do {
      SqlTable right = join.getTable();

      SqlTable trimedJoinRight = new SqlTable(right.getCatalog(),
              right.getSchema(),
              right.getName(),
              right.getAlias(),
              null,
              right.getNestedJoin(),
              right.getQuery(),
              right.getTableValueFunction(),
              right.getCrossApply());

      if (nestedTableTop.hasJoin()) {
        nestedTableTop = new SqlTable(null, nestedTableTop);
      }

      nestedTableTop = concatJoinTable(nestedTableTop,
              trimedJoinRight,
              join.getJoinType(),
              join.getCondition());

      join  = right.getJoin();
    } while (join != null);

    return nestedTableTop;
  }

  private static SqlTable unWrapNestedJoin(SqlTable top) throws Exception {
    SqlTable newTop = top;

    if (top.isNestedJoinTable()) {
      SqlTable nestedJ = top.getNestedJoin();
      newTop = unWrapNestedJoin(nestedJ);
    } else {
      newTop = new SqlTable(top.getCatalog(),
              top.getSchema(),
              top.getName(),
              top.getAlias(),
              null,
              null,
              top.getQuery(),
              top.getTableValueFunction(),
              top.getCrossApply());
    }

    boolean associative = SqlNormalizationHelper.isJoinsAssociative(top);
    if (associative) {
      SqlCollection<SqlJoin> joins = new SqlCollection<SqlJoin>();
      if (top.hasJoin()) {
        joins.add(top.getJoin());
        ParserCore.flattenJoins(joins, top.getJoin().getTable());
      }

      for (int i = 0 ; i < joins.size(); ++i) {
        SqlJoin j = joins.get(i);
        SqlTable r = j.getTable();
        while (r.isNestedJoinTable()) {
          r = r.getNestedJoin();
        }
        SqlTable nr = new SqlTable(r.getCatalog(),
                r.getSchema(),
                r.getName(),
                r.getAlias(),
                null,
                null,
                r.getQuery(),
                r.getTableValueFunction(),
                r.getCrossApply());
        newTop = concatJoinTable(newTop,
                nr,
                j.getJoinType(),
                j.getCondition());
      }
    } else {
      SqlJoin join = top.getJoin();
      newTop = concatJoinTable(newTop,
              join.getTable(),
              join.getJoinType(),
              join.getCondition());
    }
    return newTop;
  }

  private static SqlTable swapRightJoin(SqlTable top) throws Exception {
    if (top == null || (!top.hasJoin() && !top.isNestedJoinTable())) {
      return top;
    }

    if (top.isNestedJoinTable()) {
      SqlTable n1 = top.getNestedJoin();
      SqlTable n2 = swapRightJoin(n1);
      if (n1 != n2) {
        top = new SqlTable(top.getCatalog(),
                top.getSchema(),
                top.getName(),
                top.getAlias(),
                top.getJoin(),
                n2,
                top.getQuery(),
                top.getTableValueFunction(),
                top.getCrossApply());
      }
    }

    if (top.getJoin() != null) {
      SqlJoin join = top.getJoin();
      if (JoinType.RIGHT == join.getJoinType()) {
        SqlTable right = join.getTable();
        if (right.isNestedJoinTable()) {
          right = swapRightJoin(right);
        }
        SqlTable left = right;
        if (top.isNestedJoinTable()) {
          SqlTable n = top.getNestedJoin();
          right = new SqlTable(null, n);
        } else if (top.isNestedQueryTable()){
          right = new SqlTable(top.getQuery(),
                  top.getName(),
                  top.getAlias());
        } else if (top.isFunctionValueTable()) {
          right = new SqlTable(top.getTableValueFunction());
        } else {
          right = new SqlTable(top.getCatalog(),
                  top.getSchema(),
                  top.getName(),
                  top.getAlias(),
                  null,
                  top.getNestedJoin(),
                  top.getQuery(),
                  top.getTableValueFunction(),
                  top.getCrossApply());
        }
        join = new SqlJoin(JoinType.LEFT,
                right,
                join.getCondition(),
                join.isEach(),
                join.hasOuter());

        top = new SqlTable(left.getCatalog(),
                left.getSchema(),
                left.getName(),
                left.getAlias(),
                join,
                left.getNestedJoin(),
                left.getQuery(),
                left.getTableValueFunction(),
                left.getCrossApply());
      }
    }
    SqlTable resolved = top;
    return resolved;
  }

  private static boolean hasRightJoin(SqlTable table) {
    boolean hasRightJoin = false;
    if (table != null && table.hasJoin()) {
      if (table.isNestedJoinTable()) {
        hasRightJoin = hasRightJoin(table.getNestedJoin());
      }
      if (!hasRightJoin && table.hasJoin()) {
        SqlJoin join = table.getJoin();
        if (JoinType.RIGHT == join.getJoinType()) {
          hasRightJoin = true;
        } else {
          hasRightJoin = hasRightJoin(join.getTable());
        }
      }
    }
    return hasRightJoin;
  }

  private static SqlCollection<SqlOrderSpec> attachOwnerTableForOrderBy(SqlCollection<SqlOrderSpec> orders, SqlCollection<SqlTable> owners) throws Exception {
    SqlCollection<SqlOrderSpec> resolved = new SqlCollection<SqlOrderSpec>();
    for (SqlOrderSpec order : orders) {
      SqlExpression expr = order.getExpr();
      if (expr instanceof SqlColumn) {
        SqlColumn c = (SqlColumn)order.getExpr();
        if (null == c.getTable()) {
          c = attachOwnerTableForColumn(owners, c);
          resolved.add(new SqlOrderSpec(c, order.getOrder(), order.isNullsFirst(), order.hasNulls()));
        } else {
          resolved.add(order);
        }
      } else {
        resolved.add(order);
      }
    }
    return resolved;
  }

  private static SqlCollection<SqlExpression> attachOwnerTableForGroupBy(SqlCollection<SqlExpression> groups, SqlCollection<SqlTable> owners) throws Exception {
    SqlCollection<SqlExpression> resolved = new SqlCollection<SqlExpression>();
    for (SqlExpression group : groups) {
      if (group instanceof SqlColumn) {
        SqlColumn column = (SqlColumn)group;
        if (null == column.getTable()) {
          resolved.add(attachOwnerTableForColumn(owners, column));
        } else {
          resolved.add(group);
        }
      } else {
        resolved.add(group);
      }
    }
    return resolved;
  }

  private static void standardJoin(SqlTable [] mainTableInOut, SqlConditionNode [] whereInOut) throws Exception {
    SqlTable mainIn = mainTableInOut[0];
    SqlTable mainOut = mainTableInOut[0];
    SqlConditionNode whereIn = whereInOut[0];
    whereInOut[0] = null;
    SqlTable top = mainIn;
    Vector<SqlTable> rightWithJoins = new Vector<SqlTable>();
    SqlJoin join = top.getJoin();
    while (join != null) {
      SqlConditionNode on = join.getCondition();
      boolean isStandard = SqlNormalizationHelper.isStandardOnCriteria(on);
      if (!isStandard) {
        SqlConditionNode [] onInOut = new SqlConditionNode[] {on};
        standardCriteriaInJoin(onInOut, whereInOut);
        join = new SqlJoin(join.getJoinType(), join.getTable(), onInOut[0], join.isEach(), join.hasOuter());
      }
      top = new SqlTable(top.getCatalog(), top.getSchema(), top.getName(), top.getAlias(), join, top.getNestedJoin(), top.getQuery(), top.getTableValueFunction(), top.getCrossApply());
      rightWithJoins.add(top);
      top = join.getTable();
      join = top.getJoin();
    }
    SqlTable l, r;
    if (rightWithJoins.size() > 0) {
      mainOut = rightWithJoins.get(0);
      for (int i = rightWithJoins.size() - 1; i >= 1; --i) {
        r = rightWithJoins.get(i);
        l = rightWithJoins.get(i - 1);
        SqlJoin j = l.getJoin();
        l = new SqlTable(l.getCatalog(), l.getSchema(), l.getName(), l.getAlias(), new SqlJoin(j.getJoinType(), r, j.getCondition(), j.isEach(), j.hasOuter()), l.getNestedJoin(), l.getQuery(), l.getTableValueFunction(), l.getCrossApply());
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

  private static void standardCriteriaInJoin(SqlConditionNode [] onInOut, SqlConditionNode [] whereInOut) throws Exception {
    SqlConditionNode onIn = onInOut[0];
    SqlConditionNode onOut = onInOut[0];
    SqlConditionNode whereIn = whereInOut[0];
    SqlConditionNode whereOut = whereInOut[0];
    if (onIn instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition) onInOut[0];
      SqlConditionNode left = (SqlConditionNode)condition.getLeft();
      SqlConditionNode [] newLeft = new SqlConditionNode[] {left};
      boolean isLeftResolved = false;
      boolean isRightResolved = false;
      if (!SqlNormalizationHelper.isStandardOnCriteria(left)) {
        standardCriteriaInJoin(newLeft, whereInOut);
        isLeftResolved = true;
      }
      SqlConditionNode right = (SqlConditionNode)condition.getRight();
      SqlConditionNode [] newRight = new SqlConditionNode[] {right};
      if (!SqlNormalizationHelper.isStandardOnCriteria(right)) {
        standardCriteriaInJoin(newRight, whereInOut);
        isRightResolved = true;
      }
      if (isLeftResolved || isRightResolved) {
        if (newLeft[0] != null && newRight[0] != null) {
          onOut = new SqlCondition(newLeft[0], condition.getLogicOp(), newRight[0]);
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
    } else if (onIn instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria) onIn;
      if (!SqlNormalizationHelper.isStandardOnCriteria(c)) {
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

  private static SqlQueryStatement normalizeImplicitJoin(SqlQueryStatement query, JoinType type) throws Exception {
    if (SqlNormalizationHelper.isImplicitJoin(query)) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();

      SqlCollection<SqlTable> commaTables = stmt.getTables();
      for (int i = 0 ; i < commaTables.size(); ++i) {
        SqlTable commaTable = commaTables.get(i);
        if (commaTable.isNestedQueryTable()) {
          SqlQueryStatement commaQuery = normalizeImplicitJoin(commaTable.getQuery(), type);
          commaTable = new SqlTable(commaTable.getCatalog(),
                  commaTable.getSchema(),
                  commaTable.getName(),
                  commaTable.getAlias(),
                  commaTable.getJoin(),
                  null,
                  commaQuery,
                  commaTable.getTableValueFunction(),
                  commaTable.getCrossApply());
          commaTables.set(i, commaTable);
        }
      }

      boolean equiInnerFactor = false;

      if (type == JoinType.INNER) {
        equiInnerFactor = SqlNormalizationHelper.isEquiJoinCriteria(condition, tables);
      }

      boolean support = type == JoinType.INNER && equiInnerFactor || type != JoinType.INNER;

      if (!support) {
        throwNoSupportNormalizationException(IMPLICIT_JOIN_EXCEPTION_CODE, query);
      }

      SqlCriteria[] equiOn = null;
      if (equiInnerFactor) {
        SqlConditionNode [] inWhere = new SqlConditionNode[] {condition};
        equiOn = resolveEquiJoinCriteria(inWhere, tables);
        condition = inWhere[0];
      }

      SqlTable newTop = stmt.getTables().get(stmt.getTables().size() - 1);
      if (newTop.isNestedQueryTable()) {
        newTop = new SqlTable(normalizeImplicitJoin(newTop.getQuery(), type),
                newTop.getName(),
                newTop.getAlias());
      }
      for (int i = tables.size() - 1 ; i >= 1; --i) {
        SqlTable l = tables.get(i - 1);
        if (l.isNestedQueryTable()) {
          l = new SqlTable(normalizeImplicitJoin(l.getQuery(), type),
                  l.getName(),
                  l.getAlias());
        }
        boolean isOuter = type == JoinType.LEFT || type == JoinType.RIGHT || type == JoinType.FULL;
        if (newTop.hasJoin() && isOuter) {
          newTop = new SqlTable(null, newTop);
        }
        newTop = concatJoinTable(l,
                newTop,
                type,
                equiOn == null ? null : equiOn[i - 1]);
      }
      SqlCollection<SqlTable> ts = new SqlCollection<SqlTable>();
      ts.add(newTop);
      stmt.setTables(ts);
      stmt.setCriteria(condition);
      return stmt;
    } else if (query instanceof SqlSelectStatement
            && containsNestedQuery(query.getTable())) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlTable mainTable = stmt.getTable();
      if (mainTable.isNestedQueryTable()) {
        SqlQueryStatement nestedQuery = mainTable.getQuery();
        SqlQueryStatement resolvedNestedQuery = normalizeImplicitJoin(nestedQuery, type);
        mainTable = new SqlTable(null,
                null,
                mainTable.getName(),
                mainTable.getAlias(),
                mainTable.getJoin(),
                null,
                resolvedNestedQuery,
                null,
                mainTable.getCrossApply());
      }
      query.setTable(mainTable);
      return query;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeEquiInnerJoin(SqlQueryStatement query) throws Exception {

    if (!(query instanceof SqlSelectStatement)) return query;

    boolean allInner = true;
    SqlCollection<SqlJoin> joins = query.getJoins();
    SqlCollection<SqlTable> flattenTables = new SqlCollection<SqlTable>();
    SqlTable mainTable = query.getTable();
    flattenTables.add(new SqlTable(mainTable.getCatalog(),
            mainTable.getSchema(),
            mainTable.getName(),
            mainTable.getAlias(),
            null,
            null,
            mainTable.getQuery(),
            mainTable.getTableValueFunction(),
            mainTable.getCrossApply()));

    for (SqlJoin j : joins) {
      if ((j.getJoinType() != JoinType.INNER && j.getJoinType() != JoinType.NATURAL)
          || j.getTable().isNestedJoinTable()) {
        allInner = false;
        break;
      }
      SqlTable right = j.getTable();
      flattenTables.add(new SqlTable(right.getCatalog(),
              right.getSchema(),
              right.getName(),
              right.getAlias(),
              null,
              null,
              right.getQuery(),
              right.getTableValueFunction(),
              right.getCrossApply()));
    }

    if (!allInner) return query;

    boolean equiInnerFactor = SqlNormalizationHelper.isEquiJoinCriteria(query.getCriteria(), flattenTables);
    if (!equiInnerFactor) return query;

    SqlSelectStatement stmt = (SqlSelectStatement)query;
    SqlCollection<SqlColumn> columns = stmt.getColumns();
    SqlConditionNode havingClause = stmt.getHavingClause();
    SqlConditionNode condition = stmt.getCriteria();
    SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
    SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();

    SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
    boolean fromLast = stmt.getFromLast();
    SqlExpression limitExpr = stmt.getLimitExpr();
    SqlExpression offsetExpr = stmt.getOffsetExpr();

    SqlCriteria[] equiOn = null;
    if (equiInnerFactor) {
      SqlConditionNode [] inWhere = new SqlConditionNode[] {condition};
      equiOn = resolveEquiJoinCriteria(inWhere, flattenTables);
      condition = inWhere[0];
    }

    SqlTable newTop = flattenTables.get(flattenTables.size() - 1);
    if (newTop.isNestedQueryTable()) {
      newTop = new SqlTable(normalizeImplicitJoin(newTop.getQuery(), JoinType.INNER),
              newTop.getName(),
              newTop.getAlias());
    }

    for (int i = flattenTables.size() - 1 ; i >= 1; --i) {
      SqlTable l = flattenTables.get(i - 1);
      if (l.isNestedQueryTable()) {
        l = new SqlTable(normalizeImplicitJoin(l.getQuery(), JoinType.INNER),
                l.getName(),
                l.getAlias());
      }

      newTop = concatJoinTable(l,
              newTop,
              JoinType.INNER,
              equiOn == null ? null : equiOn[i - 1]);
    }

    stmt.setTable(newTop);
    stmt.setCriteria(condition);
    return stmt;
  }

  private static SqlQueryStatement normalizeTableAlias(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();
      SqlCollection<SqlTable> flattens = SqlNormalizationHelper.flattenTables(query);
      columns = resolveTableAliasInColumns(columns, flattens);

      condition = resolveTableAliasInCriteria(condition, flattens);

      havingClause = resolveTableAliasInCriteria(havingClause, flattens);

      groupBy = resolveTableAliasInGroupBy(groupBy, flattens);

      orderBy = resolveTableAliasInOrderBy(orderBy, flattens);

      for (int i = 0 ; i < tables.size(); ++i) {
        SqlTable t = tables.get(i);
        if (t.isNestedQueryTable()) {
          SqlQueryStatement nq = normalizeTableAlias(t.getQuery());
        }
        t = resolvedMainTableAlias(t, flattens);
        tables.set(i, t);
      }
      query.setColumns(columns);
      query.setCriteria(condition);
      ((SqlSelectStatement) query).setHavingClause(havingClause);
      ((SqlSelectStatement) query).setTables(tables);
      ((SqlSelectStatement) query).setGroupByClause(groupBy, query.getEachGroupBy());
      query.setOrderBy(orderBy);
      return query;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeCriteriaWithNot(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();
      condition = resolvedConditionNot(condition);
      stmt.setCriteria(condition);
      return stmt;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeCriteria(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();
      condition = resolvedCondition(condition);
      query.setCriteria(condition);
      return query;
    } else {
      return query;
    }
  }

  private static SqlQueryStatement normalizeMinimizeCriteria(SqlQueryStatement query) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();
      SqlCollection<SqlValueExpression> parameters = stmt.getParameterList();
      boolean fromLast = stmt.getFromLast();
      SqlExpression limitExpr = stmt.getLimitExpr();
      SqlExpression offsetExpr = stmt.getOffsetExpr();

      boolean modified = false;

      SqlConditionNode minimizeCriteria = (SqlConditionNode) minimizeCriteria(condition);
      if (minimizeCriteria != condition) {
        modified = true;
      }

      SqlConditionNode minimizeHaving = (SqlConditionNode) minimizeCriteria(havingClause);
      if (minimizeHaving != condition) {
        modified = true;
      }

      if (modified) {
        stmt.setCriteria(minimizeCriteria);
        stmt.setHavingClause(minimizeHaving);
        return stmt;
      } else {
        return query;
      }
    } else {
      return query;
    }
  }

  private static SqlExpression minimizeCriteria(SqlExpression node) throws Exception {
    SqlExpression resolved = node;
    if (node instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition) node;
      SqlExpression left = condition.getLeft();
      SqlExpression right = condition.getRight();

      if (SqlLogicalOperator.And == condition.getLogicOp()) {
        if (left.isEvaluatable()) {
          SqlValue trueOrfalse = left.evaluate();
          boolean v = trueOrfalse.getValueAsBool(true);
          if (v) {
            resolved = minimizeCriteria(right);
          } else {
            resolved = null;
          }
        } else if (right.isEvaluatable()) {
          SqlValue trueOrfalse = right.evaluate();
          boolean v = trueOrfalse.getValueAsBool(true);
          if (v) {
            resolved = minimizeCriteria(left);
          } else {
            resolved = null;
          }
        } else {
          SqlExpression mLeft = minimizeCriteria(left);
          SqlExpression mRight = minimizeCriteria(right);
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
      } else if (SqlLogicalOperator.Or == condition.getLogicOp()) {
        if (left.isEvaluatable()) {
          SqlValue trueOrfalse = left.evaluate();
          boolean v = trueOrfalse.getValueAsBool(false);
          if (v) {
            resolved = null;
          } else {
            resolved = minimizeCriteria(right);
          }
        } else if (right.isEvaluatable()) {
          SqlValue trueOrfalse = right.evaluate();
          boolean v = trueOrfalse.getValueAsBool(false);
          if (v) {
            resolved = null;
          } else {
            resolved = minimizeCriteria(left);
          }
        } else {
          SqlExpression mLeft = minimizeCriteria(left);
          SqlExpression mRight = minimizeCriteria(right);
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
    } else if (node instanceof SqlConditionNot) {
      SqlConditionNot notCondition = (SqlConditionNot) node;
      SqlExpression expr = notCondition.getCondition();
      if (expr.isEvaluatable()) {
        SqlValue trueOrfalse = expr.evaluate();
        boolean v = trueOrfalse.getValueAsBool(true);
        if (v) {
          resolved = new SqlCriteria(null, ComparisonType.FALSE, null);
        } else {
          resolved = null;
        }
      } else {
        SqlExpression minExpr = minimizeCriteria(expr);
        if (minExpr != expr) {
          if (minExpr != null) {
            resolved = new SqlConditionNot(minExpr);
          }
        }
      }
    } else if (node instanceof SqlCriteria) {
      SqlCriteria cr = (SqlCriteria) node;
      if (cr.isEvaluatable()) {
        resolved = null;
      }
    }
    return resolved;
  }

  private static SqlConditionNode resolvedCondition(SqlConditionNode condition) throws Exception {
    SqlConditionNode resolved = condition;
    if (condition instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)condition;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (l instanceof SqlConditionNode) {
        l = resolvedCondition((SqlConditionNode)l);
      }
      if (r instanceof SqlConditionNode) {
        r = resolvedCondition((SqlConditionNode)r);
      }
      resolved = new SqlCondition(l, c.getLogicOp(), r);
    } else if (condition instanceof SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)condition;
      if (criteria.getRight() instanceof SqlGeneralColumn && criteria.getLeft() instanceof SqlValueExpression) {
        switch (criteria.getOperator()) {
          case /*#ComparisonType.#*/EQUAL:
            resolved = new SqlCriteria(criteria.getRight(), ComparisonType.EQUAL, criteria.getLeft());
            break;
          case /*#ComparisonType.#*/BIGGER_EQUAL:
            resolved = new SqlCriteria(criteria.getRight(), ComparisonType.SMALLER_EQUAL, criteria.getLeft());
            break;
          case /*#ComparisonType.#*/BIGGER:
            resolved = new SqlCriteria(criteria.getRight(), ComparisonType.SMALLER, criteria.getLeft());
            break;
          case /*#ComparisonType.#*/SMALLER_EQUAL:
            resolved = new SqlCriteria(criteria.getRight(), ComparisonType.BIGGER_EQUAL, criteria.getLeft());
            break;
          case /*#ComparisonType.#*/SMALLER:
            resolved = new SqlCriteria( criteria.getRight(), ComparisonType.BIGGER, criteria.getLeft());
            break;
          case /*#ComparisonType.#*/NOT_EQUAL:
            resolved = new SqlCriteria(criteria.getRight(), ComparisonType.NOT_EQUAL, criteria.getLeft());
            break;
        }
      }
    }
    return resolved;
  }

  private static SqlConditionNode resolvedConditionNot(SqlConditionNode condition) throws Exception {
    SqlConditionNode resolved = condition;
    if (condition instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)condition;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (l instanceof SqlConditionNode) {
        l = resolvedConditionNot((SqlConditionNode)l);
      }
      if (r instanceof SqlConditionNode) {
        r = resolvedConditionNot((SqlConditionNode)r);
      }
      resolved = new SqlCondition(l, c.getLogicOp(), r);
    } else if (condition instanceof SqlConditionNot) {
      SqlConditionNode conditionInNot = (SqlConditionNode)((SqlConditionNot) condition).getCondition();
      resolved = resolvedConditionInNot(conditionInNot);
    }
    return resolved;
  }

  private static SqlConditionNode resolvedConditionInNot(SqlConditionNode c) throws Exception {
    SqlConditionNode resolved = c;
    if (c instanceof SqlConditionInSelect) {
      //do noting now.
      resolved = new SqlConditionNot(c);
    } else if (c instanceof SqlConditionExists) {
      //do noting now.
      resolved = new SqlConditionNot(c);
    } else if (c instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition)c;
      SqlExpression l = condition.getLeft();
      SqlExpression r = condition.getRight();
      SqlLogicalOperator newOp;
      if (condition.getLogicOp() == SqlLogicalOperator.And) {
        newOp = SqlLogicalOperator.Or;
      } else {
        newOp = SqlLogicalOperator.And;
      }
      resolved = resolvedConditionNot(new SqlCondition(new SqlConditionNot(l), newOp, new SqlConditionNot(r)));
    } else if (c instanceof SqlConditionNot) {
      resolved = resolvedConditionNot((SqlConditionNode)((SqlConditionNot) c).getCondition());
    } else if (c instanceof SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)c;
      switch (criteria.getOperator()) {
        case /*#ComparisonType.#*/EQUAL:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.NOT_EQUAL, criteria.getRight());
          break;
        case /*#ComparisonType.#*/BIGGER_EQUAL:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.SMALLER, criteria.getRight());
          break;
        case /*#ComparisonType.#*/BIGGER:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.SMALLER_EQUAL, criteria.getRight());
          break;
        case /*#ComparisonType.#*/SMALLER_EQUAL:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.BIGGER, criteria.getRight());
          break;
        case /*#ComparisonType.#*/SMALLER:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.BIGGER_EQUAL, criteria.getRight());
          break;
        case /*#ComparisonType.#*/NOT_EQUAL:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.EQUAL, criteria.getRight());
          break;
        case /*#ComparisonType.#*/IS_NOT:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.IS, criteria.getRight());
          break;
        case /*#ComparisonType.#*/IS:
          resolved = new SqlCriteria(criteria.getLeft(), ComparisonType.IS_NOT, criteria.getRight());
          break;
        case /*#ComparisonType.#*/FALSE:
        case /*#ComparisonType.#*/LIKE:
        case /*#ComparisonType.#*/IN:
          //do noting now.
          resolved = new SqlConditionNot(criteria);
          break;
        case /*#ComparisonType.#*/CUSTOM:
          resolved = new SqlCriteria(criteria.getLeft(), criteria.getCustomOp(), criteria.getRight());
          break;
      }
    }
    return resolved;
  }

  private static SqlQueryStatement normalizeCriteriaWithSubQuery(SqlQueryStatement query, JoinType joinType) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode condition = stmt.getCriteria();
      SqlTable table = stmt.getTable();
      SqlCollection<SqlTable> tables = stmt.getTables();

      SqlTable [] tableInOut = {table};
      SqlConditionNode [] conditionInOut = {condition};
      SqlExpression [] LIMIT_OFFSET = {null, null};
      normalizeCriteriaSubQuery(columns,
          tableInOut,
          conditionInOut,
          joinType,
          LIMIT_OFFSET);
      table = tableInOut[0];
      condition = conditionInOut[0];
      if (condition != stmt.getCriteria()) {
        SqlCollection<SqlTable> owner = new SqlCollection<SqlTable>();
        owner.add(new SqlTable(table.getAlias()));
        condition = attachTableForCriteria(owner, condition);
      }
      tables.set(0, table);
      stmt.setTables(tables);
      stmt.setCriteria(condition);
      if (null == stmt.getLimitExpr()) {
        stmt.setLimitExpr(LIMIT_OFFSET[0]);
      }

      if (null == stmt.getOffsetExpr()) {
        stmt.setOffsetExpr(LIMIT_OFFSET[1]);
      }
      return stmt;
    } else {
      return query;
    }
  }

  private static void getNestedQueryTables(SqlTable table, SqlCollection<SqlTable> nestedTables) throws Exception {
    if (table == null) return;
    if (containsNestedQuery(table)) {
      if (table.isNestedQueryTable()) {
        nestedTables.add(table);
      }
      if (table.hasJoin()) {
        SqlJoin join = table.getJoin();
        SqlTable r;
        do {
          r = join.getTable();
          if (r.isNestedQueryTable()) {
            nestedTables.add(r);
          }
          if (r.isNestedJoinTable()) {
            getNestedQueryTables(r.getNestedJoin(), nestedTables);
          }
          join = r.getJoin();
        } while (r.hasJoin());
      }
    }
  }

  private static SqlTable matchOwnerOfColumn(SqlColumn mc, SqlCollection<SqlTable> tableList) throws Exception {
    SqlTable owner = null;
    String tNameOrAlias = null;
    if (mc.getTable() != null) {
      tNameOrAlias = mc.getTable().getAlias();
    }
    for (SqlTable t : tableList) {
      boolean match = false;
      SqlTable matchedTable = null;
      if (t.isNestedQueryTable()) {
        if (Utilities.equalIgnoreCaseInvariant(t.getAlias(), tNameOrAlias)) {
          match = true;
          matchedTable = t;
        } else if (null == tNameOrAlias) {
          SqlColumn mappingColumn = SqlNormalizationHelper.getMappingNestedColumn(mc, t.getQuery().getColumns());
          if (mappingColumn != null) {
            match = true;
            matchedTable = t;
          }
        }
      } else if (Utilities.equalIgnoreCaseInvariant(t.getAlias(), tNameOrAlias)) {
        match = true;
        matchedTable = t;
      } else {
        if (1 == tableList.size()) {
          match = true;
          owner = t;
        } else {
          continue;
        }
      }
      if (match) {
        if (owner == null) {
          if (isSimpleTable(matchedTable)) {
            owner = new SqlTable(matchedTable.getName(), matchedTable.getAlias());
          } else if (matchedTable.isNestedQueryTable()){
            owner = (SqlTable) matchedTable.clone();
          }
        }
        break;
      }
    }
    return owner;
  }

  private static SqlQueryStatement normalizeNestedQueryTableName(SqlQueryStatement query, IDataMetadata dataMetaData) throws Exception {
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement stmt = (SqlSelectStatement)query;
      SqlCollection<SqlColumn> columns = stmt.getColumns();
      SqlConditionNode havingClause = stmt.getHavingClause();
      SqlConditionNode condition = stmt.getCriteria();
      SqlCollection<SqlOrderSpec> orderBy = stmt.getOrderBy();
      SqlCollection<SqlExpression> groupBy = stmt.getGroupBy();
      SqlCollection<SqlTable> tables = stmt.getTables();

      if (containsNestedQuery(tables)) {
        SqlTable mainTable = stmt.getTable();
        SqlCollection<SqlTable> nestedQueryTables = new SqlCollection<SqlTable>();
        for (SqlTable table : tables) {
          getNestedQueryTables(table, nestedQueryTables);
        }

        if (query.isAsteriskQuery() && query.isJoinQuery()) {
          SqlWildcardColumn wc = null;
          for (int i = 0 ; i < columns.size(); ++i) {
            SqlColumn c = columns.get(i);
            if (c instanceof SqlWildcardColumn && null == c.getTable()) {
              wc = (SqlWildcardColumn) c;
              break;
            }
          }
          if (wc != null) {
            columns.remove(wc);
            SqlCollection<SqlTable> flatten = SqlNormalizationHelper.flattenTables(query);
            for (SqlTable t : flatten) {
              columns.add(new SqlWildcardColumn(new SqlTable(t.getAlias())));
            }
          }
        }
        attachOwnerTableForColumns(nestedQueryTables, columns);
        if (mainTable.isNestedQueryTable()) {
          normalizeNestedQueryTableName(mainTable.getQuery(), dataMetaData);
        }

        SqlJoin join = mainTable.getJoin();
        SqlTable newTop = new SqlTable(mainTable.getCatalog(),
            mainTable.getSchema(),
            mainTable.getName(),
            mainTable.getAlias(),
            null,
            mainTable.getNestedJoin(),
            mainTable.getQuery(),
            mainTable.getTableValueFunction(),
            mainTable.getCrossApply());
        while (join != null) {
          SqlTable r = join.getTable();
          if (r.isNestedQueryTable()) {
            normalizeNestedQueryTableName(r.getQuery(), dataMetaData);
          }

          SqlConditionNode on = attachTableForCriteria(nestedQueryTables, join.getCondition());
          SqlTable nr = new SqlTable(r.getCatalog(),
              r.getSchema(),
              r.getName(),
              r.getAlias(),
              null,
              r.getNestedJoin(),
              r.getQuery(),
              r.getTableValueFunction(),
              r.getCrossApply());
          newTop = concatJoinTable(newTop, nr, join.getJoinType(), on);
          join = r.getJoin();
        }

        tables.set(0, newTop);

        orderBy = attachOwnerTableForOrderBy(orderBy, nestedQueryTables);

        groupBy = attachOwnerTableForGroupBy(groupBy, nestedQueryTables);

        havingClause = attachTableForCriteria(nestedQueryTables, havingClause);

        condition = attachTableForCriteria(nestedQueryTables, condition);

        stmt.setColumns(columns);
        stmt.setHavingClause(havingClause);
        stmt.setOrderBy(orderBy);
        stmt.setGroupByClause(groupBy, stmt.getEachGroupBy());
        stmt.setTables(tables);
        stmt.setCriteria(condition);
      }

      if (stmt.isJoinQuery()) {
        ColumnOwnerModifier ownerModifier = new ColumnOwnerModifier(stmt, dataMetaData);
        ownerModifier.modify(new SqlParser(stmt));
        Exception error = ownerModifier.getError();
        if (error != null) {
          throw error;
        }
      }

      return stmt;
    } else {
      return query;
    }
  }

  private static void normalizeCriteriaSubQuery(SqlCollection<SqlColumn> columnsInOut,
                                                SqlTable [] tableInOut,
                                                SqlConditionNode [] conditionInOut,
                                                JoinType joinType,
                                                SqlExpression [] outLimitOffSet) throws Exception {
    SqlConditionNode conditionParaIn = conditionInOut[0];
    if (conditionParaIn instanceof SqlConditionInSelect
        || conditionParaIn instanceof SqlCriteria) {
      SqlTable table = tableInOut[0];
      SqlCriteria c = (SqlCriteria)conditionParaIn;
      if (c.getRight() instanceof SqlSubQueryExpression) {
        SqlSubQueryExpression sub = (SqlSubQueryExpression)c.getRight();
        SqlQueryStatement sq = sub.getQuery();
        boolean support = !containsAggragation(sq.getColumns()) && sq.getGroupBy().size() == 0 && isSimpleTable(sq.getTable());
        if (!support) {
          throwNoSupportNormalizationException(CRITERIA_SUBQUERY_EXCEPTION_CODE, sq);
        }
        String leftAlias = table.getAlias();
        String rightAlias = sq.getTable().getAlias();
        if (leftAlias.equals(rightAlias)) {
          leftAlias = leftAlias + "1";
          rightAlias = rightAlias + "2";
        }
        SqlTable right = new SqlTable(sq.getTable().getCatalog(), sq.getTable().getSchema(), sq.getTable().getName(), rightAlias);
        SqlTable leftAliasTable = new SqlTable(table.getCatalog(), table.getSchema(), leftAlias, leftAlias);
        SqlCollection<SqlTable> LEFTOWNERS = new SqlCollection<SqlTable>();
        LEFTOWNERS.add(leftAliasTable);
        SqlTable rightALiasTable = new SqlTable(right.getCatalog(), table.getSchema(), rightAlias, rightAlias);
        SqlCollection<SqlTable> RIGHTOWNERS = new SqlCollection<SqlTable>();
        RIGHTOWNERS.add(rightALiasTable);
        SqlCriteria on = new SqlCriteria(attachOwnerTableForColumn(LEFTOWNERS, (SqlColumn)c.getLeft()),
                c.getOperator(),
                c.getCustomOp(),
                attachOwnerTableForColumn(RIGHTOWNERS, sq.getColumns().get(0)));
        SqlJoin join = new SqlJoin(joinType, right, on);
        table = new SqlTable(table.getCatalog(),
                table.getSchema(),
                table.getName(),
                leftAlias,
                join,
                table.getNestedJoin(),
                table.getQuery(),
                table.getTableValueFunction(),
                table.getCrossApply());
        tableInOut[0] = table;
        conditionInOut[0] = attachTableForCriteria(RIGHTOWNERS, sq.getCriteria());
        outLimitOffSet[0] = sq.getLimitExpr();
        outLimitOffSet[1] = sq.getOffsetExpr();
        attachOwnerTableForColumns(LEFTOWNERS, columnsInOut);
      }
    } else if (conditionParaIn instanceof SqlConditionExists) {
      SqlConditionExists exists = (SqlConditionExists) conditionParaIn;
      SqlQueryStatement nq = exists.getSubQuery();
      SqlCollection<SqlTable> twoTables = new SqlCollection<SqlTable>();
      SqlTable table = tableInOut[0];
      twoTables.add(new SqlTable(table.getName(), table.getAlias()));
      twoTables.add(new SqlTable(nq.getTable().getName(), nq.getTable().getAlias()));
      boolean equiInnerFactor = SqlNormalizationHelper.isEquiJoinCriteria(nq.getCriteria(), twoTables);
      boolean support = !containsAggragation(nq.getColumns())
          && nq.getGroupBy().size() == 0
          && isSimpleTable(nq.getTable())
          && equiInnerFactor;
      if (!support) {
        throwNoSupportNormalizationException(CRITERIA_SUBQUERY_EXCEPTION_CODE, nq);
      }
      table = concatJoinTable(table,
          nq.getTable(),
          joinType,
          nq.getCriteria());
      tableInOut[0] = table;
      conditionInOut[0] = null;
    } else if (conditionParaIn instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)conditionParaIn;
      SqlExpression l = c.getLeft();
      SqlConditionNode [] leftOutIn = {(SqlConditionNode) l};
      SqlExpression r = c.getRight();
      SqlConditionNode [] rightOutIn = {(SqlConditionNode) r};
      normalizeCriteriaSubQuery(columnsInOut,
          tableInOut,
          leftOutIn,
          joinType,
          outLimitOffSet);
      normalizeCriteriaSubQuery(columnsInOut,
          tableInOut,
          rightOutIn,
          joinType,
          outLimitOffSet);
      if (leftOutIn[0] != null && rightOutIn[0] != null) {
        conditionInOut[0] = new SqlCondition(leftOutIn[0], c.getLogicOp(), rightOutIn[0]);
      } else if (leftOutIn[0] != null) {
        conditionInOut[0] = leftOutIn[0];
      } else if (rightOutIn[0] != null) {
        conditionInOut[0] = rightOutIn[0];
      }
    } else if (conditionParaIn instanceof SqlConditionNot && joinType == JoinType.LEFT_SEMI) {
      SqlExpression c = ((SqlConditionNot) conditionParaIn).getCondition();
      if (c instanceof SqlConditionNode) {
        SqlConditionNode [] inSideOutIn = {(SqlConditionNode) c};
        normalizeCriteriaSubQuery(columnsInOut,
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

  private static void attachOwnerTableForColumns(SqlCollection<SqlTable> tables, SqlCollection<SqlColumn> columns) throws Exception {
    for (int i = 0 ; i < columns.size(); ++i) {
      SqlColumn c = attachOwnerTableForColumn(tables, columns.get(i));
      columns.set(i, c);
    }
  }

  private static SqlConditionNode attachTableForCriteria(SqlCollection<SqlTable> owners, SqlConditionNode c) throws Exception {
    SqlConditionNode resovled = c;
    if (c instanceof SqlConditionInSelect) {
      SqlConditionInSelect s = (SqlConditionInSelect)c;
      SqlColumn left = attachOwnerTableForColumn(owners, (SqlColumn) s.getLeft());
      resovled = new SqlConditionInSelect(left, s.getRightQuery(), s.isAll(), s.getOperator());
    } else if (c instanceof SqlConditionExists) {
      //do nothing now.
    } else if (c instanceof SqlConditionNot) {
      SqlConditionNot nc = (SqlConditionNot)c;
      resovled = new SqlConditionNot(attachTableForCriteria(owners, (SqlConditionNode)nc.getCondition()));
    } else if (c instanceof  SqlCondition) {
      SqlCondition condition = (SqlCondition) c;
      SqlExpression l = condition.getLeft();
      SqlExpression r = condition.getRight();
      if (l instanceof SqlConditionNode) {
        l = attachTableForCriteria(owners, (SqlConditionNode)l);
      }
      if (r instanceof SqlConditionNode) {
        r = attachTableForCriteria(owners, (SqlConditionNode)r);
      }
      resovled = new SqlCondition(l, condition.getLogicOp(), r);
    } else if (c instanceof SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)c;
      SqlExpression l = criteria.getLeft();
      SqlExpression r = criteria.getRight();
      if (l instanceof SqlColumn) {
        SqlColumn column = (SqlColumn)l;
        if (column.getTable() == null) {
          l = attachOwnerTableForColumn(owners, column);
        }
      }
      if (r instanceof SqlColumn) {
        SqlColumn column = (SqlColumn)r;
        if (column.getTable() == null) {
          r = attachOwnerTableForColumn(owners, column);
        }
      }
      resovled = new SqlCriteria(l, criteria.getOperator(), criteria.getCustomOp(), r);
    }
    return resovled;
  }

  private static SqlColumn attachOwnerTableForColumn(SqlCollection<SqlTable> owners, SqlColumn c) throws Exception {
    SqlColumn resovled = c;
    if (c instanceof SqlGeneralColumn) {
      if (null == c.getTable()) {
        SqlTable owner = matchOwnerOfColumn(c, owners);
        if (owner != null) {
          if (c instanceof SqlWildcardColumn) {
            resovled =  new SqlWildcardColumn(owner);
          } else {
            SqlGeneralColumn col = (SqlGeneralColumn)c;
            resovled = col.hasAlias() ? new SqlGeneralColumn(owner, col.getColumnName(), col.getAlias()) :  new SqlGeneralColumn(owner, col.getColumnName());
          }
        }
      }
    } else if (c instanceof SqlFormulaColumn) {
      SqlFormulaColumn fc = (SqlFormulaColumn)c;
      for (int i = 0 ; i < fc.getParameters().size(); ++i) {
        SqlExpression para = fc.getParameters().get(i);
        if (para instanceof SqlColumn) {
          para = attachOwnerTableForColumn(owners, (SqlColumn)para);
          fc.getParameters().set(i, para);
        } else if (para instanceof SqlConditionNode) {
          para = attachTableForCriteria(owners, (SqlConditionNode) para);
          fc.getParameters().set(i, para);
        }
      }
    } else if (c instanceof SqlOperationColumn) {
      SqlOperationColumn oc = (SqlOperationColumn)c;
      SqlOperationExpression oe = (SqlOperationExpression)oc.getExpr();
      SqlExpression l = oe.getLeft();
      SqlExpression r = oe.getRight();
      if (l instanceof SqlColumn) {
        l = attachOwnerTableForColumn(owners, (SqlColumn)l);
      }
      if (r instanceof SqlColumn) {
        r = attachOwnerTableForColumn(owners, (SqlColumn)r);
      }
      resovled = new SqlOperationColumn(oc.getAlias(), new SqlOperationExpression(oe.getOperator(), l, r));
    }
    return resovled;
  }

  private static boolean checkNormalizeSupport(SqlQueryStatement mainQuery, SqlQueryStatement nestedQuery) {
    boolean isSelect = nestedQuery instanceof SqlSelectStatement;
    boolean noWhereInMain = mainQuery.getCriteria() == null || mainQuery.getCriteria() instanceof SqlConditionExists;
    boolean noJoinInNested = !isJoinQuery(nestedQuery);
    boolean noJoinInMain = !mainQuery.getTable().hasJoin();
    boolean joinFactor = noJoinInNested || noJoinInMain;
    boolean noAggInMain = !containsAggragation(mainQuery.getColumns()) && 0 == mainQuery.getGroupBy().size();
    boolean noAggInNested = !containsAggragation(nestedQuery.getColumns()) &&  0 == nestedQuery.getGroupBy().size();
    boolean asteriskFactor = noJoinInNested || !nestedQuery.isAsteriskQuery();
    boolean aggFactor = false;
    boolean noImplicitJoin = true;
    boolean hasLimitInNested = nestedQuery.getLimitExpr() != null;
    boolean limitFactor = hasLimitInNested && (mainQuery.isJoinQuery() || !noAggInMain);
    boolean distinctFactor = nestedQuery.isDistinct();
    if (nestedQuery instanceof SqlSelectStatement) {
      noImplicitJoin = ((SqlSelectStatement) nestedQuery).getTables().size() <= 1;
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
    boolean support = isSelect && joinFactor && aggFactor && noImplicitJoin && !limitFactor && !distinctFactor && asteriskFactor;
    return support;
  }

  private static SqlQueryStatement normalizeConstantColumn(SqlQueryStatement query) throws Exception {
    if (SqlNormalizationHelper.isConstantQuery(query)) {
      SqlSelectStatement noTableSelect = new SqlSelectStatement(null);
      noTableSelect.setColumns(query.getColumns());
      return noTableSelect;
    } else {
      SqlCollection<SqlColumn> columns = query.getColumns();
      for (int i = 0 ; i < columns.size(); ++i) {
        SqlColumn c = columns.get(i);
        if (!(c instanceof SqlFormulaColumn)) {
          continue;
        }

        SqlFormulaColumn function = (SqlFormulaColumn)c;
        String aggFun =  function.getColumnName();
        boolean SUM_OR_COUNT = false;
        if (Utilities.equalIgnoreCase("SUM", Utilities.toUpperCase(aggFun))) {
          SUM_OR_COUNT = true;
        }

        if (Utilities.equalIgnoreCase("COUNT", Utilities.toUpperCase(aggFun))
            || Utilities.equalIgnoreCase("COUNT_BIG", Utilities.toUpperCase(aggFun))) {
          SqlExpression PARA = function.getParameters().get(0);
          if (PARA instanceof SqlFormulaColumn) {
            SqlFormulaColumn DISTINCT = (SqlFormulaColumn) PARA;
            if (Utilities.equalIgnoreCase("DISTINCT", DISTINCT.getColumnName())
                && DISTINCT.getParameters().get(0) instanceof SqlValueExpression) {
              SqlValue ONE = new SqlValue(SqlValueType.NUMBER, "1");
              columns.set(i, new SqlConstantColumn(function.getAlias(), new SqlValueExpression(ONE)));
              continue;
            }
          }
          SUM_OR_COUNT = true;
        }

        if (SUM_OR_COUNT) continue;

        if (!SqlUtilities.isKnownAggragation(aggFun)) continue;

        if (!(function.getParameters().get(0) instanceof SqlValueExpression)) continue;

        columns.set(i, new SqlConstantColumn(function.getAlias(), (SqlValueExpression) function.getParameters().get(0)));
      }
      return query;
    }
  }

  private static SqlQueryStatement normalizeTableExprWithQuery(SqlQueryStatement mainQuery) throws Exception {
    if (mainQuery instanceof SqlSelectStatement) {
      SqlSelectStatement newQuery = (SqlSelectStatement)mainQuery;
      if (containsNestedQuery(mainQuery.getTable())) {
        if (mainQuery.getTable().isNestedQueryTable()) {
          SqlQueryStatement nestedQuery = mainQuery.getTable().getQuery();
          boolean support = checkNormalizeSupport(mainQuery, nestedQuery);
          if (support) {
            newQuery = resolveMainTableWithQuery((SqlSelectStatement)mainQuery, mainQuery.getTable());
          } else {
            //TODO: Felix do more testing. SELECT * FROM A JOIN B JOIN (SELECT * FROM C)
            throwNoSupportNormalizationException(TABLE_EXPRESSION_EXCEPTION_CODE, mainQuery);
          }
        }
        boolean hasQueryInJoin = isJoinQuery(mainQuery) && containsNestedQuery(mainQuery.getTable().getJoin().getTable());
        if (hasQueryInJoin) {
          SqlTable right = mainQuery.getTable().getJoin().getTable();
          boolean support = !isJoinQuery(right.getQuery()) && !right.isNestedJoinTable();
          if (support) {
            SqlTable mainTable = newQuery.getTable();
            SqlTable [] top = {mainTable};
            newQuery = resolveMainTableJoinWithQuery(newQuery, top);
          } else {
            //TODO: Felix do more testing. SELECT * FROM A JOIN B JOIN (SELECT * FROM C)
            throwNoSupportNormalizationException(TABLE_EXPRESSION_EXCEPTION_CODE, mainQuery);
          }
        }
      }
      return newQuery;
    } else {
      //TODO: SqlSelectUnionStatement
      return mainQuery;
    }
  }

  private static boolean containsNestedQuery(SqlTable mainTable) throws Exception {
    boolean contains = false;
    if (null == mainTable) {
      return contains;
    }
    if (mainTable.getQuery() != null) {
      contains = true;
    } else if (mainTable.getNestedJoin() != null) {
      contains = containsNestedQuery(mainTable.getNestedJoin());
    }
    if (!contains) {
      SqlJoin join = mainTable.getJoin();
      while (join != null) {
        SqlTable right = join.getTable();
        if (containsNestedQuery(right)) {
          contains = true;
          break;
        } else {
          join = right.getJoin();
        }
      }
    }
    return contains;
  }

  private static boolean containsNestedQuery(SqlCollection<SqlTable> tables) throws Exception {
    boolean contains = false;
    for (SqlTable t : tables) {
      if (containsNestedQuery(t)) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  private static SqlQueryStatement normalizeTableExpr(SqlQueryStatement mainQuery) throws Exception {
    if (mainQuery instanceof SqlSelectStatement) {
      SqlTable mainTable = mainQuery.getTable();
      if (containsNestedQuery(mainTable)) {
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

  private static SqlExpression resolveMainQueryOptions(SqlExpression ml, SqlExpression ql) throws Exception {
    SqlExpression resolved = ml;
    if (ql instanceof SqlValueExpression) {
      if (ml instanceof SqlValueExpression) {
        int main = ml.evaluate().getValueAsInt(-1);
        int query = ql.evaluate().getValueAsInt(-1);
        if (query < main) {
          resolved = ql;
        }
      } else {
        resolved = ql;
      }
    }
    return resolved;
  }

  private static SqlTable resolvedMainTableAlias(SqlTable mainTable, SqlCollection<SqlTable> flattenTables) throws Exception {
    SqlTable resolved = mainTable;
    if (!isSimpleTable(mainTable)) {
      if (mainTable.isNestedJoinTable()) {
        SqlTable nestedJoin = resolvedMainTableAlias(mainTable.getNestedJoin(), flattenTables);
        mainTable = new SqlTable(mainTable.getCatalog(), mainTable.getSchema(), mainTable.getName(), mainTable.getAlias(), mainTable.getJoin(), nestedJoin, mainTable.getQuery(), mainTable.getTableValueFunction(), mainTable.getCrossApply());
        resolved = mainTable;
      }
      if (mainTable.hasJoin()) {
        SqlJoin join =  mainTable.getJoin();
        SqlConditionNode on = resolveTableAliasInCriteria(join.getCondition(), flattenTables);
        SqlTable right = resolvedMainTableAlias(join.getTable(), flattenTables);
        join = new SqlJoin(join.getJoinType(), right, on, join.isEach(), join.hasOuter());
        resolved = new SqlTable(mainTable.getCatalog(), mainTable.getSchema(), mainTable.getName(), mainTable.getAlias(), join, mainTable.getNestedJoin(), mainTable.getQuery(), mainTable.getTableValueFunction(), mainTable.getCrossApply());
      }
    }
    return resolved;
  }

  private static SqlSelectStatement resolveMainTableWithQuery(SqlSelectStatement mainQuery, SqlTable nestedTable) throws Exception {
    SqlCollection<SqlColumn> columns = mainQuery.getColumns();
    SqlCollection<SqlColumn> copyColumns = new SqlCollection<SqlColumn>();
    for (SqlColumn c : columns) {
      copyColumns.add(c);
    }
    SqlConditionNode havingClause = mainQuery.getHavingClause();
    SqlConditionNode condition = mainQuery.getCriteria();
    SqlCollection<SqlOrderSpec> orderBy = mainQuery.getOrderBy();
    SqlCollection<SqlExpression> groupBy = mainQuery.getGroupBy();
    boolean fromLast = mainQuery.getFromLast();
    SqlExpression limitExpr = mainQuery.getLimitExpr();
    SqlExpression offsetExpr = mainQuery.getOffsetExpr();
    SqlCollection<SqlValueExpression> parameters = mainQuery.getParameterList();

    SqlTable mainTable = mainQuery.getTable();

    SqlQueryStatement nestedQueryRaw = nestedTable.getQuery();

    SqlQueryStatement nestedQuery = nestedQueryRaw;
    if (SqlNormalizationHelper.isConstantQuery(nestedQueryRaw)) {
      if (null == nestedQueryRaw.getTable()) {
        nestedQueryRaw.setTable(new SqlTable(SqlNormalizationHelper.NO_TABLE_QUERY_TABLE_NAME_PREFIX));
      }
    } else {
      try {
        nestedQuery = normalizeTableExprWithQuery(nestedQueryRaw);
      } catch (Exception ex) {
     	//do nothing.
        ;
      }
      if (!checkNormalizeSupport(mainQuery, nestedQuery)) {
        return mainQuery;
      }
    }

    nestedQuery = normalizeTableAlias(nestedQuery);

    SqlCollection<SqlColumn> nestedColumns = nestedQuery.getColumns();

    if (nestedQueryRaw != nestedQuery) {
      nestedTable = new SqlTable(nestedQuery, nestedTable.getName(), nestedTable.getAlias());
    }

    columns = resolveMainQueryColumns(columns, nestedTable);

    if (needResolveNestedQueryAliasInMainQuery(nestedQuery)) {
      Hashtable<String, SqlCollection<SqlColumn>> tAlias2NestedColumnsMap = new Hashtable<String, SqlCollection<SqlColumn>>();
      String KEY = Utilities.toUpperCase(nestedTable.getAlias());
      tAlias2NestedColumnsMap.put(KEY, nestedColumns);

      MainOwnerColumnsModifier mTableAliasModifier = new MainOwnerColumnsModifier(copyColumns, tAlias2NestedColumnsMap);
      mainQuery.setColumns(columns);
      SqlParser parser = new SqlParser(null, mainQuery, StatementType.SELECT);
      mTableAliasModifier.modify(parser);

      columns = mainQuery.getColumns();
      condition = mainQuery.getCriteria();
      havingClause = mainQuery.getHavingClause();
      orderBy = mainQuery.getOrderBy();
      groupBy = mainQuery.getGroupBy();
    }

    if (nestedQuery.getCriteria() != null
            || nestedQuery.getOrderBy().size() > 0
            || nestedQuery.getGroupBy().size() > 0) {
      if (!isJoinQuery(nestedQuery)) {
        NestedQueryModifier nestedQueryModifier = new NestedQueryModifier(new SqlTable(mainTable.getAlias(), mainTable.getAlias()));
        nestedQueryModifier.modify(new SqlParser(null, nestedQuery, StatementType.SELECT));
      }
    }

    SqlCollection<SqlColumn> populateColumns = populateNestedColumns2Main(nestedTable);

    condition = resolveColumnAliasInCriteria(condition, populateColumns);

    condition = populateNestedWhere2Main(condition, nestedQuery.getCriteria());

    havingClause = populateNestedHaving2Main(havingClause, ((SqlSelectStatement) nestedQuery).getHavingClause());

    for (int i = 0 ; i < orderBy.size(); ++i) {
      SqlOrderSpec od = orderBy.get(i);
      SqlExpression oe = od.getExpr();
      if (!(oe instanceof SqlGeneralColumn)) {
        continue;
      }
      SqlColumn c = resolveColumnAlias((SqlColumn) oe, populateColumns);
      if (c != oe) {
        od = new SqlOrderSpec(c, od.getOrder(), od.isNullsFirst(), od.hasNulls());
        orderBy.set(i, od);
      }
    }

    orderBy = populateNestedOrder2Main(orderBy, nestedQuery.getOrderBy(), nestedTable);

    for (int i = 0 ; i < groupBy.size(); ++i) {
      SqlExpression group = groupBy.get(i);
      if (!(group instanceof SqlGeneralColumn)) {
        continue;
      }
      SqlColumn c = resolveColumnAlias((SqlColumn) group, populateColumns);
      if (c != group) {
        groupBy.set(i, c);
      }
    }

    groupBy = populateNestedGroup2Main(groupBy, nestedQuery.getGroupBy());

    limitExpr = resolveMainQueryOptions(limitExpr, nestedQuery.getLimitExpr());

    if (SqlNormalizationHelper.isImplicitJoin(nestedQuery)) {
      boolean support = !mainTable.hasJoin();
      if (!support) {
        throwNoSupportNormalizationException("resolveMainTableWithQuery", mainQuery);
      }
      SqlCollection<SqlTable> nestedImplicitTables = ((SqlSelectStatement) nestedQuery).getTables();
      mainQuery.setColumns(columns);
      mainQuery.setHavingClause(havingClause);
      mainQuery.setCriteria(condition);
      mainQuery.setOrderBy(orderBy);
      mainQuery.setGroupByClause(groupBy, mainQuery.getEachGroupBy());
      mainQuery.setTables(nestedImplicitTables);
      return mainQuery;
    } else {
      SqlTable newTable = mainTable;
      String mainTableName = mainTable.getName();
      if (mainTableName.startsWith(ParserCore.DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX)) {
        SqlTable resolvedMain = nestedQuery.getTable();
        if (mainTable.getJoin() == null) {
          String mainTableAlias = needResolveNestedQueryAliasInMainQuery(nestedQuery) ? resolvedMain.getAlias() : mainTable.getAlias();
          newTable = new SqlTable(resolvedMain.getCatalog(),
                  resolvedMain.getSchema(),
                  resolvedMain.getName(),
                  mainTableAlias,
                  resolvedMain.getJoin(),
                  resolvedMain.getNestedJoin(),
                  resolvedMain.getQuery(),
                  mainTable.getTableValueFunction(),
                  mainTable.getCrossApply());
        } else {
          //TODO: SELECT * FROM (SELECT * FROM A JOIN B) AS NESTED_ALIAS JOIN C;
          SqlJoin j = mainTable.getJoin();
          SqlConditionNode on = resolveColumnAliasInCriteria(j.getCondition(), populateColumns);
          if (on != j.getCondition()) {
            j = new SqlJoin(j.getJoinType(),
                    j.getTable(),
                    on,
                    j.isEach(),
                    j.hasOuter());
          }
          newTable = new SqlTable(resolvedMain.getCatalog(),
                  resolvedMain.getSchema(),
                  resolvedMain.getName(),
                  mainTable.getAlias(),
                  j,
                  mainTable.getNestedJoin(),
                  null,
                  mainTable.getTableValueFunction(),
                  mainTable.getCrossApply());
        }
      }
      mainQuery.setColumns(columns);
      mainQuery.setHavingClause(havingClause);
      mainQuery.setCriteria(condition);
      mainQuery.setOrderBy(orderBy);
      mainQuery.setGroupByClause(groupBy, mainQuery.getEachGroupBy());
      mainQuery.setTable(newTable);
      mainQuery.setLimitExpr(limitExpr);
      mainQuery.setOffsetExpr(offsetExpr);
      return mainQuery;
    }
  }

  private static SqlSelectStatement resolveMainTableJoinWithQuery(SqlSelectStatement mainQuery, SqlTable [] topInOut) throws Exception {
    SqlTable topTable = topInOut[0];
    SqlJoin join = topTable.getJoin();
    SqlTable left = new SqlTable(topTable.getCatalog(), topTable.getSchema(), topTable.getName(), topTable.getAlias(), null, null, topTable.getQuery(), topTable.getTableValueFunction(), topTable.getCrossApply());
    while (join != null) {
      SqlTable right = join.getTable();
      if (right.isNestedQueryTable()) {
        SqlQueryStatement nestedQuery = right.getQuery();
        boolean noWhereInNestedQuery = null == nestedQuery.getCriteria();
        boolean support = checkNormalizeSupport(mainQuery, nestedQuery) && noWhereInNestedQuery;
        if (support) {
          mainQuery = resolveMainTableWithQuery(mainQuery, right);
          SqlTable resolvedRight = new SqlTable(right.getCatalog(), right.getSchema(), nestedQuery.getTableName(), right.getAlias());
          SqlCollection<SqlColumn> populateColumns = populateNestedColumns2Main(right);
          SqlConditionNode on = resolveColumnAliasInCriteria(join.getCondition(), populateColumns);
          left = concatJoinTable(left, resolvedRight, join.getJoinType(), on);
        } else {
          throwNoSupportNormalizationException(TABLE_EXPRESSION_EXCEPTION_CODE, mainQuery);
        }
      } else {
        left = right;
      }
      join = right.getJoin();
    }
    topInOut[0] = left;
    mainQuery.setTable(topInOut[0]);
    return mainQuery;
  }

  private static SqlCollection<SqlOrderSpec> populateNestedOrder2Main(SqlCollection<SqlOrderSpec> mainOrderBy,
                                                                      SqlCollection<SqlOrderSpec> nestedOrderBy,
                                                                      SqlTable nestedTable) throws Exception {
    SqlTable outerOwner = new SqlTable(nestedTable.getQuery().getTable().getAlias(), nestedTable.getAlias());
    for (int i = 0 ; i < nestedOrderBy.size(); ++i) {
      SqlOrderSpec o = nestedOrderBy.get(i);
      SqlExprModifier modifier = new NestedExprPopulateModifier(outerOwner);
      if (modifier.modify(o.getExpr())) {
        SqlExpression expr = modifier.getModifiedExpr();
        o = new SqlOrderSpec(expr, o.getOrder(), o.isNullsFirst(), o.hasNulls());
      }
      mainOrderBy.add(o);
    }
    return mainOrderBy;
  }

  private static SqlCollection<SqlExpression> populateNestedGroup2Main(SqlCollection<SqlExpression> mainGroups, SqlCollection<SqlExpression> nestedGroups) throws Exception {
    for (SqlExpression g : nestedGroups) {
      mainGroups.add(g);
    }
    return mainGroups;
  }

  private static SqlCollection<SqlColumn> resolveTableAliasInColumns(SqlCollection<SqlColumn> columns, SqlCollection<SqlTable> tables) throws Exception {
    SqlCollection<SqlColumn> resovled = columns;
    for (int i = 0 ; i < columns.size(); ++i) {
      SqlColumn c = columns.get(i);
      c = resovleTableAliasInMainQueryColumn(c, tables);
      columns.set(i, c);
    }
    return resovled;
  }

  private static SqlCollection<SqlExpression> resolveTableAliasInGroupBy(SqlCollection<SqlExpression> groups, SqlCollection<SqlTable> tables) throws Exception {
    SqlCollection<SqlExpression> resovled = groups;
    for (int i = 0 ; i < groups.size(); ++i) {
      SqlExpression g = groups.get(i);
      if (g instanceof SqlColumn) {
        SqlColumn c = (SqlColumn)g;
        c = resovleTableAliasInMainQueryColumn(c, tables);
        groups.set(i, c);
      }
    }
    return resovled;
  }

  private static SqlCollection<SqlOrderSpec> resolveTableAliasInOrderBy(SqlCollection<SqlOrderSpec> orders, SqlCollection<SqlTable> tables) throws Exception {
    SqlCollection<SqlOrderSpec> resovled = orders;
    for (int i = 0 ; i < orders.size(); ++i) {
      SqlOrderSpec o = orders.get(i);
      if (o.getExpr() instanceof SqlColumn) {
        SqlColumn c = (SqlColumn)o.getExpr();
        c = resovleTableAliasInMainQueryColumn(c, tables);
        o = new SqlOrderSpec(c, o.getOrder(), o.isNullsFirst(), o.hasNulls());
        orders.set(i, o);
      }
    }
    return resovled;
  }

  private static SqlConditionNode resolveTableAliasInCriteria(SqlConditionNode criteria, SqlCollection<SqlTable> tables) throws Exception {
    SqlConditionNode resovled = criteria;
    if (criteria instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (l instanceof SqlConditionNode) {
        l = (SqlExpression) resolveTableAliasInCriteria((SqlConditionNode)l, tables);
      }
      if (r instanceof SqlConditionNode) {
        r = (SqlExpression) resolveTableAliasInCriteria((SqlConditionNode)r, tables);
      }
      resovled = new SqlCondition(l, c.getLogicOp(), r);
    } else if (criteria instanceof SqlConditionExists) {
      SqlConditionExists c = (SqlConditionExists)criteria;
      //do nothing now.
    } else if (criteria instanceof SqlConditionNot) {
      SqlConditionNot c = (SqlConditionNot)criteria;
      SqlConditionNode r = resolveTableAliasInCriteria((SqlConditionNode)c.getCondition(), tables);
      resovled = new SqlConditionNot(r);
    } else if (criteria instanceof SqlConditionInSelect) {
      SqlConditionInSelect c = (SqlConditionInSelect)criteria;
      SqlExpression l = c.getLeft();
      if (l instanceof SqlColumn) {
        SqlColumn r = resovleTableAliasInMainQueryColumn((SqlColumn) l, tables);
        resovled = new SqlConditionInSelect(r, c.getRightQuery(), c.isAll(), c.getOperator());
      }
    } else if (criteria instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria)criteria;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (l instanceof SqlColumn) {
        l = resovleTableAliasInMainQueryColumn((SqlColumn)l, tables);
      }
      if (r instanceof SqlColumn) {
        r = resovleTableAliasInMainQueryColumn((SqlColumn)r, tables);
      }
      resovled = new SqlCriteria(l, c.getOperator(), c.getCustomOp(), r);
    }
    return resovled;
  }

  private static SqlConditionNode resolveColumnAliasInCriteria(SqlConditionNode criteria, SqlCollection<SqlColumn> primitiveColumns) throws Exception {
    SqlConditionNode resovled = criteria;
    if (criteria instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (l instanceof SqlConditionNode) {
        l = (SqlExpression)resolveColumnAliasInCriteria((SqlConditionNode) l, primitiveColumns);
      }
      if (r instanceof SqlConditionNode) {
        r = (SqlExpression)resolveColumnAliasInCriteria((SqlConditionNode) r, primitiveColumns);
      }
      resovled = new SqlCondition(l, c.getLogicOp(), r);
    } else if (criteria instanceof SqlConditionExists) {
      SqlConditionExists c = (SqlConditionExists)criteria;
      //do nothing now.
    } else if (criteria instanceof SqlConditionNot) {
      SqlConditionNot c = (SqlConditionNot)criteria;
      SqlConditionNode r = resolveColumnAliasInCriteria((SqlConditionNode) c.getCondition(), primitiveColumns);
      resovled = new SqlConditionNot(r);
    } else if (criteria instanceof SqlConditionInSelect) {
      SqlConditionInSelect c = (SqlConditionInSelect)criteria;
      //do nothing now.
    } else if (criteria instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria)criteria;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (l instanceof SqlColumn) {
        l = resolveColumnAlias((SqlColumn) l, primitiveColumns);
      }
      if (r instanceof SqlColumn) {
        r = resolveColumnAlias((SqlColumn) r, primitiveColumns);
      }
      resovled = new SqlCriteria(l, c.getOperator(), c.getCustomOp(), r);
    }
    return resovled;
  }

  private static boolean needResolveNestedQueryAliasInMainQuery(SqlQueryStatement nestedQuery) {
    //SELECT ... FROM (SELECT ... FROM A JOIN B JOIN C) NESTED_ALIAS WHERE NESTED_ALIAS.xxxx GROUP BY NESTED_ALIAS.xxxxx HAVING NESTED_ALIAS.xxxxx ORDER BY NESTED_ALIAS.xxxxx
    return nestedQuery != null && nestedQuery.getJoins().size() > 0;
  }

  private static SqlCollection<SqlColumn> populateNestedColumns2Main(SqlTable nestedTable) throws Exception {
    SqlCollection<SqlColumn> populateColumns = new SqlCollection<SqlColumn>();
    SqlCollection<SqlColumn> nestedColumns = nestedTable.getQuery().getColumns();
    for (SqlColumn nc : nestedColumns) {
      SqlColumn pcolumn = nc;
      if (!nestedTable.getQuery().isJoinQuery()) {
        SqlTable outerOwner = new SqlTable(nestedTable.getQuery().getTable().getAlias(), nestedTable.getAlias());
        SqlExprModifier modifier = new NestedExprPopulateModifier(outerOwner);
        modifier.modify(nc);
        SqlExpression expr = (SqlColumn) modifier.getModifiedExpr();
        if (expr != null) {
          pcolumn = (SqlColumn) expr;
        }
      } else {
        //do nothing now
      }
      populateColumns.add(pcolumn);
    }
    return populateColumns;
  }

  private static SqlConditionNode populateNestedWhere2Main(SqlConditionNode mainWhere, SqlConditionNode nestedWhere) throws Exception {
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

  private static SqlConditionNode populateNestedHaving2Main(SqlConditionNode mainHaving, SqlConditionNode nestedHaving) throws Exception {
    SqlConditionNode resolved = mainHaving;
    if (nestedHaving != null) {
      if (mainHaving != null) {
        resolved = new SqlCondition((SqlExpression) mainHaving.clone(), SqlLogicalOperator.And, nestedHaving);
      } else {
        resolved = nestedHaving;
      }
    }
    return resolved;
  }

  private static SqlCollection<SqlColumn> resolveMainQueryColumns(SqlCollection<SqlColumn> mainColumns,
                                                                  SqlTable sourceNestedTable) throws Exception {
    SqlCollection<SqlColumn> newMainColumns = new SqlCollection<SqlColumn>();
    for (int i = 0; i < mainColumns.size(); ++i) {
      SqlColumn mc = mainColumns.get(i);
      boolean match = false;
      if (mc.getTable() != null) {
        String owner = Utilities.toUpperCase(mc.getTable().getAlias());
        if (Utilities.equalIgnoreCaseInvariant(owner, sourceNestedTable.getAlias())) {
          match = true;
        }
      } else {
        match = true;
      }
      if (match) {
        if (mc instanceof SqlWildcardColumn) {
          SqlCollection<SqlColumn> populateColumns = populateNestedColumns2Main(sourceNestedTable);
          newMainColumns.addAll(populateColumns);
        } else {
          MainColumnModifier mainColumnModifier = new MainColumnModifier(sourceNestedTable);
          mainColumnModifier.modify(mc);
          mc = (SqlColumn) mainColumnModifier.getModifiedExpr();
          newMainColumns.add(mc);
        }
      } else {
        newMainColumns.add(mc);
      }
    }
    return newMainColumns;
  }

  private static boolean isSimpleTable(SqlTable table) {
    return !table.isNestedQueryTable() && !table.isNestedJoinTable() && !table.hasJoin();
  }

  private static SqlColumn resovleTableAliasInMainQueryColumn(SqlColumn mc, SqlCollection<SqlTable> flatten) throws Exception {
    SqlColumn resovledColumn = mc;
    if (mc instanceof SqlGeneralColumn && mc.getTable() != null) {
      SqlTable tableAlias = mc.getTable();
      SqlTable ownerOfColumn = matchOwnerOfColumn(mc, flatten);
      if (ownerOfColumn != null && ownerOfColumn != tableAlias) {
        SqlTable attachTable = ownerOfColumn;
        if (mc instanceof SqlWildcardColumn) {
          resovledColumn = new SqlWildcardColumn(attachTable);
        } else {
          resovledColumn = mc.hasAlias() ? new SqlGeneralColumn(attachTable, mc.getColumnName(), mc.getAlias()) : new SqlGeneralColumn(attachTable, mc.getColumnName());
        }
      }
    } else if (mc instanceof SqlFormulaColumn) {
      SqlFormulaColumn fc = (SqlFormulaColumn)mc;
      for (int i = 0 ; i < fc.getParameters().size(); ++i) {
        SqlExpression p = fc.getParameters().get(i);
        if (p instanceof SqlGeneralColumn) {
          p = resovleTableAliasInMainQueryColumn((SqlColumn) p, flatten);
          fc.getParameters().set(i, p);
        }
      }
    } else if (mc instanceof SqlOperationColumn) {
      SqlOperationColumn oc = (SqlOperationColumn)mc;
      SqlOperationExpression oe = (SqlOperationExpression)oc.getExpr();
      SqlExpression l = oe.getLeft();
      SqlExpression r = oe.getRight();
      if (l instanceof SqlColumn) {
        l = resovleTableAliasInMainQueryColumn((SqlColumn)l, flatten);
      }
      if (r instanceof SqlColumn) {
        r = resovleTableAliasInMainQueryColumn((SqlColumn)r, flatten);
      }
      resovledColumn = new SqlOperationColumn(oc.getAlias(), new SqlOperationExpression(oe.getOperator(), l, r));
    }
    return resovledColumn;
  }

  private static boolean isStandardQuery(SqlStatement query) {
    boolean isStandard = true;
    if (query.getColumns().size() == 0) {
      isStandard = false;
    }
    if (query.getTable() == null) {
      isStandard = false;
    }
    return isStandard;
  }

  private static boolean isJoinQuery(SqlQueryStatement query) {
    return query != null && query.getJoins().size() > 0 ? true : false;
  }

  private static boolean containsAggragation(SqlCollection<SqlColumn> columns) {
    boolean containsAgg = false;
    for (SqlColumn column : columns) {
      boolean isAgg = containsAggragation(column);
      if (isAgg) {
        containsAgg = true;
        break;
      }
    }
    return containsAgg;
  }

  public static boolean containsAggragation(SqlExpression expr) {
    boolean containsAggragation = false;
    if (expr instanceof SqlFormulaColumn) {
      SqlFormulaColumn formula = (SqlFormulaColumn)expr;
      SqlCollection<SqlExpression> paras = formula.getParameters();
      boolean isAgg = SqlUtilities.isKnownAggragation(formula.getColumnName());
      if (!isAgg) {
        for (SqlExpression para : paras) {
          isAgg = containsAggragation(para);
          if (isAgg) {
            containsAggragation = true;
            break;
          }
        }
      } else {
        //MAX(1), SUM(1),...etc.
        boolean isConstantAgg = false;
        if (1 == paras.size() && paras.get(0) instanceof SqlValueExpression) {
          isConstantAgg = true;
        }
        if (!isConstantAgg) {
          containsAggragation = true;
        }
      }
    }
    return containsAggragation;
  }

  private static void throwNoSupportNormalizationException(String code, SqlStatement statement) throws Exception {
    Dialect dialect = statement.getDialectProcessor();
    SqlBuilder builder;
    if (null != dialect) {
      builder = SqlBuilder.createBuilder(statement.getDialectProcessor());
    } else {
      builder = SqlBuilder.createBuilder();
    }
    throw SqlExceptions.Exception(code, SqlExceptions.NO_SUPPORT_SQL_NORMALIZATION, builder.build(statement));
  }

  private static ArrayList<NormalizationOptions> buildConfigedOptions(NormalizationOption option) {
    ArrayList<NormalizationOptions> configed = new ArrayList<NormalizationOptions>();
    if (option.normalizeImplicitCommaJoin()) {
      configed.add(NormalizationOptions.ImplicitCommaJoin);
    } else if (option.normalizeImplicitNaturalJoin()) {
      configed.add(NormalizationOptions.ImplicitNaturalJoin);
    } else if (option.normalizeImplicitCrossJoin()) {
      configed.add(NormalizationOptions.ImplicitCrossJoin);
    } else if (option.normalizeImplicitInnerJoin()) {
      configed.add(NormalizationOptions.ImplicitInnerJoin);
    }

    if (option.normalizeNestedQueryTableName()) {
      configed.add(NormalizationOptions.AppendNestedQueryTableName);
    }

    if (option.normalizeRightJoin()) {
      configed.add(NormalizationOptions.RightJoin);
    }

    if (option.normalizeNestedJoin()) {
      configed.add(NormalizationOptions.NestedJoin);
    }

    if (option.normalizeEquiInnerJoin()) {
      configed.add(NormalizationOptions.EquiInnerJoin);
    }

    if (option.normalizeOperationExpression()) {
      configed.add(NormalizationOptions.OperationExpression);
    }

    if (option.normalizeRemoveDistinctIfColumnUnique()) {
      configed.add(NormalizationOptions.RemoveDistinctIfColumnUnique);
    }

    if (option.normalizeTableExprWithQuery()) {
      configed.add(NormalizationOptions.TableExprWithQuery);
    }

    if (option.normalizeConstantColumn()) {
      configed.add(NormalizationOptions.ConstantColumn);
    }

    if (option.normalizeDistinct()) {
      configed.add(NormalizationOptions.Distinct);
    }

    if (option.normalizeCountDistinct()) {
      configed.add(NormalizationOptions.Count_Distinct);
    }

    if (option.normalizeCriteriaInJoin()) {
      configed.add(NormalizationOptions.CriteriaInJoin);
    }

    if (option.normalizeCriteriaWithSubQuery()) {
      configed.add(NormalizationOptions.CriteriaWithSubQuery);
    }

    if (option.normalizedSemiAntiJoin()) {
      configed.add(NormalizationOptions.SemiAntiJoin);
    }

    if (option.normalizePredictTrue()) {
      configed.add(NormalizationOptions.PredictTrue);
    }

    if (option.normalizeCriteriaWithNot()) {
      configed.add(NormalizationOptions.CriteriaWithNot);
    }

    if (option.normalizeCriteria()) {
      configed.add(NormalizationOptions.Criteria);
    }

    if (option.normalizeMinimizeCriteria()) {
      configed.add(NormalizationOptions.MinimizeCriteria);
    }

    if (option.normalizeFormulaAlias()) {
      configed.add(NormalizationOptions.FormulaAlias);
    }

    if (option.normalizeFixUniqueAlias()) {
      configed.add(NormalizationOptions.FixUniqueAlias);
    }

    if (option.normalizedFunctionSubstitute()) {
      configed.add(NormalizationOptions.FunctionSubstitute);
    }

    if (option.normalizeResolveTableAlias()) {
      configed.add(NormalizationOptions.ResolveTableAlias);
    }
    return configed;
  }

}

final class FunctionModifier extends SqlModifier {
  private final ArrayList<IFunctionSubstitute> _functions = new ArrayList<IFunctionSubstitute>();
  public FunctionModifier() {
    this._functions.add(new NULLIFSubstitue());
    this._functions.add(new COALESCESubstitue());
    this._functions.add(new ATATVariableSubstitue());
    this._functions.add(new IIFSubstitue());
  }

//@
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    for (IFunctionSubstitute fs : this._functions) {
      if (fs.match(element)) {
        element = (SqlColumn) fs.substitute(element);
      }
    }
    return super.visit(element);
  }
}

final class ColumnOwnerModifier extends SqlModifier {
  private final SqlCollection<SqlTable> _tables;
  private final IDataMetadata _dataMeta;
  private Exception _error = null;
  public ColumnOwnerModifier(SqlQueryStatement query, IDataMetadata dataMeta) throws Exception {
    this._tables = SqlUtilities.getTables(query, new SourceTableMatcher());
    this._dataMeta = dataMeta;
  }

//@
  @Override
  protected SqlTable visit(SqlTable element) throws Exception {
//@
/*#
  protected override SqlTable Visit(SqlTable element) {
#*/
    return element;
  }

//@
  @Override
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    if (element.getTable() != null) return element;

    if (!(element instanceof SqlGeneralColumn)) return super.visit(element);

    if (element instanceof SqlWildcardColumn) {
      return element;
    }

    SqlGeneralColumn issuedColumn = (SqlGeneralColumn) element;

    if (null == this._dataMeta) {
      return issuedColumn;
    }

    SqlCollection<SqlTable> matched = new SqlCollection<SqlTable>();
    for (SqlTable t : this._tables) {
      ColumnInfo[] columns = this._dataMeta.getTableMetadata(t.getCatalog(), t.getSchema(), t.getName());
      if (columns == null || columns.length == 0) continue;
      for (ColumnInfo c : columns) {
        if (Utilities.equalIgnoreCase(c.getColumnName(), issuedColumn.getColumnName())) {
          matched.add(t);
          break;
        }
      }
    }

    if (matched.size() > 1) {
      if (this._error == null) {
        this._error = SqlExceptions.Exception(null, SqlExceptions.AMBIGUOUS_COLUMN_IN_FIELDS_LIST, element.getColumnName());
      }
      return element;
    } else if (matched.size() == 0) {
      if (this._error == null) {
        this._error = SqlExceptions.Exception(null, SqlExceptions.UNKNOWN_COLUMN_IN_FIELDS_LIST, element.getColumnName());
      }
      return element;
    }

    SqlTable owner = matched.get(0);

    if (issuedColumn.hasAlias()) {
      issuedColumn = new SqlGeneralColumn(new SqlTable(owner.getAlias()), issuedColumn.getColumnName(), issuedColumn.getAlias());
    } else {
      issuedColumn = new SqlGeneralColumn(new SqlTable(owner.getAlias()), issuedColumn.getColumnName());
    }
    return issuedColumn;
  }

  public Exception getError() {
    return this._error;
  }

  class SourceTableMatcher implements ITableMatch {

    public SqlTable create(SqlTable t) {
      return new SqlTable(t.getCatalog(),
          t.getSchema(),
          t.getName(),
          t.getAlias());
    }

    public boolean accept(SqlTable t, TablePartType type) {
      if (TablePartType.SimpleTable == type) return true;

      return (TablePartType.JoinPart == type &&
          (null == t.getQuery() && null == t.getNestedJoin()));
    }

    public boolean unwind(SqlTable t, TablePartType type) {
      return false;
    }
  }
}

class OperationColumnModifier extends SqlModifier {
  private static boolean isOperationExpression(SqlExpression expr) {
    boolean isOperation = false;
    if (expr instanceof SqlOperationColumn) {
      isOperation = true;
    } else if (expr instanceof SqlOperationExpression) {
      isOperation = true;
    } else if (expr instanceof SqlFormulaColumn) {
      SqlFormulaColumn formula = (SqlFormulaColumn) expr;
      for (SqlExpression para : formula.getParameters()) {
        if (para == null) continue;
        isOperation = isOperationExpression(para);
        if (isOperation) break;
      }
    } else if (expr instanceof SqlConditionNode) {
      isOperation = containOperationExpr((SqlConditionNode) expr);
    }
    return isOperation;
  }

  private static boolean containOperationExpr(SqlConditionNode selector) {
    boolean contains = false;
    if (selector instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria)selector;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (isOperationExpression(l) || isOperationExpression(r)) {
        contains = true;
      }
    } else if (selector instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)selector;
      if (c.getLeft() instanceof SqlConditionNode) {
        contains = containOperationExpr((SqlConditionNode)c.getLeft());
      }
      if (!contains && c.getRight() instanceof SqlConditionNode) {
        contains = containOperationExpr((SqlConditionNode)c.getRight());
      }
    }
    return contains;
  }

  private static SqlConditionNode resolveOperationSelector(SqlConditionNode selector) throws Exception {
    SqlConditionNode resolved = selector;
    if (selector instanceof SqlCriteria) {
      SqlCriteria criteria = (SqlCriteria)selector;
      SqlExpression l = criteria.getLeft();
      if (isOperationExpression(l)) {
        OperationExprModifier modifier = new OperationExprModifier();
        modifier.modify(l);
        l = modifier.getModifiedExpr();
      }
      SqlExpression r = criteria.getRight();
      if (isOperationExpression(r)) {
        OperationExprModifier modifier = new OperationExprModifier();
        modifier.modify(r);
        r = modifier.getModifiedExpr();
      }
      resolved = new SqlCriteria(l, criteria.getOperator(), criteria.getCustomOp(), r, criteria.getEscape());
    } else if (selector instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition) selector;
      SqlExpression l = condition.getLeft();
      if (l instanceof SqlConditionNode) {
        if (containOperationExpr((SqlConditionNode)l)) {
          l = resolveOperationSelector((SqlConditionNode)l);
        }
      }
      SqlExpression r = condition.getRight();
      if (r instanceof SqlConditionNode) {
        if (containOperationExpr((SqlConditionNode)r)) {
          r = resolveOperationSelector((SqlConditionNode)r);
        }
      }
      resolved = new SqlCondition(l, condition.getLogicOp(), r);
    }
    return resolved;
  }
//@
  @Override
  protected SqlConditionNode visit(SqlConditionNode element) throws Exception {
//@
/*#
  protected override SqlConditionNode Visit(SqlConditionNode element) {
#*/
    if (containOperationExpr(element)) {
      element = resolveOperationSelector(element);
    }
    return element;
  }
//@
  @Override
  protected SqlTable visit(SqlTable element) throws Exception {
//@
/*#
  protected override SqlTable Visit(SqlTable element) {
#*/
    SqlQueryStatement nestedQuery = element.getQuery();
    SqlTable nestedJoin = element.getNestedJoin();
    SqlJoin join = element.getJoin();
    boolean modified = false;
    if (nestedQuery != null) {
      SqlModifier modifier = new OperationColumnModifier();
      modified = modifier.modify(new SqlParser(nestedQuery));
    }

    if (nestedJoin != null) {
      SqlTable modifiedNJ = visit(nestedJoin);
      if (modifiedNJ != nestedJoin) {
        nestedJoin = modifiedNJ;
        modified = true;
      }
    }
    if (join != null) {
      SqlTable modifiedR = visit(join.getTable());
      SqlConditionNode modifiedOn = visit(join.getCondition());
      if (modifiedR != join.getTable() || modifiedOn != join.getCondition()) {
        join = new SqlJoin(join.getJoinType(),
                modifiedR,
                modifiedOn,
                join.isEach(),
                join.hasOuter());
        modified = true;
      }
    }
    if (modified) {
      element = new SqlTable(element.getCatalog(),
              element.getSchema(),
              element.getName(),
              element.getAlias(),
              join,
              nestedJoin,
              nestedQuery,
              element.getTableValueFunction(),
              element.getCrossApply());
    }
    return element;
  }
//@
  @Override
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    if (isOperationExpression(element)) {
      OperationExprModifier modifier = new OperationExprModifier();
      modifier.modify(element);
      return (SqlColumn) modifier.getModifiedExpr();
    } else if (element instanceof SqlFormulaColumn && Utilities.equalIgnoreCase("IF", element.getColumnName())) {
      SqlFormulaColumn IF = (SqlFormulaColumn) element;
      SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
      paras.add(null);
      for (SqlExpression p : IF.getParameters()) {
        paras.add(p);
      }
      SqlFormulaColumn caseWhenF = new SqlFormulaColumn("CASE", IF.getAlias(), paras);
      return caseWhenF;
    } else if (SqlNormalization.containsAggragation(element)) {
      SqlFormulaColumn agg = (SqlFormulaColumn) element;
      if (1 == agg.getParameters().size()
              && agg.getParameters().get(0) instanceof SqlFormulaColumn) {
        SqlFormulaColumn negate = (SqlFormulaColumn) agg.getParameters().get(0);
        if (Utilities.equalIgnoreCase("NEGATE", negate.getColumnName())) {
          SqlExpression expr = negate.getParameters().get(0);
          if (Utilities.equalIgnoreCase("MIN", agg.getColumnName())) {
            agg = new SqlFormulaColumn("MAX", agg.getAlias(), agg.getParameters());
            agg.getParameters().set(0, expr);
          } else if (Utilities.equalIgnoreCase("MAX", agg.getColumnName())) {
            agg = new SqlFormulaColumn("MIN", agg.getAlias(), agg.getParameters());
            agg.getParameters().set(0, expr);
          } else {
            agg.getParameters().set(0, expr);
          }
          negate.getParameters().set(0, agg);
          negate = new SqlFormulaColumn("NEGATE", agg.getAlias(), negate.getParameters());
          return negate;
        }
        return super.visit(element);
      }
    }
    return element;
  }

//@
  @Override
  protected SqlExpression visit(SqlExpression element) throws Exception {
//@
/*#
  protected override SqlExpression Visit(SqlExpression element) {
#*/
    if (isOperationExpression(element)) {
      OperationExprModifier modifier = new OperationExprModifier();
      modifier.modify(element);
      return modifier.getModifiedExpr();
    }
    return super.visit(element);
  }
}

class MainOwnerColumnsModifier extends SqlModifier {
  private Hashtable<String, SqlCollection<SqlColumn>> _tAlias2NestedColumnsMap;
  private SqlCollection<SqlColumn> _originalMainColumns;
  public MainOwnerColumnsModifier(SqlCollection<SqlColumn> originalMainColumns, Hashtable<String, SqlCollection<SqlColumn>> tAlias2NestedColumnsMap) {
    this._originalMainColumns = originalMainColumns;
    this._tAlias2NestedColumnsMap = tAlias2NestedColumnsMap;
  }

//@
  protected SqlTable visit(SqlTable element) throws Exception {
//@
/*#
  protected override SqlTable Visit(SqlTable element) {
#*/
    if (element == null) return element;

    if (element.hasJoin()) {
      SqlJoin j = element.getJoin();
      SqlJoin tempJ = this.visit(j);
      if (j != tempJ) {
        element = new SqlTable(element.getCatalog(),
            element.getSchema(),
            element.getName(),
            element.getAlias(),
            tempJ,
            element.getNestedJoin(),
            element.getQuery(),
            element.getTableValueFunction(),
            element.getCrossApply());
      }
    }
    return element;
  }

  //@
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    if (!(element instanceof SqlGeneralColumn)){
      return super.visit(element);
    }
    SqlGeneralColumn gc = (SqlGeneralColumn) element;
    SqlGeneralColumn omc = (SqlGeneralColumn) element;
    for (SqlColumn om : this._originalMainColumns) {
      if (om instanceof SqlFormulaColumn) continue;
      if (Utilities.equalIgnoreCase(om.getAlias(), element.getAlias())) {
        omc = (SqlGeneralColumn) om;
        break;
      }
    }
    if (gc.getTable() != null) {
      String alias = gc.getTable().getAlias();
      String KEY = Utilities.toUpperCase(alias);
      if (this._tAlias2NestedColumnsMap.containsKey(KEY)) {
        SqlColumn innerColumn = getMappingNestedColumn(omc, this._tAlias2NestedColumnsMap.get(KEY));
        if (innerColumn != null && innerColumn.getTable() != null) {
          if (this._clause == CLAUSE_TYPE.COLUMNS) {
            element = gc.hasAlias() ? new SqlGeneralColumn(innerColumn.getTable(), innerColumn.getColumnName(), gc.getAlias()) : new SqlGeneralColumn(innerColumn.getTable(), innerColumn.getColumnName());
          } else if (this._clause ==CLAUSE_TYPE.CRITERIA
            || this._clause ==CLAUSE_TYPE.HAVING){
            element = new SqlGeneralColumn(innerColumn.getTable(), innerColumn.getColumnName());
          } else {
            element = new SqlGeneralColumn(gc.getAlias());
          }
        }
      }
    }
    return element;
  }

//@
  protected SqlOrderSpec visit(SqlOrderSpec element) throws Exception {
//@
/*#
  protected override SqlOrderSpec Visit(SqlOrderSpec element) {
#*/
    if (this._clause == CLAUSE_TYPE.ORDERBY) {
      if (element.getExpr() instanceof SqlColumn) {
        SqlColumn column = visit((SqlColumn)element.getExpr());
        element = new SqlOrderSpec(column, element.getOrder(), element.isNullsFirst(), element.hasNulls());
      }
    }
    return element;
  }

  private SqlColumn getMappingNestedColumn(SqlColumn mc, SqlCollection<SqlColumn> nestedColumns) {
    SqlColumn mappingColumn = null;
    for (SqlColumn nc : nestedColumns) {
      if (Utilities.equalIgnoreCaseInvariant(mc.getColumnName(), nc.getAlias())) {
        mappingColumn = nc;
        break;
      }
    }
    return mappingColumn;
  }
}

abstract class SqlExprModifier {
  protected SqlExpression _originalExpr;
  protected EXPR_BELONGS _exprBelongs;
  private SqlExpression _modifiedExpr;
  public enum EXPR_BELONGS { NONE, COLUMNS, FORMULA, CRITERIA }
  protected SqlExprModifier() {
    this._exprBelongs = EXPR_BELONGS.NONE;
  }

  protected boolean modify(SqlExpression expr) throws Exception {
    this._originalExpr = expr;
    this._modifiedExpr = visit(this._originalExpr);
    return (this._modifiedExpr != this._originalExpr);
  }

  protected SqlExpression getModifiedExpr() {
    return this._modifiedExpr;
  }

  protected /*#virtual#*/ SqlExpression visit(SqlExpression element) throws Exception{
    SqlExpression modified = element;
    if (element instanceof SqlColumn) {
      if (this._exprBelongs == EXPR_BELONGS.NONE) {
        this._exprBelongs = EXPR_BELONGS.COLUMNS;
      }
      modified = visit((SqlColumn) element);
    } else if (element instanceof SqlConditionNode) {
      if (this._exprBelongs == EXPR_BELONGS.NONE) {
        this._exprBelongs = EXPR_BELONGS.CRITERIA;
      }
      modified = visit((SqlConditionNode) element);
    }
    return modified;
  }

  protected /*#virtual#*/ SqlColumn visit(SqlColumn element) throws Exception{
    SqlColumn modified = element;
    if (element instanceof SqlGeneralColumn) {
      modified = visit((SqlGeneralColumn) element);
    } else if (element instanceof SqlFormulaColumn) {
      modified = visit((SqlFormulaColumn) element);
    } else if (element instanceof SqlOperationColumn) {
      modified = visit((SqlOperationColumn) element);
    } else if (element instanceof SqlConstantColumn) {
      modified = visit((SqlConstantColumn) element);
    } else if (element instanceof SqlSubQueryColumn) {
      modified = visit((SqlSubQueryColumn)element);
    }
    return modified;
  }

  protected /*#virtual#*/ SqlColumn visit(SqlGeneralColumn element) throws Exception {
    return element;
  }

  protected /*#virtual#*/ SqlColumn visit(SqlFormulaColumn element) throws Exception {
    SqlCollection<SqlExpression> modifiedParas = new SqlCollection<SqlExpression>();
    EXPR_BELONGS belonged = this._exprBelongs;
    this._exprBelongs = EXPR_BELONGS.FORMULA;
    for (SqlExpression para : element.getParameters()) {
      SqlExpression modifiedPara = visit(para);
      modifiedParas.add(modifiedPara);
    }
    this._exprBelongs = belonged;
    SqlColumn modified = new SqlFormulaColumn(element.getColumnName(), element.getAlias(), modifiedParas);
    return modified;
  }

  protected /*#virtual#*/ SqlColumn visit(SqlOperationColumn element) throws Exception {
    SqlOperationExpression opExpr = (SqlOperationExpression) element.getExpr();
    SqlOperationExpression modifiedExpr = opExpr;
    SqlExpression left = opExpr.getLeft();
    SqlExpression right = opExpr.getRight();
    SqlExpression modifiedLeft = visit(left);
    SqlExpression modifiedRight = visit(right);
    if (modifiedLeft != left || modifiedRight != right) {
      modifiedExpr = new SqlOperationExpression(opExpr.getOperator(), modifiedLeft, modifiedRight);
    }
    if (modifiedExpr != opExpr) {
      return new SqlOperationColumn(element.getAlias(), modifiedExpr);
    } else {
      return element;
    }
  }

  protected /*#virtual#*/ SqlColumn visit(SqlSubQueryColumn element) throws Exception {
    return element;
  }

  protected /*#virtual#*/ SqlColumn visit(SqlConstantColumn element) throws Exception {
    return element;
  }

  protected /*#virtual#*/ SqlConditionNode visit(SqlConditionNode element) throws Exception {
    if (element instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition) element;
      SqlExpression left = condition.getLeft();
      SqlExpression ml = visit(left);
      SqlExpression right = condition.getRight();
      SqlExpression mr = visit(right);
      if (ml != left || mr != right) {
        element = new SqlCondition(ml, condition.getLogicOp(), mr);
        return element;
      }
    } else if (element instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria) element;
      SqlExpression left = c.getLeft();
      SqlExpression ml = visit(left);
      SqlExpression right = c.getRight();
      SqlExpression mr = visit(right);
      if (ml != left || mr != right) {
        element = new SqlCriteria(ml, c.getOperator(), c.getCustomOp(), mr, c.getEscape());
        return element;
      }
    } else if (element instanceof SqlConditionNot) {
      SqlConditionNot Not = (SqlConditionNot) element;
      SqlExpression expr = visit(Not.getCondition());
      if (expr != Not.getCondition()) {
        return new SqlConditionNot(expr);
      }
    }
    return element;
  }
}

class PredictTrueModifier extends SqlExprModifier {
  @Override
  protected SqlConditionNode visit(SqlConditionNode element) throws Exception {
    if (element instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition) element;
      SqlExpression left = condition.getLeft();
      if (left instanceof SqlConditionNode) {
        left = visit((SqlConditionNode) left);
      } else {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        left = new SqlCriteria(left, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
      }
      SqlExpression right = condition.getRight();
      if (right instanceof SqlConditionNode) {
        right = visit((SqlConditionNode) right);
      } else {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        right = new SqlCriteria(right, ComparisonType.IS, SqlCriteria.CUSTOM_OP_PREDICT_TRUE, new SqlValueExpression(trueValue));
      }
      if (left != condition.getLeft() || right != condition.getRight()) {
        element = new SqlCondition(left, condition.getLogicOp(), right);
        return element;
      }
    } else if (element instanceof SqlConditionNot) {
      SqlConditionNot not = (SqlConditionNot) element;
      SqlExpression expr = not.getCondition();
      if (!(expr instanceof SqlConditionNode)) {
        SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        return new SqlConditionNot(new SqlCriteria(expr,
                ComparisonType.IS,
                SqlCriteria.CUSTOM_OP_PREDICT_TRUE,
                new SqlValueExpression(trueValue)));
      }
    } else {
      return super.visit(element);
    }
    return element;
  }
}

class MainColumnModifier extends SqlExprModifier {
  SqlTable _sourceNestedTable;
  protected MainColumnModifier(SqlTable sourceNestedTable) {
    super();
    this._sourceNestedTable = sourceNestedTable;
  }

  @Override
  protected SqlColumn visit(SqlGeneralColumn element) throws Exception {
    SqlColumn modified = element;
    SqlColumn mappingColumn = SqlNormalizationHelper.getMappingNestedColumn(element, this._sourceNestedTable.getQuery().getColumns());
    if (element instanceof SqlWildcardColumn) {
      if (this._exprBelongs == EXPR_BELONGS.FORMULA) {
        //do nothing.
      } else {
        modified = populate2MainColumn(mappingColumn,
            element,
            this._sourceNestedTable.getQuery().getTable().hasJoin() ? null : element.getTable());
      }
    } else if (mappingColumn != null) {
      modified = populate2MainColumn(mappingColumn,
          element,
          this._sourceNestedTable.getQuery().getTable().hasJoin() ? null : element.getTable());
      if (this._exprBelongs == EXPR_BELONGS.FORMULA) {
        if (modified.hasAlias()) {
          if (modified instanceof SqlGeneralColumn) {
            // There are 2 cases, those columns are as following,
            // 1. SELECT q.c1, sum(q.c1) FROM (SELECT c c1 FROM ...) q
            //    q.c1 -> q.c c1
            //    sum(q.c1) -> sum(q.c c1) -> sum(q.c)
            // 2. SELECT q.c1, sum(q.c1) FROM (SELECT t2.c c1,t1.c c2 FROM t1 JOIN t2 ...) q
            //    q.c1 -> q.c1 c1 -> t2.c c1
            //    sum(q.c1) -> sum(q.c c1) -> sum(q.c1 c1) -> sum(t2.c)
            SqlQueryStatement nq = this._sourceNestedTable.getQuery();
            if (!nq.isJoinQuery()) {
              modified = new SqlGeneralColumn(modified.getTable(),
                modified.getColumnName());
            } else {
              modified = new SqlGeneralColumn(modified.getTable(),
                modified.getAlias());
            }
          } else if(modified instanceof SqlFormulaColumn) {
            modified = new SqlFormulaColumn(modified.getColumnName(),
                ((SqlFormulaColumn) modified).getParameters(),
                ((SqlFormulaColumn) modified).getOverClause());
          }
        }
      }
    }
    return modified;
  }

  @Override
  protected SqlConditionNode visit(SqlConditionNode element) throws Exception {
    SqlConditionNode modified = element;
    if (element instanceof SqlCondition) {
      SqlCondition mc = (SqlCondition) element;
      SqlExpression left = visit(mc.getLeft());
      SqlExpression right = visit(mc.getRight());
      modified = new SqlCondition(left, mc.getLogicOp(), right);
    } else if (element instanceof SqlCriteria) {
      SqlCriteria mc = (SqlCriteria) element;
      SqlExpression left = visit(mc.getLeft());
      SqlExpression right = visit(mc.getRight());
      modified = new SqlCriteria(left, mc.getOperator(), mc.getCustomOp(), right, mc.getEscape());
    } else if (element instanceof SqlConditionNot) {
      SqlConditionNot  cNot = (SqlConditionNot) element;
      modified = new SqlConditionNot(visit(cNot.getCondition()));
    } else if (element instanceof SqlConditionExists) {
      //do nothing.
    } else if (element instanceof SqlConditionInSelect) {
      //do nothing.
    }
    return modified;
  }

  private static SqlColumn populate2MainColumn(SqlColumn c, SqlColumn m, SqlTable ow) throws Exception {
    SqlColumn resovled = c;
    if (c instanceof SqlGeneralColumn) {
      if (m != null) {
        if (m instanceof SqlWildcardColumn) {
          if (c instanceof SqlWildcardColumn) {
            //do nothing.
          } else {
            if (m.getTable() != null) {
              SqlTable owner =  m.getTable();
              if (c.getTable() != null) {
                SqlTable t1 = c.getTable();
                SqlTable t2 = m.getTable();
                if (!Utilities.equalIgnoreCase(t1.getValidName(), t2.getValidName())) {
                  owner = c.getTable();
                }
              }
              if (c.hasAlias()) {
                resovled = new SqlGeneralColumn(owner, c.getColumnName(), c.getAlias());
              } else {
                resovled = new SqlGeneralColumn(owner, c.getColumnName());
              }
            } else {
              resovled = c;
            }
          }
        } else {
          SqlTable t = null;
          if (c.getTable() == null) {
            t = m.getTable();
          } else {
            t = m.getTable();
          }
          if (m.hasAlias()) {
            resovled = new SqlGeneralColumn(t, c.getColumnName(), m.getAlias());
          } else {
            boolean hasAlias = true;
            String n1 = c.getColumnName();
            String n2 = m.getColumnName();
            if (n1.equals(n2)) {
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
        resovled = (SqlColumn) c.clone();
      }
    } else if (c instanceof SqlFormulaColumn) {
      SqlFormulaColumn fc = (SqlFormulaColumn)c;
      if (m != null) {
        SqlCollection<SqlExpression> popluateParas = new SqlCollection<SqlExpression>();
        NestedExprPopulateModifier populateModifier = new NestedExprPopulateModifier(ow);
        for (SqlExpression p : fc.getParameters()) {
          populateModifier.modify(p);
          SqlExpression populatePara = populateModifier.getModifiedExpr();
          popluateParas.add(populatePara);
        }
        resovled = new SqlFormulaColumn(c.getColumnName(), m.getAlias(), popluateParas);
      } else {
        resovled = (SqlColumn)c.clone();
      }
    } else if (c instanceof SqlConstantColumn) {
      if (m != null) {
        resovled = new SqlConstantColumn(m.getAlias(), (SqlValueExpression)c.getExpr());
      } else {
        resovled = (SqlColumn)c.clone();
      }
    } else if (c instanceof SqlOperationColumn) {
      if (m != null) {
        NestedExprPopulateModifier populateModifier = new NestedExprPopulateModifier(m.getTable());
        populateModifier.modify(c);
        SqlOperationColumn modified = (SqlOperationColumn) populateModifier.getModifiedExpr();
        resovled = new SqlOperationColumn(m.getAlias(), (SqlOperationExpression) modified.getExpr());
      } else {
        resovled = (SqlColumn)c.clone();
      }
    } else if (c instanceof SqlSubQueryColumn) {
      if (m != null) {
        resovled = new SqlSubQueryColumn(m.getAlias(), (SqlSubQueryExpression)c.getExpr());
      } else {
        resovled = (SqlColumn)c.clone();
      }
    }
    return resovled;
  }
}

class NestedExprPopulateModifier extends SqlExprModifier {
  private SqlTable _outerOwner;
  protected NestedExprPopulateModifier(SqlTable outerOwner) {
    this._outerOwner = outerOwner;
  }

  @Override
  protected SqlColumn visit(SqlGeneralColumn element) throws Exception {
    SqlColumn modified = element;
    SqlTable outerOwner = this._outerOwner != null ? new SqlTable(this._outerOwner.getAlias()) : element.getTable();

    if (element instanceof SqlWildcardColumn) {
      modified = new SqlWildcardColumn(outerOwner);
      return modified;
    }

    if (element.hasAlias()) {
      modified = new SqlGeneralColumn(outerOwner, element.getColumnName(), element.getAlias());
    } else {
      modified = new SqlGeneralColumn(outerOwner, element.getColumnName());
    }
    return modified;
  }
}

class OperationExprModifier extends SqlExprModifier {
  @Override
  protected SqlExpression visit(SqlExpression element) throws Exception {
    if (element instanceof SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) element;
      SqlExpression l = opExpr.getLeft();
      SqlExpression r = opExpr.getRight();
      SqlCollection<SqlExpression> PARAS = new SqlCollection<SqlExpression>();
      PARAS.add(new SqlValueExpression(SqlValueType.STRING, opExpr.getOperatorAsString()));
      PARAS.add(this.visit(l));
      PARAS.add(this.visit(r));
      SqlFormulaColumn EXPR = new SqlFormulaColumn("EXPR", PARAS);
      return EXPR;
    }
    return super.visit(element);
  }

  @Override
  protected SqlColumn visit(SqlOperationColumn element) throws Exception {
    SqlOperationExpression opExpr = (SqlOperationExpression) element.getExpr();
    SqlFormulaColumn EXPR = (SqlFormulaColumn) this.visit(opExpr);
    if (element.hasAlias()) {
      return new SqlFormulaColumn(EXPR.getColumnName(),
          element.getAlias(),
          EXPR.getParameters(),
          EXPR.getOverClause());
    } else {
      return new SqlFormulaColumn(EXPR.getColumnName(),
          EXPR.getParameters(),
          EXPR.getOverClause());
    }
  }

  @Override
  protected SqlColumn visit(SqlFormulaColumn element) throws Exception {
    if (Utilities.equalIgnoreCase("EXPR", element.getColumnName())) {
      if (element.getParameters().size() == 1 && element.getParameters().get(0) instanceof SqlOperationExpression) {
        SqlFormulaColumn modified = (SqlFormulaColumn)this.visit(element.getParameters().get(0));
        return new SqlFormulaColumn(element.getColumnName(),
                element.getAlias(),
                modified.getParameters(),
                element.getOverClause());
      }
    }
    return super.visit(element);
  }
}

class NestedQueryModifier extends SqlModifier {
  private SqlTable _outerOwner;
  public NestedQueryModifier(SqlTable outerOwner) {
   this._outerOwner = outerOwner;
  }

//@
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    if (element instanceof SqlGeneralColumn) {
      SqlColumn c = (SqlColumn) element;
      if (this._clause == CLAUSE_TYPE.CRITERIA ||
              this._clause == CLAUSE_TYPE.HAVING) {
        element = new SqlGeneralColumn(this._outerOwner, c.getColumnName());
      } else if (this._clause == CLAUSE_TYPE.ORDERBY) {
        element = SqlNormalization.resolveColumnAlias(c, this._statement.getColumns());
        element = new SqlGeneralColumn(this._outerOwner, element.getColumnName());
      } else if (this._clause == CLAUSE_TYPE.GROUPBY) {
        element = SqlNormalization.resolveColumnAlias(c, this._statement.getColumns());
        element = new SqlGeneralColumn(this._outerOwner, element.getColumnName());
      }
    } else if (element instanceof SqlOperationColumn) {
      //do nothing now.
    } else if (element instanceof SqlConstantColumn) {
      //do nothing now.
    } else if (element instanceof SqlFormulaColumn) {
      if (this._clause == CLAUSE_TYPE.CRITERIA
          || this._clause == CLAUSE_TYPE.GROUPBY
          || this._clause == CLAUSE_TYPE.ORDERBY) {
        SqlFormulaColumn fc = (SqlFormulaColumn) element;
        SqlCollection<SqlExpression> paras = new SqlCollection<SqlExpression>();
        for (SqlExpression p : fc.getParameters()) {
          paras.add(visit(p));
        }
        element = new SqlFormulaColumn(fc.getColumnName(), paras);
      }
    } else if (element instanceof SqlSubQueryColumn) {
      //do nothing now.
    }
    return element;
  }

//@
  protected SqlOrderSpec visit(SqlOrderSpec element) throws Exception {
//@
/*#
  protected override SqlOrderSpec Visit(SqlOrderSpec element) {
#*/
    if (this._clause == CLAUSE_TYPE.ORDERBY) {
      if (element.getExpr() instanceof SqlColumn) {
        SqlColumn column = visit((SqlColumn)element.getExpr());
        element = new SqlOrderSpec(column, element.getOrder(), element.isNullsFirst(), element.hasNulls());
      }
    }
    return element;
  }
//@
  protected SqlConditionNode visit(SqlConditionNode element) throws Exception {
//@
/*#
  protected override SqlConditionNode Visit(SqlConditionNode element) {
#*/
    if (element instanceof SqlConditionExists) {
      return element;
    } else if (element instanceof SqlConditionInSelect) {
      return element;
    } else {
      return super.visit(element);
    }
  }

//@
  @Override
  protected SqlTable visit(SqlTable element) throws Exception {
//@
/*#
  protected override SqlTable Visit(SqlTable element) {
#*/
    return element;
  }
}

class SqlNormalizationException extends RSBException {
  private static String getDetails(List<Exception> exceptions) {
    StringBuilder builder = new StringBuilder();
    for (Exception ex : exceptions) {
      if (builder.length() > 0) {
        builder.append(Utilities.SYS_LINESEPARATOR);
      }
      builder.append(Utilities.getExceptionMessage(ex));
    }
    return builder.toString();
  }
  public SqlNormalizationException(String code, List<Exception> exceptions) {
    super(code, getDetails(exceptions));
  }
}

class SqlNormalizationHelper {
  protected static final String NO_TABLE_QUERY_TABLE_NAME_PREFIX = "NO_TABLE_QUERY_TABLE";
  protected static SqlColumn getMappingNestedColumn(SqlColumn mc, SqlCollection<SqlColumn> nestedColumns) {
    SqlColumn mappingColumn = null;
    for (SqlColumn nc : nestedColumns) {
      if (mc instanceof SqlWildcardColumn) {
        mappingColumn = nc;
        break;
      }
      if (Utilities.equalIgnoreCaseInvariant(mc.getColumnName(), nc.getAlias())) {
        mappingColumn = nc;
        break;
      }
    }
    return mappingColumn;
  }

  protected static void flattenTables(SqlCollection<SqlTable> flattens, SqlTable main) throws Exception {
    if (main == null) return;
    if (main.isNestedQueryTable()) {
      flattens.add(main);
    } else if (main.isNestedJoinTable()) {
      if (main.hasAlias()) {
        flattens.add(main);
      }
      flattenTables(flattens, main.getNestedJoin());
    } else {
      if (main.hasAlias()) {
        flattens.add(new SqlTable(main.getName(), main.getAlias()));
      } else {
        flattens.add(new SqlTable(main.getName()));
      }
    }
    if (main.hasJoin()) {
      flattenTables(flattens, main.getJoin().getTable());
    }
    // THIS is a hack for Cross Apply.
    // We add each one as a pseudo table so that
    // we correctly resolve prefixes during normalization
    SqlCrossApply ca = main.getCrossApply();
    while ( ca != null ) {
      flattens.add(ca.getPseudoTable());
      ca = ca.getCrossApply();
    }
  }

  protected static SqlCollection<SqlTable> flattenTables(SqlQueryStatement query) throws Exception {
    SqlCollection<SqlTable> flattens = new SqlCollection<SqlTable>();
    if (isImplicitJoin(query)) {
      SqlCollection<SqlTable> implicitTables = ((SqlSelectStatement)query).getTables();
      for (SqlTable iTable : implicitTables) {
        SqlNormalizationHelper.flattenTables(flattens, iTable);
      }
    } else {
      SqlNormalizationHelper.flattenTables(flattens, query.getTable());
    }
    return flattens;
  }

  protected static boolean isImplicitJoin(SqlQueryStatement query) {
    boolean implicitJoin = false;
    if (query instanceof SqlSelectStatement) {
      SqlSelectStatement select = (SqlSelectStatement)query;
      if (select.getTables().size() > 1) {
        implicitJoin = true;
      }
    }
    return implicitJoin;
  }

  protected static boolean isConstantQuery(SqlQueryStatement query) {
    if (query.getGroupBy().size() > 0) return false;

    boolean containsAggConstant = false;
    boolean containsGeneralColumn = false;
    SqlCollection<SqlColumn> columns = query.getColumns();
    for (SqlColumn c : columns) {
      if (c instanceof SqlFormulaColumn) {
        SqlFormulaColumn function = (SqlFormulaColumn)c;
        String aggFun =  function.getColumnName();
        if (Utilities.equalIgnoreCase("SUM", Utilities.toUpperCase(aggFun)) || Utilities.equalIgnoreCase("COUNT", Utilities.toUpperCase(aggFun)) || Utilities.equalIgnoreCase("COUNT_BIG", Utilities.toUpperCase(aggFun)))
          return false;

        if (SqlUtilities.isKnownAggragation(aggFun)) {
          SqlCollection<SqlExpression> paras = function.getParameters();
          if (paras.get(0) instanceof SqlValueExpression) {
            containsAggConstant = true;
          } else {
            containsGeneralColumn = true;
          }
        }
      } else if (c instanceof SqlConstantColumn) {
        if (null == query.getTable() || Utilities.equalIgnoreCase(NO_TABLE_QUERY_TABLE_NAME_PREFIX, query.getTableName())) {
          containsAggConstant = true;
        }
      } else {
        containsGeneralColumn = true;
      }
    }
    return !containsGeneralColumn && containsAggConstant;
  }

  protected static boolean containsNestedJoin(SqlTable table) {
    boolean contain = false;
    if (table.isNestedJoinTable()) {
      contain = true;
    } else {
      if (table.hasJoin()) {
        SqlJoin j = table.getJoin();
        contain = containsNestedJoin(j.getTable());
      }
    }
    return contain;
  }

  protected static boolean isJoinsAssociative(SqlTable left) {
    boolean isAssociative = false;
    if (left.isNestedJoinTable()) {
      isAssociative = isJoinsAssociative(left.getNestedJoin());
    } else {
      isAssociative = true;
    }

    if (isAssociative) {
      if (left.hasJoin()) {
        SqlJoin j = left.getJoin();
        SqlTable lrm = left;
        if (left.isNestedJoinTable()) {
          lrm = getRightMostTable(left.getNestedJoin());
        }
        SqlTable rlm = getLeftMostTable(j.getTable());
        boolean isOnAdjacent = isOnAdjacent(j.getCondition(), lrm, rlm) || null == j.getCondition();
        boolean NotRightJoin = j.getJoinType() != JoinType.RIGHT;
        if (isOnAdjacent) {
          isAssociative = true;
        } else {
          isAssociative = false;
        }
        if (isAssociative) {
          isAssociative = isJoinsAssociative(rlm);
        }
      } else {
        isAssociative = true;
      }
    }
    return isAssociative;
  }

  protected static boolean isOnAdjacent(SqlConditionNode on, SqlTable lm, SqlTable rm) {
    boolean isAdjacent = false;
    if (on instanceof SqlCriteria) {
      if (isStandardOnCriteria(on)) {
        SqlGeneralColumn lc = (SqlGeneralColumn) ((SqlCriteria)on).getLeft();
        SqlGeneralColumn rc = (SqlGeneralColumn) ((SqlCriteria)on).getRight();
        boolean sourceNameMatch1 = Utilities.equalIgnoreCase(lm.getValidName(), lc.getTableName()) && Utilities.equalIgnoreCase(rm.getValidName(), rc.getTableName());
        boolean sourceNameMatch2 = Utilities.equalIgnoreCase(lm.getValidName(), rc.getTableName()) && Utilities.equalIgnoreCase(rm.getValidName(), lc.getTableName());
        boolean aliasNameMatch1 = Utilities.equalIgnoreCase(lm.getAlias(), lc.getTable().getAlias()) && Utilities.equalIgnoreCase(rm.getAlias(), rc.getTable().getAlias());
        boolean aliasNameMatch2 = Utilities.equalIgnoreCase(lm.getAlias(), rc.getTable().getAlias()) && Utilities.equalIgnoreCase(rm.getAlias(), lc.getTable().getAlias());
        if (sourceNameMatch1 || sourceNameMatch2 || aliasNameMatch1 || aliasNameMatch2) {
          isAdjacent = true;
        }
      }
    }
    return isAdjacent;
  }

  protected static boolean isStandardCriteriaInJoin(SqlJoin topJoin) {
    boolean isStandard = true;
    if (topJoin != null) {
      isStandard = isStandardOnCriteria(topJoin.getCondition());
      if (isStandard) {
        isStandard = isStandardCriteriaInJoin(topJoin.getTable().getJoin());
      }
    }
    return isStandard;
  }

  protected static boolean isStandardOnCriteria(SqlConditionNode condition) {
    boolean isStandard = true;
    if (condition instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria)condition;
      SqlExpression l = c.getLeft();
      SqlExpression r = c.getRight();
      if (!(l instanceof SqlGeneralColumn && r instanceof SqlGeneralColumn)) {
        isStandard = false;
      }
    } else if (condition instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)condition;
      if (SqlLogicalOperator.Or == c.getLogicOp()) {
        isStandard = true;
      } else {
        isStandard = isStandardOnCriteria((SqlConditionNode)c.getLeft());
        if (isStandard) {
          isStandard = isStandardOnCriteria((SqlConditionNode)c.getRight());
        }
      }
    }
    return isStandard;
  }

  protected static boolean isEquiJoinCriteria(SqlConditionNode where, SqlCollection<SqlTable> tables) {
    int tableIndex = 0;
    boolean isEquiJoin = true;
    for( ; (tableIndex + 1)< tables.size(); ) {
      SqlTable l = tables.get(tableIndex);
      SqlTable r = tables.get(++tableIndex);
      isEquiJoin = isEquiJoinCriteria(where, l, r);
      if(!isEquiJoin) {
        break;
      }
    }

    return isEquiJoin;
  }

  protected static boolean isEquiJoinCriteria(SqlConditionNode where, SqlTable left, SqlTable right) {
    boolean isEquiJoin = false;
    if (where instanceof SqlCondition) {
      SqlCondition andCond = (SqlCondition) where;
      if (andCond.getLogicOp() == SqlLogicalOperator.And) {
        SqlExpression l = andCond.getLeft();
        SqlExpression r = andCond.getRight();
        if (l instanceof SqlConditionNode && isStandardOnCriteria((SqlConditionNode) l)) {
          isEquiJoin = isEquiJoinCriteria((SqlConditionNode) l, left, right);
        }
        if (!isEquiJoin && r instanceof SqlConditionNode && isStandardOnCriteria((SqlConditionNode) r)) {
          isEquiJoin = isEquiJoinCriteria((SqlConditionNode) r, left, right);
        }
      }
    } else if (where instanceof SqlCriteria && isStandardOnCriteria(where)) {
      if (SqlUtilities.isSimpleTable(left) && SqlUtilities.isSimpleTable(right)) {
        isEquiJoin = isEquiJoinCriteria((SqlCriteria)where,
                left.getAlias(),
                right.getAlias());
      } else if (SqlUtilities.isSimpleTable(left)) {
        if (right.isNestedQueryTable()) {
          isEquiJoin =  true;
        }
      } else if (SqlUtilities.isSimpleTable(right)) {
        if (left.isNestedQueryTable()) {
          isEquiJoin = true;
        }
      } else {
        if (left.isNestedQueryTable() && right.isNestedQueryTable()) {
          isEquiJoin = true;
        }
      }
    }
    return isEquiJoin;
  }

  protected static boolean isEquiJoinCriteria(SqlCriteria criteria, String t1, String t2) {
    boolean isEquiJoin = false;
    SqlColumn lc = (SqlColumn)criteria.getLeft();
    SqlColumn rc = (SqlColumn)criteria.getRight();
    if (Utilities.equalIgnoreCaseInvariant(t1, lc.getTableName())
            && Utilities.equalIgnoreCaseInvariant(t2, rc.getTableName())) {
      isEquiJoin = true;
    } else if (Utilities.equalIgnoreCaseInvariant(t1, rc.getTableName())
            && Utilities.equalIgnoreCaseInvariant(t2, lc.getTableName())) {
      isEquiJoin = true;
    }
    return isEquiJoin;
  }

  private static SqlTable getRightMostTable(SqlTable t) {
    SqlTable rm = t;
    if (t.hasJoin()) {
      SqlJoin join = t.getJoin();
      SqlTable right = join.getTable();
      rm = getRightMostTable(right);
    }
    return rm;
  }

  private static SqlTable getLeftMostTable(SqlTable t) {
    SqlTable lm = t;
    if (t.isNestedJoinTable()) {
      SqlTable nestedJoin = t.getNestedJoin();
      lm = getLeftMostTable(nestedJoin);
    }
    return lm;
  }

}

interface IColumnCompare {
  public boolean compare(SqlColumn c1, SqlColumn c2);
}

interface ITableCompare {
  public boolean compare(SqlTable t1, SqlTable t2);
}

interface IFunctionSubstitute {
  public SqlExpression substitute(SqlColumn f) throws Exception;
  public boolean match(SqlColumn f);
}

final class SourceColumnComparer implements IColumnCompare {
  /*@*/public/*@*/ /*#public#*/  boolean compare(SqlColumn c1, SqlColumn c2) {
    SimpleTableComparer tc = new SimpleTableComparer();
    if (!tc.compare(c1.getTable(), c2.getTable())) return false;

    boolean r1 = Utilities.equalIgnoreCase(c1.getColumnName(), c2.getAlias());
    boolean r2 = Utilities.equalIgnoreCase(c2.getColumnName(), c1.getAlias());
    return r1 || r2;
  }
}

final class SimpleTableComparer implements ITableCompare {
  /*@*/public/*@*/ /*#public#*/ boolean compare(SqlTable t1, SqlTable t2) {
    if (t1 == null || t2 == null) {
      return true;
    }
    boolean c1 = Utilities.equalIgnoreCase(t1.getName(), t2.getAlias());
    boolean c2 = Utilities.equalIgnoreCase(t2.getName(), t1.getAlias());
    return c1 || c2;
  }
}

final class NULLIFSubstitue implements IFunctionSubstitute {
  private static final String Name = "NULLIF";
  /*@*/public/*@*/ /*#public#*/ SqlExpression substitute(SqlColumn f) throws Exception {
    if (!Utilities.equalIgnoreCase(Name, f.getColumnName())) {
      return f;
    }
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    SqlCollection<SqlExpression> parameters = new SqlCollection<SqlExpression>();
    parameters.add(null);
    SqlCriteria c = new SqlCriteria(func.getParameters().get(0), ComparisonType.NOT_EQUAL, func.getParameters().get(1));
    parameters.add(c);
    parameters.add(func.getParameters().get(0));
    parameters.add(new SqlValueExpression(SqlValue.getNullValueInstance()));
    if (f.hasAlias()) {
      return new SqlFormulaColumn("CASE", func.getAlias(), parameters);
    } else {
      return new SqlFormulaColumn("CASE", parameters);
    }
  }

  /*@*/public/*@*/ /*#public#*/ boolean match(SqlColumn f) {
    if (!(f instanceof SqlFormulaColumn)) return false;
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    return Utilities.equalIgnoreCase(Name, func.getColumnName()) && 2 == func.getParameters().size();
  }
}

final class IIFSubstitue implements IFunctionSubstitute {
  private static final String Name = "IIF";
  /*@*/public/*@*/ /*#public#*/  SqlExpression substitute(SqlColumn f) throws Exception {
    //https://docs.microsoft.com/en-us/sql/t-sql/functions/logical-functions-iif-transact-sql?view=sql-server-2017
    if (!Utilities.equalIgnoreCase(Name, f.getColumnName())) {
      return f;
    }

    SqlFormulaColumn func = (SqlFormulaColumn) f;
    SqlCollection<SqlExpression> parameters = new SqlCollection<SqlExpression>();
    parameters.add(null);
    parameters.add(func.getParameters().get(0));
    parameters.add(func.getParameters().get(1));
    parameters.add(func.getParameters().get(2));
    if (f.hasAlias()) {
      return new SqlFormulaColumn("CASE", func.getAlias(), parameters);
    } else {
      return new SqlFormulaColumn("CASE", parameters);
    }
  }

  /*@*/public/*@*/ /*#public#*/  boolean match(SqlColumn f) {
    if (!(f instanceof SqlFormulaColumn)) return false;
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    return Utilities.equalIgnoreCase(Name, func.getColumnName()) && 3 == func.getParameters().size();
  }
}

final class COALESCESubstitue implements IFunctionSubstitute {
  private static final String Name = "COALESCE";
  /*@*/public/*@*/ /*#public#*/ SqlExpression substitute(SqlColumn f) throws Exception {
    if (!Utilities.equalIgnoreCase(Name, f.getColumnName())) {
      return f;
    }
    SqlCollection<SqlExpression> parameters = new SqlCollection<SqlExpression>();
    parameters.add(null);

    SqlFormulaColumn func = (SqlFormulaColumn) f;
    for (int i = 0 ; i < func.getParameters().size() - 1; ++i) {
      SqlExpression p = func.getParameters().get(i);
      SqlCriteria c = new SqlCriteria(p, ComparisonType.IS_NOT, new SqlValueExpression(SqlValue.getNullValueInstance()));
      parameters.add(c);
      parameters.add(p);
    }

    parameters.add(func.getParameters().get(func.getParameters().size() - 1));

    if (func.hasAlias()) {
      return new SqlFormulaColumn("CASE", func.getAlias(), parameters);
    } else {
      return new SqlFormulaColumn("CASE", parameters);
    }
  }

  /*@*/public/*@*/ /*#public#*/ boolean match(SqlColumn f) {
    if (!(f instanceof SqlFormulaColumn)) return false;
    SqlFormulaColumn func = (SqlFormulaColumn) f;
    return Utilities.equalIgnoreCase(Name, func.getColumnName()) && func.getParameters().size() > 1;
  }
}

final class ATATVariableSubstitue implements IFunctionSubstitute {

  /*@*/public/*@*/ /*#public#*/  SqlExpression substitute(SqlColumn f) throws Exception {
    SqlValueExpression p = (SqlValueExpression) f.getExpr();
    SqlCollection<SqlExpression> paras =new SqlCollection<SqlExpression>();
    paras.add(new SqlValueExpression(SqlValueType.STRING, p.getParameterName()));
    if (f.hasAlias()) {
      return new SqlFormulaColumn("SYSTEM_VARIABLE", f.getAlias(), paras);
    } else {
      return new SqlFormulaColumn("SYSTEM_VARIABLE", paras);
    }
  }

  /*@*/public/*@*/ /*#public#*/  boolean match(SqlColumn f) {
    if (!(f instanceof SqlConstantColumn)) return false;
    SqlConstantColumn cc = (SqlConstantColumn) f;
    if (!ParserCore.isParameterExpression(cc.getExpr())) {
      return false;
    }

    SqlValueExpression p = (SqlValueExpression) cc.getExpr();

    String pName = p.getParameterName();
    if (pName != null && pName.startsWith("@@")) {
      return true;
    }

    return false;
  }
}
