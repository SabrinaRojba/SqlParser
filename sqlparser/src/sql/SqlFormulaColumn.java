//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import rssbus.oputils.common.Utilities;

public final class SqlFormulaColumn extends SqlColumn implements ISqlElement {
  public static final String SCALAR_FUNCTION_NAME_PREFIX = "fn ";
  private final SqlCollection<SqlExpression> parameters;
  private final boolean hasAlias;
  private final SqlOverClause _overClause;

  public SqlFormulaColumn(String fname, SqlCollection<SqlExpression> paras) {
    this(fname, buildName(fname, paras), paras, false, null);
  }

  public SqlFormulaColumn(String fname,
                          SqlCollection<SqlExpression> paras,
                          SqlOverClause overClause) {
    this(fname, buildName(fname, paras), paras, false, overClause);
  }

  public SqlFormulaColumn(String fname,
                          String alias,
                          SqlCollection<SqlExpression> paras) {
    this(fname, alias, paras, true, null);
  }

  public SqlFormulaColumn(String fname,
                          String alias,
                          SqlCollection<SqlExpression> paras,
                          SqlOverClause overClause) {
    this(fname,
            alias,
            paras,
            true,
            overClause);
  }

  private SqlFormulaColumn(String fname,
                           String alias,
                           SqlCollection<SqlExpression> paras,
                           boolean hasAlias,
                           SqlOverClause overClause) {
    super(fname, alias);
    parameters = paras;
    this.hasAlias = hasAlias;
    this._overClause = overClause;
  }

  public /*#override#*/ boolean equals(String name) {
    if (!Utilities.isNullOrEmpty(name)) {
      return name.equalsIgnoreCase(this.getColumnName());
    }
    return false;
  }

  public SqlCollection<SqlExpression> getParameters() {
    return parameters;
  }

  public SqlOverClause getOverClause() {
    return this._overClause;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    SqlColumnMeta meta = getMetadata();
    if (meta != null && meta.Formula != null) {
      IFormula f = meta.Formula;
      //TODO: it will be implemented later.

      Object fv = "";
      try {
        fv = f.evaluate(null);
      } catch (Exception ex) {;}
      SqlValue v = new SqlValue(SqlValueType.STRING, fv + "");
      return v;
    } else {
      return SqlValue.getNullValueInstance();
    }
  }

  public /*#override#*/ Object clone() {
    SqlFormulaColumn obj = new SqlFormulaColumn(this.getColumnName(), this.getAlias(), this.getParameters(), this.hasAlias(), this._overClause);
    obj.copy(this);
    return obj;
  }

  public /*#override#*/ boolean hasAlias() {
    return this.hasAlias;
  }

  public boolean isScalarFunction() {
    String fname = this.getColumnName();
    return fname.startsWith(SCALAR_FUNCTION_NAME_PREFIX);
  }

  private static String buildName(String name, SqlCollection<SqlExpression> parameters){
    StringBuffer result = new StringBuffer();
    if (name != null) {
      result.append(name);
      for(SqlExpression para : parameters) {
        if (para instanceof SqlValueExpression) {
          if (!Utilities.equalIgnoreCase("EXPR", name)) {
            continue;
          }
        }
        String n = buildName(para);
        if (n != null && n.length() > 0) {
          result.append("_").append(buildName(para));
        }
      }
    }
    return result.toString();
  }

  private static String buildName(SqlExpression expr) {
    if (expr instanceof SqlFormulaColumn) {
      SqlFormulaColumn function = (SqlFormulaColumn) expr;
      return buildName(function.getColumnName(), function.getParameters());
    } else if (expr instanceof SqlColumn) {
      return getAlphaNumeric(((SqlColumn)expr).getColumnName());
    } else if (expr instanceof SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) expr;
      SqlExpression l = opExpr.getLeft();
      SqlExpression r = opExpr.getRight();
      StringBuilder sb = new StringBuilder();
      sb.append(buildName(l)).append("_").append(buildName(r));
      return sb.toString();
    } else if (expr instanceof SqlCriteria) {
        SqlCriteria criteriaExpr = (SqlCriteria) expr;
        SqlExpression l = criteriaExpr.getLeft();
        SqlExpression r = criteriaExpr.getRight();
        StringBuilder sb = new StringBuilder();
        sb.append(buildName(l)).append("_").append(criteriaExpr.getOperatorAsEnglishString()).append("_").append(buildName(r));
        return sb.toString();
    } else if (expr instanceof SqlCondition) {
      SqlCondition condition = (SqlCondition) expr;
      SqlExpression l = condition.getLeft();
      SqlExpression r = condition.getRight();
      StringBuilder sb = new StringBuilder();
      sb.append(buildName(l)).append("_").append(condition.getLogicOp()).append("_").append(buildName(r));
      return sb.toString();
    } else if (expr instanceof SqlValueExpression) {
      if (expr.isEvaluatable()) {
        return expr.evaluate().getValueAsString("");
      }
    }
    return "";
  }
  
  private static String getAlphaNumeric(String original) {
    String modified = "";
    for (int i=0; i<original.length(); i++) {
      if (!Utilities.isAlpha(original.charAt(i)) && !Utilities.IsNumber(modified))
        modified += Utilities.toString((int)original.charAt(i));
      else
        modified += original.charAt(i);
    }
    return modified;
  }
}
