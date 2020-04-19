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


public sealed class SqlFormulaColumn : SqlColumn , ISqlElement {
  public const string SCALAR_FUNCTION_NAME_PREFIX = "fn ";
  private readonly SqlCollection<SqlExpression> parameters;
  private readonly bool hasAlias;
  private readonly SqlOverClause _overClause;

  public SqlFormulaColumn(string fname, SqlCollection<SqlExpression> paras) : this(fname, BuildName(fname, paras), paras, false, null) {
  }

  public SqlFormulaColumn(string fname,
                          SqlCollection<SqlExpression> paras,
                          SqlOverClause overClause) : this(fname, BuildName(fname, paras), paras, false, overClause) {
  }

  public SqlFormulaColumn(string fname,
                          string alias,
                          SqlCollection<SqlExpression> paras) : this(fname, alias, paras, true, null) {
  }

  public SqlFormulaColumn(string fname,
                          string alias,
                          SqlCollection<SqlExpression> paras,
                          SqlOverClause overClause) : this(fname,
            alias,
            paras,
            true,
            overClause) {
  }

  private SqlFormulaColumn(string fname,
                           string alias,
                           SqlCollection<SqlExpression> paras,
                           bool hasAlias,
                           SqlOverClause overClause) : base(fname, alias) {
    parameters = paras;
    this.hasAlias = hasAlias;
    this._overClause = overClause;
  }

  public override bool Equals(string name) {
    if (!Utilities.IsNullOrEmpty(name)) {
      return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(name, this.GetColumnName());
    }
    return false;
  }

  public SqlCollection<SqlExpression> GetParameters() {
    return parameters;
  }

  public SqlOverClause GetOverClause() {
    return this._overClause;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    SqlColumnMeta meta = GetMetadata();
    if (meta != null && meta.Formula != null) {
      IFormula f = meta.Formula;
      //TODO: it will be implemented later.

      Object fv = "";
      try {
        fv = f.Evaluate(null);
      } catch (Exception ex) {;}
      SqlValue v = new SqlValue(SqlValueType.STRING, fv + "");
      return v;
    } else {
      return SqlValue.GetNullValueInstance();
    }
  }

  public override Object Clone() {
    SqlFormulaColumn obj = new SqlFormulaColumn(this.GetColumnName(), this.GetAlias(), this.GetParameters(), this.HasAlias(), this._overClause);
    obj.Copy(this);
    return obj;
  }

  public override bool HasAlias() {
    return this.hasAlias;
  }

  public bool IsScalarFunction() {
    string fname = this.GetColumnName();
    return fname.StartsWith(SCALAR_FUNCTION_NAME_PREFIX);
  }

  private static string BuildName(string name, SqlCollection<SqlExpression> parameters){
    ByteBuffer result = new ByteBuffer();
    if (name != null) {
      result.Append(name);
      foreach(SqlExpression para in parameters) {
        if (para is SqlValueExpression) {
          if (!Utilities.EqualIgnoreCase("EXPR", name)) {
            continue;
          }
        }
        string n = BuildName(para);
        if (n != null && n.Length > 0) {
          result.Append("_").Append(BuildName(para));
        }
      }
    }
    return result.ToString();
  }

  private static string BuildName(SqlExpression expr) {
    if (expr is SqlFormulaColumn) {
      SqlFormulaColumn function = (SqlFormulaColumn) expr;
      return BuildName(function.GetColumnName(), function.GetParameters());
    } else if (expr is SqlColumn) {
      return GetAlphaNumeric(((SqlColumn)expr).GetColumnName());
    } else if (expr is SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) expr;
      SqlExpression l = opExpr.GetLeft();
      SqlExpression r = opExpr.GetRight();
      ByteBuffer sb = new ByteBuffer();
      sb.Append(BuildName(l)).Append("_").Append(BuildName(r));
      return sb.ToString();
    } else if (expr is SqlCriteria) {
        SqlCriteria criteriaExpr = (SqlCriteria) expr;
        SqlExpression l = criteriaExpr.GetLeft();
        SqlExpression r = criteriaExpr.GetRight();
        ByteBuffer sb = new ByteBuffer();
        sb.Append(BuildName(l)).Append("_").Append(criteriaExpr.GetOperatorAsEnglishString()).Append("_").Append(BuildName(r));
        return sb.ToString();
    } else if (expr is SqlCondition) {
      SqlCondition condition = (SqlCondition) expr;
      SqlExpression l = condition.GetLeft();
      SqlExpression r = condition.GetRight();
      ByteBuffer sb = new ByteBuffer();
      sb.Append(BuildName(l)).Append("_").Append(condition.GetLogicOp()).Append("_").Append(BuildName(r));
      return sb.ToString();
    } else if (expr is SqlValueExpression) {
      if (expr.IsEvaluatable()) {
        return expr.Evaluate().GetValueAsString("");
      }
    }
    return "";
  }
  
  private static string GetAlphaNumeric(string original) {
    string modified = "";
    for (int i=0; i<original.Length; i++) {
      if (!Utilities.IsAlpha(original[i]) && !Utilities.IsNumber(modified))
        modified += Utilities.ToString((int)original[i]);
      else
        modified += original[i];
    }
    return modified;
  }
}
}

