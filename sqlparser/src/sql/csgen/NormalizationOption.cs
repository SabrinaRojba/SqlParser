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

public sealed class NormalizationOption {
  private readonly bool _implicitCrossJoin;
  private readonly bool _implicitNaturalJoin;
  private readonly bool _tableExprWithQuery;
  private readonly bool _criteriaWithSubQuery;
  private readonly bool _criteriaWithNot;
  private readonly bool _criteria;
  private readonly bool _resolveTableAlias;
  private readonly bool _fixUniqueAlias;
  private readonly bool _criteriaInJoin;
  private readonly bool _implicitCommaJoin;
  private readonly bool _implicitInnerJoin;
  private readonly bool _operationExpression;
  private readonly bool _appendNestedQueryTableName;
  private readonly bool _rightJoin;
  private readonly bool _minimizeCriteria;
  private readonly bool _constantColumn;
  private readonly bool _formulaAlias;
  private readonly bool _nestedJoin;
  private readonly bool _distinct;
  private readonly bool _count_distinct;
  private readonly bool _predictTrue;
  private readonly bool _equiInnerJoin;
  private readonly bool _semi_anti_join;
  private readonly bool _functionSubstitute;
  private readonly bool _removeDistinctIfColumnUnique;
  
  public static string[] DEFAULT_NORMALIZATIONS  = new string[] {
          "ImplicitNaturalJoin",
          "TableExprWithQuery",
          "ConstantColumn",
          "Distinct",
          "CriteriaWithSubQuery",
          "CriteriaWithNot",
          "Criteria",
          "MinimizeCriteria",
          "ResolveTableAlias",
          "FixUniqueAlias",
          "CriteriaInJoin",
          "OperationExpression",
          "AppendNestedQueryTableName",
          "NestedJoin",
          "PredictTrue",
          "EquiInnerJoin",
          "FunctionSubstitute"
  };

  public static NormalizationOption BuildNormalizationOption(string[] normalizationOptions) {
    NormalizationOptionBuilder builder = new NormalizationOptionBuilder();
    if (normalizationOptions != null && normalizationOptions.Length > 0) {
      foreach(String opt in normalizationOptions) {
        if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "ImplicitNaturalJoin")) {
          builder.Config(NormalizationOptions.ImplicitNaturalJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "ImplicitCrossJoin")) {
          builder.Config(NormalizationOptions.ImplicitCrossJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "ImplicitCommaJoin")) {
          builder.Config(NormalizationOptions.ImplicitCommaJoin, true);
        }  else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "ImplicitInnerJoin")) {
          builder.Config(NormalizationOptions.ImplicitInnerJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "TableExprWithQuery")) {
          builder.Config(NormalizationOptions.TableExprWithQuery, true);
        }  else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "ConstantColumn")) {
          builder.Config(NormalizationOptions.ConstantColumn, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "CriteriaWithSubQuery")) {
          builder.Config(NormalizationOptions.CriteriaWithSubQuery, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "CriteriaWithNot")) {
          builder.Config(NormalizationOptions.CriteriaWithNot, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "Criteria")) {
          builder.Config(NormalizationOptions.Criteria, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "ResolveTableAlias")) {
          builder.Config(NormalizationOptions.ResolveTableAlias, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "FixUniqueAlias")) {
          builder.Config(NormalizationOptions.FixUniqueAlias, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "CriteriaInJoin")) {
          builder.Config(NormalizationOptions.CriteriaInJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "OperationExpression")) {
          builder.Config(NormalizationOptions.OperationExpression, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "AppendNestedQueryTableName")) {
          builder.Config(NormalizationOptions.AppendNestedQueryTableName, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "RightJoin")) {
          builder.Config(NormalizationOptions.RightJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "MinimizeCriteria")) {
          builder.Config(NormalizationOptions.MinimizeCriteria, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "FormulaAlias")) {
          builder.Config(NormalizationOptions.FormulaAlias, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "NestedJoin")) {
          builder.Config(NormalizationOptions.NestedJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "Distinct")) {
          builder.Config(NormalizationOptions.Distinct, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "Count_Distinct")) {
          builder.Config(NormalizationOptions.Count_Distinct, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "PredictTrue")) {
          builder.Config(NormalizationOptions.PredictTrue, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "EquiInnerJoin")) {
          builder.Config(NormalizationOptions.EquiInnerJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "SemiAntiJoin")) {
          builder.Config(NormalizationOptions.SemiAntiJoin, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "FunctionSubstitute")) {
          builder.Config(NormalizationOptions.FunctionSubstitute, true);
        } else if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(opt, "RemoveDistinctIfColumnUnique")) {
          builder.Config(NormalizationOptions.RemoveDistinctIfColumnUnique, true);
        }
      }
    }
    builder.Config(NormalizationOptions.FormulaAlias, true);
    builder.Config(NormalizationOptions.ResolveTableAlias, true);
    return builder.Build();
  }

  public static string[] SetImplicitJoin(string [] DEFAULT_NORMALIZATIONS, string implicitJoinType) {
    string[] result = DEFAULT_NORMALIZATIONS;
    string temp = Utilities.ToUpper(implicitJoinType);
    if (temp != null && temp.StartsWith("IMPLICIT")) {
      result = new string[DEFAULT_NORMALIZATIONS.Length];
      for (int i = 0 ; i < DEFAULT_NORMALIZATIONS.Length; ++i) {
        string optionName = Utilities.ToUpper(DEFAULT_NORMALIZATIONS[i]);
        if (optionName.StartsWith("IMPLICIT")) {
          result[i] = implicitJoinType;
        } else {
          result[i] = optionName;
        }
      }
    }
    return result;
  }

  public static string[] AddToDefaultNormalizations(string normalizations) {
    JavaVector<string> modifiedOptions = new JavaVector<string>();
    if(normalizations != null) {
      string[] add= Utilities.SplitString(normalizations.ToUpper(),",");
      int total = DEFAULT_NORMALIZATIONS.Length;
      for(int i=0;i<DEFAULT_NORMALIZATIONS.Length;i++){
        modifiedOptions.Add(DEFAULT_NORMALIZATIONS[i].ToUpper());
      }
      foreach(String s in add) {
        if(s.StartsWith("-")) modifiedOptions.Remove(RSSBus.core.j2cs.Converter.GetSubstring(s, 1));
        else if(s.StartsWith("+")) modifiedOptions.Add(RSSBus.core.j2cs.Converter.GetSubstring(s, 1));
        else modifiedOptions.Add(s);
      }
    }
    string[] retValue = new string[modifiedOptions.Size()];
    for(int i =0; i<modifiedOptions.Size(); i++) {
      retValue[i] = modifiedOptions.ElementAt(i);
    }
    return retValue;
  }

  public static string[] AddToNormalizations(string [] normalizations, string addings) {
    string[] add = Utilities.SplitString(addings, ",");

    if (null == normalizations) {
      normalizations = new string[0];
    }
    int total = normalizations.Length + add.Length;
    string[] result = new string[total];
    for(int i=0;i<normalizations.Length;i++){
      result[i] = normalizations[i];
    }
    for(int i=normalizations.Length, j=0;i<total;i++, j++){
      result[i] = add[j];
    }
    return result;
  }

  public NormalizationOption(NormalizationOptionBuilder builder) {
    this._implicitCommaJoin = builder.GetConfigAsBoolean(NormalizationOptions.ImplicitCommaJoin);
    this._implicitNaturalJoin = builder.GetConfigAsBoolean(NormalizationOptions.ImplicitNaturalJoin);
    this._implicitCrossJoin = builder.GetConfigAsBoolean(NormalizationOptions.ImplicitCrossJoin);
    this._implicitInnerJoin = builder.GetConfigAsBoolean(NormalizationOptions.ImplicitInnerJoin);
    this._tableExprWithQuery = builder.GetConfigAsBoolean(NormalizationOptions.TableExprWithQuery);
    this._criteriaWithSubQuery = builder.GetConfigAsBoolean(NormalizationOptions.CriteriaWithSubQuery);
    this._criteriaWithNot = builder.GetConfigAsBoolean(NormalizationOptions.CriteriaWithNot);
    this._criteria = builder.GetConfigAsBoolean(NormalizationOptions.Criteria);
    this._resolveTableAlias = builder.GetConfigAsBoolean(NormalizationOptions.ResolveTableAlias);
    this._fixUniqueAlias = builder.GetConfigAsBoolean(NormalizationOptions.FixUniqueAlias);
    this._criteriaInJoin = builder.GetConfigAsBoolean(NormalizationOptions.CriteriaInJoin);
    this._operationExpression = builder.GetConfigAsBoolean(NormalizationOptions.OperationExpression);
    this._appendNestedQueryTableName = builder.GetConfigAsBoolean(NormalizationOptions.AppendNestedQueryTableName);
    this._rightJoin = builder.GetConfigAsBoolean(NormalizationOptions.RightJoin);
    this._minimizeCriteria = builder.GetConfigAsBoolean(NormalizationOptions.MinimizeCriteria);
    this._constantColumn = builder.GetConfigAsBoolean(NormalizationOptions.ConstantColumn);
    this._formulaAlias = builder.GetConfigAsBoolean(NormalizationOptions.FormulaAlias);
    this._nestedJoin = builder.GetConfigAsBoolean(NormalizationOptions.NestedJoin);
    this._distinct = builder.GetConfigAsBoolean(NormalizationOptions.Distinct);
    this._count_distinct = builder.GetConfigAsBoolean(NormalizationOptions.Count_Distinct);
    this._predictTrue = builder.GetConfigAsBoolean(NormalizationOptions.PredictTrue);
    this._equiInnerJoin = builder.GetConfigAsBoolean(NormalizationOptions.EquiInnerJoin);
    this._semi_anti_join = builder.GetConfigAsBoolean(NormalizationOptions.SemiAntiJoin);
    this._functionSubstitute = builder.GetConfigAsBoolean(NormalizationOptions.FunctionSubstitute);
    this._removeDistinctIfColumnUnique = builder.GetConfigAsBoolean(NormalizationOptions.RemoveDistinctIfColumnUnique);
  }

  public bool NormalizeImplicitCommaJoin() {
    return this._implicitCommaJoin;
  }

  public bool NormalizeImplicitCrossJoin() {
    return this._implicitCrossJoin;
  }

  public bool NormalizeImplicitInnerJoin() {
    return this._implicitInnerJoin;
  }

  public bool NormalizeImplicitNaturalJoin() {
    return this._implicitNaturalJoin;
  }

  public bool NormalizeTableExprWithQuery() {
    return this._tableExprWithQuery;
  }

  public bool NormalizeCriteriaWithSubQuery() {
    return _criteriaWithSubQuery;
  }

  public bool NormalizeCriteriaWithNot() {
    return _criteriaWithNot;
  }

  public bool NormalizeResolveTableAlias() {
    return _resolveTableAlias;
  }

  public bool NormalizeFixUniqueAlias() {
    return this._fixUniqueAlias;
  }

  public bool NormalizeCriteria() {
    return this._criteria;
  }

  public bool NormalizeCriteriaInJoin() {
    return this._criteriaInJoin;
  }

  public bool NormalizeOperationExpression() {
    return this._operationExpression;
  }

  public bool NormalizeNestedQueryTableName() {
    return this._appendNestedQueryTableName;
  }

  public bool NormalizeRightJoin() {
    return this._rightJoin;
  }

  public bool NormalizeMinimizeCriteria() {
    return this._minimizeCriteria;
  }

  public bool NormalizeConstantColumn() {
    return this._constantColumn;
  }

  public bool NormalizeFormulaAlias() {
    return this._formulaAlias;
  }

  public bool NormalizeNestedJoin() {
    return this._nestedJoin;
  }

  public bool NormalizeDistinct() {
    return this._distinct;
  }

  public bool NormalizeCountDistinct() {
    return this._count_distinct;
  }

  public bool NormalizePredictTrue() {
    return this._predictTrue;
  }

  public bool NormalizeEquiInnerJoin() {
    return this._equiInnerJoin;
  }

  public bool NormalizedSemiAntiJoin() {
    return this._semi_anti_join;
  }

  public bool NormalizedFunctionSubstitute() {
    return this._functionSubstitute;
  }

  public bool NormalizeRemoveDistinctIfColumnUnique() {
    return this._removeDistinctIfColumnUnique;
  }
}
}

