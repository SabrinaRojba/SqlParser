//@
package cdata.sql;
import rssbus.oputils.common.*;
import java.util.*;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public final class NormalizationOption {
  private final boolean _implicitCrossJoin;
  private final boolean _implicitNaturalJoin;
  private final boolean _tableExprWithQuery;
  private final boolean _criteriaWithSubQuery;
  private final boolean _criteriaWithNot;
  private final boolean _criteria;
  private final boolean _resolveTableAlias;
  private final boolean _fixUniqueAlias;
  private final boolean _criteriaInJoin;
  private final boolean _implicitCommaJoin;
  private final boolean _implicitInnerJoin;
  private final boolean _operationExpression;
  private final boolean _appendNestedQueryTableName;
  private final boolean _rightJoin;
  private final boolean _minimizeCriteria;
  private final boolean _constantColumn;
  private final boolean _formulaAlias;
  private final boolean _nestedJoin;
  private final boolean _distinct;
  private final boolean _count_distinct;
  private final boolean _predictTrue;
  private final boolean _equiInnerJoin;
  private final boolean _semi_anti_join;
  private final boolean _functionSubstitute;
  private final boolean _removeDistinctIfColumnUnique;
  
  public static final String[] DEFAULT_NORMALIZATIONS  = new String[] {
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

  public static NormalizationOption buildNormalizationOption(String[] normalizationOptions) {
    NormalizationOptionBuilder builder = new NormalizationOptionBuilder();
    if (normalizationOptions != null && normalizationOptions.length > 0) {
      for (String opt : normalizationOptions) {
        if (opt.equalsIgnoreCase("ImplicitNaturalJoin")) {
          builder.config(NormalizationOptions.ImplicitNaturalJoin, true);
        } else if (opt.equalsIgnoreCase("ImplicitCrossJoin")) {
          builder.config(NormalizationOptions.ImplicitCrossJoin, true);
        } else if (opt.equalsIgnoreCase("ImplicitCommaJoin")) {
          builder.config(NormalizationOptions.ImplicitCommaJoin, true);
        }  else if (opt.equalsIgnoreCase("ImplicitInnerJoin")) {
          builder.config(NormalizationOptions.ImplicitInnerJoin, true);
        } else if (opt.equalsIgnoreCase("TableExprWithQuery")) {
          builder.config(NormalizationOptions.TableExprWithQuery, true);
        }  else if (opt.equalsIgnoreCase("ConstantColumn")) {
          builder.config(NormalizationOptions.ConstantColumn, true);
        } else if (opt.equalsIgnoreCase("CriteriaWithSubQuery")) {
          builder.config(NormalizationOptions.CriteriaWithSubQuery, true);
        } else if (opt.equalsIgnoreCase("CriteriaWithNot")) {
          builder.config(NormalizationOptions.CriteriaWithNot, true);
        } else if (opt.equalsIgnoreCase("Criteria")) {
          builder.config(NormalizationOptions.Criteria, true);
        } else if (opt.equalsIgnoreCase("ResolveTableAlias")) {
          builder.config(NormalizationOptions.ResolveTableAlias, true);
        } else if (opt.equalsIgnoreCase("FixUniqueAlias")) {
          builder.config(NormalizationOptions.FixUniqueAlias, true);
        } else if (opt.equalsIgnoreCase("CriteriaInJoin")) {
          builder.config(NormalizationOptions.CriteriaInJoin, true);
        } else if (opt.equalsIgnoreCase("OperationExpression")) {
          builder.config(NormalizationOptions.OperationExpression, true);
        } else if (opt.equalsIgnoreCase("AppendNestedQueryTableName")) {
          builder.config(NormalizationOptions.AppendNestedQueryTableName, true);
        } else if (opt.equalsIgnoreCase("RightJoin")) {
          builder.config(NormalizationOptions.RightJoin, true);
        } else if (opt.equalsIgnoreCase("MinimizeCriteria")) {
          builder.config(NormalizationOptions.MinimizeCriteria, true);
        } else if (opt.equalsIgnoreCase("FormulaAlias")) {
          builder.config(NormalizationOptions.FormulaAlias, true);
        } else if (opt.equalsIgnoreCase("NestedJoin")) {
          builder.config(NormalizationOptions.NestedJoin, true);
        } else if (opt.equalsIgnoreCase("Distinct")) {
          builder.config(NormalizationOptions.Distinct, true);
        } else if (opt.equalsIgnoreCase("Count_Distinct")) {
          builder.config(NormalizationOptions.Count_Distinct, true);
        } else if (opt.equalsIgnoreCase("PredictTrue")) {
          builder.config(NormalizationOptions.PredictTrue, true);
        } else if (opt.equalsIgnoreCase("EquiInnerJoin")) {
          builder.config(NormalizationOptions.EquiInnerJoin, true);
        } else if (opt.equalsIgnoreCase("SemiAntiJoin")) {
          builder.config(NormalizationOptions.SemiAntiJoin, true);
        } else if (opt.equalsIgnoreCase("FunctionSubstitute")) {
          builder.config(NormalizationOptions.FunctionSubstitute, true);
        } else if (opt.equalsIgnoreCase("RemoveDistinctIfColumnUnique")) {
          builder.config(NormalizationOptions.RemoveDistinctIfColumnUnique, true);
        }
      }
    }
    builder.config(NormalizationOptions.FormulaAlias, true);
    builder.config(NormalizationOptions.ResolveTableAlias, true);
    return builder.build();
  }

  public static final String[] setImplicitJoin(String [] DEFAULT_NORMALIZATIONS, String implicitJoinType) {
    String[] result = DEFAULT_NORMALIZATIONS;
    String temp = Utilities.toUpperCase(implicitJoinType);
    if (temp != null && temp.startsWith("IMPLICIT")) {
      result = new String[DEFAULT_NORMALIZATIONS.length];
      for (int i = 0 ; i < DEFAULT_NORMALIZATIONS.length; ++i) {
        String optionName = Utilities.toUpperCase(DEFAULT_NORMALIZATIONS[i]);
        if (optionName.startsWith("IMPLICIT")) {
          result[i] = implicitJoinType;
        } else {
          result[i] = optionName;
        }
      }
    }
    return result;
  }

  public static final String[] AddToDefaultNormalizations(String normalizations) {
    Vector<String> modifiedOptions = new Vector<String>();
    if(normalizations != null) {
      String[] add= Utilities.splitString(normalizations.toUpperCase(),",");
      int total = DEFAULT_NORMALIZATIONS.length;
      for(int i=0;i<DEFAULT_NORMALIZATIONS.length;i++){
        modifiedOptions.add(DEFAULT_NORMALIZATIONS[i].toUpperCase());
      }
      for(String s : add) {
        if(s.startsWith("-")) modifiedOptions.remove(s.substring(1));
        else if(s.startsWith("+")) modifiedOptions.add(s.substring(1));
        else modifiedOptions.add(s);
      }
    }
    String[] retValue = new String[modifiedOptions.size()];
    for(int i =0; i<modifiedOptions.size(); i++) {
      retValue[i] = modifiedOptions.elementAt(i);
    }
    return retValue;
  }

  public static final String[] addToNormalizations(String [] normalizations, String addings) {
    String[] add = Utilities.splitString(addings, ",");

    if (null == normalizations) {
      normalizations = new String[0];
    }
    int total = normalizations.length + add.length;
    String[] result = new String[total];
    for(int i=0;i<normalizations.length;i++){
      result[i] = normalizations[i];
    }
    for(int i=normalizations.length, j=0;i<total;i++, j++){
      result[i] = add[j];
    }
    return result;
  }

  public NormalizationOption(NormalizationOptionBuilder builder) {
    this._implicitCommaJoin = builder.getConfigAsBoolean(NormalizationOptions.ImplicitCommaJoin);
    this._implicitNaturalJoin = builder.getConfigAsBoolean(NormalizationOptions.ImplicitNaturalJoin);
    this._implicitCrossJoin = builder.getConfigAsBoolean(NormalizationOptions.ImplicitCrossJoin);
    this._implicitInnerJoin = builder.getConfigAsBoolean(NormalizationOptions.ImplicitInnerJoin);
    this._tableExprWithQuery = builder.getConfigAsBoolean(NormalizationOptions.TableExprWithQuery);
    this._criteriaWithSubQuery = builder.getConfigAsBoolean(NormalizationOptions.CriteriaWithSubQuery);
    this._criteriaWithNot = builder.getConfigAsBoolean(NormalizationOptions.CriteriaWithNot);
    this._criteria = builder.getConfigAsBoolean(NormalizationOptions.Criteria);
    this._resolveTableAlias = builder.getConfigAsBoolean(NormalizationOptions.ResolveTableAlias);
    this._fixUniqueAlias = builder.getConfigAsBoolean(NormalizationOptions.FixUniqueAlias);
    this._criteriaInJoin = builder.getConfigAsBoolean(NormalizationOptions.CriteriaInJoin);
    this._operationExpression = builder.getConfigAsBoolean(NormalizationOptions.OperationExpression);
    this._appendNestedQueryTableName = builder.getConfigAsBoolean(NormalizationOptions.AppendNestedQueryTableName);
    this._rightJoin = builder.getConfigAsBoolean(NormalizationOptions.RightJoin);
    this._minimizeCriteria = builder.getConfigAsBoolean(NormalizationOptions.MinimizeCriteria);
    this._constantColumn = builder.getConfigAsBoolean(NormalizationOptions.ConstantColumn);
    this._formulaAlias = builder.getConfigAsBoolean(NormalizationOptions.FormulaAlias);
    this._nestedJoin = builder.getConfigAsBoolean(NormalizationOptions.NestedJoin);
    this._distinct = builder.getConfigAsBoolean(NormalizationOptions.Distinct);
    this._count_distinct = builder.getConfigAsBoolean(NormalizationOptions.Count_Distinct);
    this._predictTrue = builder.getConfigAsBoolean(NormalizationOptions.PredictTrue);
    this._equiInnerJoin = builder.getConfigAsBoolean(NormalizationOptions.EquiInnerJoin);
    this._semi_anti_join = builder.getConfigAsBoolean(NormalizationOptions.SemiAntiJoin);
    this._functionSubstitute = builder.getConfigAsBoolean(NormalizationOptions.FunctionSubstitute);
    this._removeDistinctIfColumnUnique = builder.getConfigAsBoolean(NormalizationOptions.RemoveDistinctIfColumnUnique);
  }

  public boolean normalizeImplicitCommaJoin() {
    return this._implicitCommaJoin;
  }

  public boolean normalizeImplicitCrossJoin() {
    return this._implicitCrossJoin;
  }

  public boolean normalizeImplicitInnerJoin() {
    return this._implicitInnerJoin;
  }

  public boolean normalizeImplicitNaturalJoin() {
    return this._implicitNaturalJoin;
  }

  public boolean normalizeTableExprWithQuery() {
    return this._tableExprWithQuery;
  }

  public boolean normalizeCriteriaWithSubQuery() {
    return _criteriaWithSubQuery;
  }

  public boolean normalizeCriteriaWithNot() {
    return _criteriaWithNot;
  }

  public boolean normalizeResolveTableAlias() {
    return _resolveTableAlias;
  }

  public boolean normalizeFixUniqueAlias() {
    return this._fixUniqueAlias;
  }

  public boolean normalizeCriteria() {
    return this._criteria;
  }

  public boolean normalizeCriteriaInJoin() {
    return this._criteriaInJoin;
  }

  public boolean normalizeOperationExpression() {
    return this._operationExpression;
  }

  public boolean normalizeNestedQueryTableName() {
    return this._appendNestedQueryTableName;
  }

  public boolean normalizeRightJoin() {
    return this._rightJoin;
  }

  public boolean normalizeMinimizeCriteria() {
    return this._minimizeCriteria;
  }

  public boolean normalizeConstantColumn() {
    return this._constantColumn;
  }

  public boolean normalizeFormulaAlias() {
    return this._formulaAlias;
  }

  public boolean normalizeNestedJoin() {
    return this._nestedJoin;
  }

  public boolean normalizeDistinct() {
    return this._distinct;
  }

  public boolean normalizeCountDistinct() {
    return this._count_distinct;
  }

  public boolean normalizePredictTrue() {
    return this._predictTrue;
  }

  public boolean normalizeEquiInnerJoin() {
    return this._equiInnerJoin;
  }

  public boolean normalizedSemiAntiJoin() {
    return this._semi_anti_join;
  }

  public boolean normalizedFunctionSubstitute() {
    return this._functionSubstitute;
  }

  public boolean normalizeRemoveDistinctIfColumnUnique() {
    return this._removeDistinctIfColumnUnique;
  }
}
