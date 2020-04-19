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


public class SqlCriteria : SqlConditionNode {
  public const string CUSTOM_OP_PREDICT_TRUE = "PREDICT TRUE";
  private readonly SqlExpression left;
  private readonly ComparisonType oper;
  private readonly SqlExpression right;
  private readonly string customOp;
  private readonly SqlExpression esc;

  public SqlCriteria(SqlExpression left, ComparisonType op, string customOp, SqlExpression right, SqlExpression escape) {
    this.left = left;
    this.oper = op;
    this.customOp = customOp;
    this.right = right;
    this.esc = escape;
  }

  public SqlCriteria(SqlExpression left, ComparisonType op, SqlExpression right) : this(left, op, null, right) {
  }

  public SqlCriteria(SqlExpression left, string customOp, SqlExpression right) : this(left, ComparisonType.CUSTOM, customOp, right) {
  }

  public SqlCriteria(SqlExpression left, ComparisonType op, string customOp, SqlExpression right) : this(left, op, customOp, right, null) {
  }

  public SqlCriteria(SqlExpression left, SqlExpression right, SqlExpression escape) : this(left, ComparisonType.LIKE, null, right, escape) {
  }

  public override  bool IsLeaf() { return true; }

  public override void Prepare() {
    //TODO: Felix.
  }

  public override  bool IsEvaluatable() {
    if (left != null && right != null) {
      if (left.IsEvaluatable() && right.IsEvaluatable()) {
        return true;
      }
    } else if (ComparisonType.FALSE == oper) {
      return true;
    }
    return false;
  }

  public override SqlValue Evaluate() {
    if (IsEvaluatable()) {
      int l = 0;
      int r = 0;
      SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
      SqlValue falseValue = new SqlValue(SqlValueType.BOOLEAN, "FALSE");
      if (ComparisonType.FALSE == oper) {
        return falseValue;
      } else if ((ComparisonType.IS == oper || ComparisonType.IS_NOT == oper)) {
        try {
          SqlValueType rightValueType = right.Evaluate().GetValueType();
          if (rightValueType == SqlValueType.NULL) {
            Object leftValue = left.Evaluate().GetOriginalValue();
            if (ComparisonType.IS_NOT == oper) {
              return leftValue != null ? trueValue : falseValue;
            } else {
              return leftValue == null ? trueValue : falseValue;
            }
          } else if (rightValueType == SqlValueType.BOOLEAN) {
            SqlValue v = left.Evaluate();
            bool leftValue = false;
            if (v != SqlValue.GetNullValueInstance()) {
              leftValue = left.Evaluate().GetValueAsBool(false);
            }
            if (ComparisonType.IS_NOT == oper) {
              return !leftValue ? trueValue : falseValue;
            } else {
              return leftValue ? trueValue : falseValue;
            }
          } else {
            return falseValue;
          }
        } catch (Exception ex) {
          return falseValue;
        }
      } else {
        try {
          l = left.Evaluate().GetValueAsInt(0);
          r = right.Evaluate().GetValueAsInt(0);
        } catch (Exception ex) {
          return falseValue;
        }
        if (oper == ComparisonType.EQUAL) {
          return l == r ? trueValue : falseValue;
        } else if (oper == ComparisonType.BIGGER_EQUAL) {
          return l >= r ? trueValue : falseValue;
        } else if (oper == ComparisonType.BIGGER) {
          return l > r ? trueValue : falseValue;
        } else if (oper == ComparisonType.SMALLER_EQUAL) {
          return l <= r ? trueValue : falseValue;
        } else if (oper == ComparisonType.SMALLER) {
          return l < r ? trueValue : falseValue;
        } else if (oper == ComparisonType.NOT_EQUAL) {
          return l != r ? trueValue : falseValue;
        }
      }
      return falseValue;
    }
    return SqlValue.GetNullValueInstance();
  }

  public SqlExpression GetLeft() {
    return this.left;
  }

  public SqlExpression GetRight() {
    return this.right;
  }

  public bool IsLeftParameter() {
    return ParserCore.IsParameterExpression(left);
  }

  public bool IsRightParameter() {
    return ParserCore.IsParameterExpression(right);
  }

  public bool IsRightValueNull() {
    if (right != null) {
      return right.Evaluate().GetValueType() == SqlValueType.NULL;
    }
    return true;
  }

  public string GetResolvedLeftName() {
    if (left is SqlColumn) {
      return ((SqlColumn)left).GetColumnName();
    } else if (ParserCore.IsParameterExpression(left)) {
      return ((SqlValueExpression)left).GetParameterName();
    }
    return null;
  }

  public string GetResolvedRightName() {
    if (right is SqlColumn) {
      return ((SqlColumn)right).GetColumnName();
    } else if (ParserCore.IsParameterExpression(right)) {
      return ((SqlValueExpression)right).GetParameterName();
    }
    return null;
  }

  public ComparisonType GetOperator() {
    return this.oper;
  }

  public string GetCustomOp() {
    return this.customOp;
  }

  public SqlExpression GetEscape() {
    return this.esc;
  }

  public string GetOperatorAsString() {
    if (ComparisonType.EQUAL == oper) {
      return "=";
    } else if (ComparisonType.BIGGER_EQUAL == oper) {
      return ">=";
    } else if (ComparisonType.BIGGER == oper) {
      return ">";
    } else if (ComparisonType.SMALLER_EQUAL == oper) {
      return "<=";
    } else if (ComparisonType.SMALLER == oper) {
      return "<";
    } else if (ComparisonType.NOT_EQUAL == oper) {
      return "!=";
    } else if (ComparisonType.LIKE == oper) {
      return "LIKE";
    } else if (ComparisonType.IS_NOT == oper) {
      return "IS NOT";
    } else if (ComparisonType.IS == oper) {
      return "IS";
    } else if (ComparisonType.FALSE == oper) {
      return "FALSE";
    } else if (ComparisonType.IN == oper) {
      return "IN";
    }  else if (ComparisonType.NOT_IN == oper) {
      return "NOT IN";
    } else if (ComparisonType.CUSTOM == oper) {
      return this.customOp;
    } else if (ComparisonType.CONTAINS == oper) {
      return "CONTAINS";
    }
    return null;
  }
  
  public string GetOperatorAsEnglishString() {
    if (ComparisonType.EQUAL == oper) {
      return "equal";
    } else if (ComparisonType.BIGGER_EQUAL == oper) {
      return "bigger_equal";
    } else if (ComparisonType.BIGGER == oper) {
      return "bigger";
    } else if (ComparisonType.SMALLER_EQUAL == oper) {
      return "smaller_equal";
    } else if (ComparisonType.SMALLER == oper) {
      return "smaller";
    } else if (ComparisonType.NOT_EQUAL == oper) {
      return "not_equal";
    } else if (ComparisonType.LIKE == oper) {
      return "like";
    } else if (ComparisonType.IS_NOT == oper) {
      return "is_not";
    } else if (ComparisonType.IS == oper) {
      return "is";
    } else if (ComparisonType.FALSE == oper) {
      return "false";
    } else if (ComparisonType.IN == oper) {
      return "in";
    }  else if (ComparisonType.NOT_IN == oper) {
      return "not_in";
    } else if (ComparisonType.CUSTOM == oper) {
      return this.customOp;
    } else if (ComparisonType.CONTAINS == oper) {
      return "contains";
    }
    return null;
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
  }

  public override Object Clone() {
    return new SqlCriteria(this.left, this.oper, this.customOp, this.right, this.esc);
  }

  public static ComparisonType GetCompareType(string oper) {
    if ("=".Equals(oper)) {
      return ComparisonType.EQUAL;
    } else if (">".Equals(oper)) {
      return ComparisonType.BIGGER;
    } else if (">=".Equals(oper)) {
      return ComparisonType.BIGGER_EQUAL;
    } else if ("<".Equals(oper)) {
      return ComparisonType.SMALLER;
    } else if ("<=".Equals(oper)) {
      return ComparisonType.SMALLER_EQUAL;
    } else if ("=".Equals(oper)) {
      return ComparisonType.EQUAL;
    } else if ("!=".Equals(oper)) {
      return ComparisonType.NOT_EQUAL;
    } else if ("<>".Equals(oper)) {
      return ComparisonType.NOT_EQUAL;
    } else if ("<=>".Equals(oper)) {
      return ComparisonType.EQUAL;
    } else if (Utilities.EqualIgnoreCaseInvariant("CONTAINS", oper)) {
      return ComparisonType.CONTAINS;
    } else {
      return ComparisonType.NONE;
    }
  }

}
}

