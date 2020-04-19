//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

import core.ParserCore;
import rssbus.oputils.common.Utilities;

public class SqlCriteria extends SqlConditionNode {
  public static final String CUSTOM_OP_PREDICT_TRUE = "PREDICT TRUE";
  private final SqlExpression left;
  private final ComparisonType oper;
  private final SqlExpression right;
  private final String customOp;
  private final SqlExpression esc;

  public SqlCriteria(SqlExpression left, ComparisonType op, String customOp, SqlExpression right, SqlExpression escape) {
    this.left = left;
    this.oper = op;
    this.customOp = customOp;
    this.right = right;
    this.esc = escape;
  }

  public SqlCriteria(SqlExpression left, ComparisonType op, SqlExpression right) {
    this(left, op, null, right);
  }

  public SqlCriteria(SqlExpression left, String customOp, SqlExpression right) {
    this(left, ComparisonType.CUSTOM, customOp, right);
  }

  public SqlCriteria(SqlExpression left, ComparisonType op, String customOp, SqlExpression right) {
    this(left, op, customOp, right, null);
  }

  public SqlCriteria(SqlExpression left, SqlExpression right, SqlExpression escape) {
    this(left, ComparisonType.LIKE, null, right, escape);
  }

  public /*#override#*/  boolean isLeaf() { return true; }

  public /*#override#*/ void prepare() throws Exception {
    //TODO: Felix.
  }

  public /*#override#*/  boolean isEvaluatable() {
    if (left != null && right != null) {
      if (left.isEvaluatable() && right.isEvaluatable()) {
        return true;
      }
    } else if (/*@*/oper != null && /*@*/ComparisonType.FALSE == oper) {
      return true;
    }
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    if (isEvaluatable()) {
      int l = 0;
      int r = 0;
      SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
      SqlValue falseValue = new SqlValue(SqlValueType.BOOLEAN, "FALSE");
      if (/*@*/oper != null && /*@*/ComparisonType.FALSE == oper) {
        return falseValue;
      } else if (/*@*/oper != null && /*@*/(ComparisonType.IS == oper || ComparisonType.IS_NOT == oper)) {
        try {
          SqlValueType rightValueType = right.evaluate().getValueType();
          if (rightValueType == SqlValueType.NULL) {
            Object leftValue = left.evaluate().getOriginalValue();
            if (ComparisonType.IS_NOT == oper) {
              return leftValue != null ? trueValue : falseValue;
            } else {
              return leftValue == null ? trueValue : falseValue;
            }
          } else if (rightValueType == SqlValueType.BOOLEAN) {
            SqlValue v = left.evaluate();
            boolean leftValue = false;
            if (v != SqlValue.getNullValueInstance()) {
              leftValue = left.evaluate().getValueAsBool(false);
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
          l = left.evaluate().getValueAsInt(0);
          r = right.evaluate().getValueAsInt(0);
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
    return SqlValue.getNullValueInstance();
  }

  public SqlExpression getLeft() {
    return this.left;
  }

  public SqlExpression getRight() {
    return this.right;
  }

  public boolean isLeftParameter() {
    return ParserCore.isParameterExpression(left);
  }

  public boolean isRightParameter() {
    return ParserCore.isParameterExpression(right);
  }

  public boolean isRightValueNull() throws Exception {
    if (right != null) {
      return right.evaluate().getValueType() == SqlValueType.NULL;
    }
    return true;
  }

  public String getResolvedLeftName() {
    if (left instanceof SqlColumn) {
      return ((SqlColumn)left).getColumnName();
    } else if (ParserCore.isParameterExpression(left)) {
      return ((SqlValueExpression)left).getParameterName();
    }
    return null;
  }

  public String getResolvedRightName() {
    if (right instanceof SqlColumn) {
      return ((SqlColumn)right).getColumnName();
    } else if (ParserCore.isParameterExpression(right)) {
      return ((SqlValueExpression)right).getParameterName();
    }
    return null;
  }

  public ComparisonType getOperator() {
    return this.oper;
  }

  public String getCustomOp() {
    return this.customOp;
  }

  public SqlExpression getEscape() {
    return this.esc;
  }

  public String getOperatorAsString() {
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
  
  public String getOperatorAsEnglishString() {
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

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
  }

  public /*#override#*/ Object clone() {
    return new SqlCriteria(this.left, this.oper, this.customOp, this.right, this.esc);
  }

  public static ComparisonType getCompareType(String oper) {
    if ("=".equals(oper)) {
      return ComparisonType.EQUAL;
    } else if (">".equals(oper)) {
      return ComparisonType.BIGGER;
    } else if (">=".equals(oper)) {
      return ComparisonType.BIGGER_EQUAL;
    } else if ("<".equals(oper)) {
      return ComparisonType.SMALLER;
    } else if ("<=".equals(oper)) {
      return ComparisonType.SMALLER_EQUAL;
    } else if ("=".equals(oper)) {
      return ComparisonType.EQUAL;
    } else if ("!=".equals(oper)) {
      return ComparisonType.NOT_EQUAL;
    } else if ("<>".equals(oper)) {
      return ComparisonType.NOT_EQUAL;
    } else if ("<=>".equals(oper)) {
      return ComparisonType.EQUAL;
    } else if (Utilities.equalIgnoreCaseInvariant("CONTAINS", oper)) {
      return ComparisonType.CONTAINS;
    } else {
      return ComparisonType.NONE;
    }
  }

}
