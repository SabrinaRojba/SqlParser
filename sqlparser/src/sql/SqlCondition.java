//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public /*@*/final/*@*//*#sealed#*/ class SqlCondition extends SqlConditionNode {
  private /*@*/final/*@*/ /*#readonly#*/  SqlLogicalOperator logicOp;
  private /*@*/final/*@*/ /*#readonly#*/  SqlExpression lhs, rhs;

  public SqlCondition(SqlExpression lhs, SqlLogicalOperator op, SqlExpression rhs)  {
    this.lhs = lhs;
    this.logicOp = op;
    this.rhs = rhs;
  }

  public /*#override#*/ boolean isEvaluatable() {
    if (lhs != null && rhs != null) {
      if (rhs.isEvaluatable() && lhs.isEvaluatable()) {
        return true;
      } else if (rhs.isEvaluatable()) {
        if (SqlLogicalOperator.And == this.getLogicOp()) {
          return isFalseValue(rhs);
        } else if (SqlLogicalOperator.Or == this.getLogicOp()) {
          return isTrueValue(rhs);
        }
      } else if (lhs.isEvaluatable()) {
        if (SqlLogicalOperator.And == this.getLogicOp()) {
          return isFalseValue(lhs);
        } else if (SqlLogicalOperator.Or == this.getLogicOp()) {
          return isTrueValue(lhs);
        }
      }
    }
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    if (isEvaluatable()) {
      SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
      SqlValue falseValue = new SqlValue(SqlValueType.BOOLEAN, "FALSE");
      if (logicOp == SqlLogicalOperator.And) {
        if ((lhs.isEvaluatable() && isFalseValue(lhs))
          || (rhs.isEvaluatable() && isFalseValue(rhs))) {
          return falseValue;
        } else {
          if ((lhs.isEvaluatable() && isTrueValue(lhs))
            && (rhs.isEvaluatable() && isTrueValue(rhs))) {
            return trueValue;
          }
        }
      } else if (logicOp == SqlLogicalOperator.Or) {
        if ((lhs.isEvaluatable() && isTrueValue(lhs))
          || (rhs.isEvaluatable() && isTrueValue(rhs))) {
          return trueValue;
        } else {
          return falseValue;
        }
      } else {
        return falseValue;
      }
    }
    return SqlValue.getNullValueInstance();
  }

  public /*#override#*/ boolean isLeaf() { return false; }

  public /*#override#*/ void prepare() throws Exception {
    if (lhs instanceof SqlConditionNode) {
      ((SqlConditionNode)lhs).prepare();
    }
    if (rhs instanceof SqlConditionNode) {
      ((SqlConditionNode)rhs).prepare();
    }
  }

  public SqlExpression getLeft() {
    return this.lhs;
  }

  public SqlExpression getRight() {
    return this.rhs;
  }

  public SqlLogicalOperator getLogicOp() {
    return logicOp;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    SqlExpression l = this.getLeft();
    SqlExpression r = this.getRight();
    if (l instanceof SqlCondition) {
      ((SqlCondition) l).accept(visitor);
    } else if (l instanceof SqlConditionNode) {
      visitor.visit((SqlConditionNode) l);
    }

    if (r instanceof SqlCondition) {
      ((SqlCondition) r).accept(visitor);
    } else if (r instanceof SqlConditionNode) {
      visitor.visit((SqlConditionNode) r);
    }
  }

  public /*#override#*/ Object clone() {
    SqlExpression l = getLeft();
    SqlExpression r = getRight();
    if (l instanceof SqlConditionNode) {
      l = (SqlExpression)((SqlConditionNode)l).clone();
    }
    if (r instanceof SqlConditionNode) {
      r = (SqlExpression)((SqlConditionNode)r).clone();
    }
    return new SqlCondition(l, getLogicOp(), r);
  }

  private static boolean isFalseValue(SqlExpression expr) {
    boolean isFalse = false;
    try {
      SqlValue v = expr.evaluate();
      if (v != null) {
        if (SqlValueType.BOOLEAN == v.getValueType()) {
          isFalse = false == v.getValueAsBool(true);
        }
      }
    } catch (Exception ex) {}
    return isFalse;
  }

  private static boolean isTrueValue(SqlExpression expr) {
    boolean isTrue = false;
    try {
      SqlValue v = expr.evaluate();
      if (v != null) {
        if (SqlValueType.BOOLEAN == v.getValueType()) {
          isTrue = true == v.getValueAsBool(false);
        }
      }
    } catch (Exception ex) {}
    return isTrue;
  }
}