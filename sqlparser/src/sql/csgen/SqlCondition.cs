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


public sealed class SqlCondition : SqlConditionNode {
  private  readonly  SqlLogicalOperator logicOp;
  private  readonly  SqlExpression lhs, rhs;

  public SqlCondition(SqlExpression lhs, SqlLogicalOperator op, SqlExpression rhs)  {
    this.lhs = lhs;
    this.logicOp = op;
    this.rhs = rhs;
  }

  public override bool IsEvaluatable() {
    if (lhs != null && rhs != null) {
      if (rhs.IsEvaluatable() && lhs.IsEvaluatable()) {
        return true;
      } else if (rhs.IsEvaluatable()) {
        if (SqlLogicalOperator.And == this.GetLogicOp()) {
          return IsFalseValue(rhs);
        } else if (SqlLogicalOperator.Or == this.GetLogicOp()) {
          return IsTrueValue(rhs);
        }
      } else if (lhs.IsEvaluatable()) {
        if (SqlLogicalOperator.And == this.GetLogicOp()) {
          return IsFalseValue(lhs);
        } else if (SqlLogicalOperator.Or == this.GetLogicOp()) {
          return IsTrueValue(lhs);
        }
      }
    }
    return false;
  }

  public override SqlValue Evaluate() {
    if (IsEvaluatable()) {
      SqlValue trueValue = new SqlValue(SqlValueType.BOOLEAN, "TRUE");
      SqlValue falseValue = new SqlValue(SqlValueType.BOOLEAN, "FALSE");
      if (logicOp == SqlLogicalOperator.And) {
        if ((lhs.IsEvaluatable() && IsFalseValue(lhs))
          || (rhs.IsEvaluatable() && IsFalseValue(rhs))) {
          return falseValue;
        } else {
          if ((lhs.IsEvaluatable() && IsTrueValue(lhs))
            && (rhs.IsEvaluatable() && IsTrueValue(rhs))) {
            return trueValue;
          }
        }
      } else if (logicOp == SqlLogicalOperator.Or) {
        if ((lhs.IsEvaluatable() && IsTrueValue(lhs))
          || (rhs.IsEvaluatable() && IsTrueValue(rhs))) {
          return trueValue;
        } else {
          return falseValue;
        }
      } else {
        return falseValue;
      }
    }
    return SqlValue.GetNullValueInstance();
  }

  public override bool IsLeaf() { return false; }

  public override void Prepare() {
    if (lhs is SqlConditionNode) {
      ((SqlConditionNode)lhs).Prepare();
    }
    if (rhs is SqlConditionNode) {
      ((SqlConditionNode)rhs).Prepare();
    }
  }

  public SqlExpression GetLeft() {
    return this.lhs;
  }

  public SqlExpression GetRight() {
    return this.rhs;
  }

  public SqlLogicalOperator GetLogicOp() {
    return logicOp;
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    SqlExpression l = this.GetLeft();
    SqlExpression r = this.GetRight();
    if (l is SqlCondition) {
      ((SqlCondition) l).Accept(visitor);
    } else if (l is SqlConditionNode) {
      visitor.Visit((SqlConditionNode) l);
    }

    if (r is SqlCondition) {
      ((SqlCondition) r).Accept(visitor);
    } else if (r is SqlConditionNode) {
      visitor.Visit((SqlConditionNode) r);
    }
  }

  public override Object Clone() {
    SqlExpression l = GetLeft();
    SqlExpression r = GetRight();
    if (l is SqlConditionNode) {
      l = (SqlExpression)((SqlConditionNode)l).Clone();
    }
    if (r is SqlConditionNode) {
      r = (SqlExpression)((SqlConditionNode)r).Clone();
    }
    return new SqlCondition(l, GetLogicOp(), r);
  }

  private static bool IsFalseValue(SqlExpression expr) {
    bool isFalse = false;
    try {
      SqlValue v = expr.Evaluate();
      if (v != null) {
        if (SqlValueType.BOOLEAN == v.GetValueType()) {
          isFalse = false == v.GetValueAsBool(true);
        }
      }
    } catch (Exception ex) {}
    return isFalse;
  }

  private static bool IsTrueValue(SqlExpression expr) {
    bool isTrue = false;
    try {
      SqlValue v = expr.Evaluate();
      if (v != null) {
        if (SqlValueType.BOOLEAN == v.GetValueType()) {
          isTrue = true == v.GetValueAsBool(false);
        }
      }
    } catch (Exception ex) {}
    return isTrue;
  }
}
}

