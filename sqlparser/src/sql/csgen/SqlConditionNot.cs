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


public sealed class SqlConditionNot : SqlConditionNode {
  private  readonly  SqlExpression condition;

  public SqlConditionNot(SqlExpression c) {
    condition = c;
  }

  public override bool IsLeaf() {
    return true;
  }

  public override void Prepare() {
    if (condition is SqlConditionNode) {
      SqlConditionNode c = (SqlConditionNode)condition;
      c.Prepare();
    }
  }

  public override bool IsEvaluatable() {
    return condition.IsEvaluatable();
  }

  public override SqlValue Evaluate() {
    if (condition.IsEvaluatable()) {
      SqlValue value = condition.Evaluate();
      if (SqlValueType.BOOLEAN == value.GetValueType()) {
        bool b = false;
        try {
          b = value.GetValueAsBool(false);
        } catch (Exception ex) {;}
        if (b) {
          return new SqlValue(SqlValueType.BOOLEAN, "FALSE");
        } else {
          return new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        }
      }
    }
    return SqlValue.GetNullValueInstance();
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    if (this.condition is SqlConditionNode) {
      ((SqlConditionNode) this.condition).Accept(visitor);
    }
  }

  public SqlExpression GetCondition() {
    return condition;
  }

  public override Object Clone() {
    SqlExpression c = GetCondition();
    if (c is SqlConditionNode) {
      c = (SqlExpression)((SqlConditionNode)c).Clone();
    }
    return new SqlConditionNot(c);
  }
}
}

