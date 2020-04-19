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


public sealed class SqlOperationExpression : SqlExpression {
  private readonly SqlOperationType type;
  private readonly SqlExpression left;
  private readonly SqlExpression right;
  public SqlOperationExpression(SqlOperationType t, SqlExpression l, SqlExpression r) {
    type = t;
    left = l;
    right = r;
  }

  public SqlOperationType GetOperator() {
    return type;
  }

  public string GetOperatorAsString() {
    if (SqlOperationType.PLUS == type) {
      return "+";
    } else if (SqlOperationType.MINUS == type) {
      return "-";
    } else if (SqlOperationType.MULTIPLY == type) {
      return "*";
    } else if (SqlOperationType.DIVIDE == type) {
      return "/";
    } else if (SqlOperationType.MODULUS == type) {
      return "%";
    } else if (SqlOperationType.CONCAT == type) {
      return "||";
    }
    return null;
  }

  public override bool IsEvaluatable() {
    if (left != null && right != null) {
      return left.IsEvaluatable() && right.IsEvaluatable();
    }
    return false;
  }

  public override SqlValue Evaluate() {
    double l, r, value;
    string strL, strR;
    if (IsEvaluatable()) {
      SqlValue lv = left.Evaluate();
      if (lv == null || SqlValueType.NULL == lv.GetValueType()) {
        return SqlValue.GetNullValueInstance();
      }

      SqlValue rv = right.Evaluate();
      if (rv == null || SqlValueType.NULL == rv.GetValueType()) {
        return SqlValue.GetNullValueInstance();
      }
      strL = left.Evaluate().GetValueAsString(null);
      strR = right.Evaluate().GetValueAsString(null);
      if (SqlOperationType.CONCAT == type) {
        return new SqlValue(SqlValueType.STRING, strL + strR);
      }

      l = 0;
      try {
        l = Utilities.GetValueAsDouble(strL, 0);
      } catch (Exception ex) {;}

      r = 0;
      try {
        r = Utilities.GetValueAsDouble(strR, 0);
      } catch (Exception ex) {;}

      if (SqlOperationType.PLUS == type) {
        value = l + r;
      } else if (SqlOperationType.MINUS == type) {
        value = l - r;
      } else if (SqlOperationType.MODULUS == type) {
        value = l % r;
      } else if (SqlOperationType.MULTIPLY == type) {
        value = l * r;
      } else if (SqlOperationType.DIVIDE == type) {
        value = l / r;
      } else {
        value = 0;
      }
      return new SqlValue(ColumnInfo.DATA_TYPE_DOUBLE, value, null);
    }
    return SqlValue.GetNullValueInstance();
  }

  public SqlExpression GetLeft() {
    return this.left;
  }

  public SqlExpression GetRight() {
    return this.right;
  }

  public override Object Clone() {
    return new SqlOperationExpression(this.type, this.left, this.right);
  }

  protected override void Copy(SqlExpression obj) {
  }
}
}

