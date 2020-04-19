//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.SqlExceptions;
import rssbus.oputils.common.Utilities;

public final class SqlOperationExpression extends SqlExpression {
  private final SqlOperationType type;
  private final SqlExpression left;
  private final SqlExpression right;
  public SqlOperationExpression(SqlOperationType t, SqlExpression l, SqlExpression r) {
    type = t;
    left = l;
    right = r;
  }

  public SqlOperationType getOperator() {
    return type;
  }

  public String getOperatorAsString() {
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

  public /*#override#*/ boolean isEvaluatable() {
    if (left != null && right != null) {
      return left.isEvaluatable() && right.isEvaluatable();
    }
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    double l, r, value;
    String strL, strR;
    if (isEvaluatable()) {
      SqlValue lv = left.evaluate();
      if (lv == null || SqlValueType.NULL == lv.getValueType()) {
        return SqlValue.getNullValueInstance();
      }

      SqlValue rv = right.evaluate();
      if (rv == null || SqlValueType.NULL == rv.getValueType()) {
        return SqlValue.getNullValueInstance();
      }
      strL = left.evaluate().getValueAsString(null);
      strR = right.evaluate().getValueAsString(null);
      if (SqlOperationType.CONCAT == type) {
        return new SqlValue(SqlValueType.STRING, strL + strR);
      }

      l = 0;
      try {
        l = Utilities.getValueAsDouble(strL, 0);
      } catch (Exception ex) {;}

      r = 0;
      try {
        r = Utilities.getValueAsDouble(strR, 0);
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
    return SqlValue.getNullValueInstance();
  }

  public SqlExpression getLeft() {
    return this.left;
  }

  public SqlExpression getRight() {
    return this.right;
  }

  @Override
  public Object clone() {
    return new SqlOperationExpression(this.type, this.left, this.right);
  }

  @Override
  protected void copy(SqlExpression obj) {
  }
}
