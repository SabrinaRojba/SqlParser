//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public /*@*/final/*@*//*#sealed#*/ class SqlConditionNot extends SqlConditionNode {
  private /*@*/final/*@*/ /*#readonly#*/  SqlExpression condition;

  public SqlConditionNot(SqlExpression c) {
    condition = c;
  }

  public /*#override#*/ boolean isLeaf() {
    return true;
  }

  public /*#override#*/ void prepare() throws Exception {
    if (condition instanceof SqlConditionNode) {
      SqlConditionNode c = (SqlConditionNode)condition;
      c.prepare();
    }
  }

  public /*#override#*/ boolean isEvaluatable() {
    return condition.isEvaluatable();
  }

  public /*#override#*/ SqlValue evaluate() {
    if (condition.isEvaluatable()) {
      SqlValue value = condition.evaluate();
      if (SqlValueType.BOOLEAN == value.getValueType()) {
        boolean b = false;
        try {
          b = value.getValueAsBool(false);
        } catch (Exception ex) {;}
        if (b) {
          return new SqlValue(SqlValueType.BOOLEAN, "FALSE");
        } else {
          return new SqlValue(SqlValueType.BOOLEAN, "TRUE");
        }
      }
    }
    return SqlValue.getNullValueInstance();
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    if (this.condition instanceof SqlConditionNode) {
      ((SqlConditionNode) this.condition).accept(visitor);
    }
  }

  public SqlExpression getCondition() {
    return condition;
  }

  public /*#override#*/ Object clone() {
    SqlExpression c = getCondition();
    if (c instanceof SqlConditionNode) {
      c = (SqlExpression)((SqlConditionNode)c).clone();
    }
    return new SqlConditionNot(c);
  }
}
