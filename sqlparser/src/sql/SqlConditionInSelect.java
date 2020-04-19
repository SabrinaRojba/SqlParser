//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public /*@*/final/*@*//*#sealed#*/ class SqlConditionInSelect extends SqlCriteria {
  private /*@*/final/*@*/ /*#readonly#*/  boolean isAll;
  private /*@*/final/*@*/ /*#readonly#*/  SqlQueryStatement query;
  public SqlConditionInSelect(SqlExpression left, SqlQueryStatement query, boolean isAll, ComparisonType type) {
    super(left, type, new SqlSubQueryExpression(query));
    this.query = query;
    this.isAll = isAll;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ boolean isLeaf() {
    return true;
  }

  public /*#override#*/ void prepare() throws Exception {
    // Do nothing now.
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
  }

  public SqlQueryStatement getRightQuery() {
    return this.query;
  }

  public boolean isAll() {
    return this.isAll;
  }

  public /*#override#*/ Object clone() {
    return new SqlConditionInSelect(getLeft(), getRightQuery(), isAll(), getOperator());
  }
}
