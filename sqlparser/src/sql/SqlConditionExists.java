//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public /*@*/final/*@*//*#sealed#*/ class SqlConditionExists extends SqlConditionNode {
  private /*@*/final/*@*/ /*#readonly#*/ SqlQueryStatement subQuery;
  public SqlConditionExists(SqlQueryStatement query) {
    subQuery = query;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
  }


  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    return SqlValue.getNullValueInstance();
  }

  public /*#override#*/ boolean isLeaf() { return false; }

  public /*#override#*/ void prepare() throws Exception {
    //Do nothing.
  }

  public SqlQueryStatement getSubQuery() {
    return subQuery;
  }

  public /*#override#*/ Object clone() {
    return new SqlConditionExists(getSubQuery());
  }
}
