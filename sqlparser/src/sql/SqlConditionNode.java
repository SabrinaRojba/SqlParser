//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

abstract public class SqlConditionNode extends SqlExpression implements ISqlElement {
  public abstract boolean isLeaf();
  public abstract void prepare() throws Exception;
  public abstract void accept(ISqlQueryVisitor visitor) throws Exception;

  @Override
  protected void copy(SqlExpression obj) {
  }
}