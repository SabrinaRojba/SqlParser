//@
package cdata.sql;
//@

/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

public abstract class SqlExpression implements ISqlCloneable {
  public abstract boolean isEvaluatable();
  public abstract SqlValue evaluate();
  public abstract Object clone();
  protected abstract void copy(SqlExpression obj);
}