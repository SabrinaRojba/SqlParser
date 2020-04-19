//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlOrderSpec implements ISqlCloneable {
  private /*@*/final/*@*/ /*#readonly#*/ SqlExpression column;
  private /*@*/final/*@*/ /*#readonly#*/ SortOrder order;
  private /*@*/final/*@*/ /*#readonly#*/ boolean isNullsFirst;
  private /*@*/final/*@*/ /*#readonly#*/ boolean hasNulls;

  public SqlOrderSpec(SqlExpression col, SortOrder order, boolean isNullsFirst, boolean hasNulls) {
    this.column = col;
    this.order = order;
    this.isNullsFirst = isNullsFirst;
    this.hasNulls = hasNulls;
  }

  public String getResolvedColumnName() {
    if (column instanceof SqlColumn) {
      return ((SqlColumn) column).getColumnName();
    } else if (column instanceof SqlValueExpression) {
      return ((SqlValueExpression) column).evaluate().getValueAsString("");
    }
    return null;
  }

  public SortOrder getOrder() {
    return order;
  }

  public SqlExpression getExpr() {
    return this.column;
  }

  public boolean isNullsFirst() {
    return this.isNullsFirst;
  }

  public boolean hasNulls() {
    return this.hasNulls;
  }

  public Object clone() {
    return new SqlOrderSpec(this.column, this.order, this.isNullsFirst, this.hasNulls);
  }
}
