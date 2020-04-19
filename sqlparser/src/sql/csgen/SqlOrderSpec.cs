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


public class SqlOrderSpec : ISqlCloneable {
  private  readonly SqlExpression column;
  private  readonly SortOrder order;
  private  readonly bool isNullsFirst;
  private  readonly bool hasNulls;

  public SqlOrderSpec(SqlExpression col, SortOrder order, bool isNullsFirst, bool hasNulls) {
    this.column = col;
    this.order = order;
    this.isNullsFirst = isNullsFirst;
    this.hasNulls = hasNulls;
  }

  public string GetResolvedColumnName() {
    if (column is SqlColumn) {
      return ((SqlColumn) column).GetColumnName();
    } else if (column is SqlValueExpression) {
      return ((SqlValueExpression) column).Evaluate().GetValueAsString("");
    }
    return null;
  }

  public SortOrder GetOrder() {
    return order;
  }

  public SqlExpression GetExpr() {
    return this.column;
  }

  public bool IsNullsFirst() {
    return this.isNullsFirst;
  }

  public bool HasNulls() {
    return this.hasNulls;
  }

  public Object Clone() {
    return new SqlOrderSpec(this.column, this.order, this.isNullsFirst, this.hasNulls);
  }
}
}

