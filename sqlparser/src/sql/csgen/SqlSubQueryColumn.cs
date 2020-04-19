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


public sealed class SqlSubQueryColumn : SqlColumn , ISqlElement {
  private readonly SqlSubQueryExpression expr;

  public SqlSubQueryColumn(SqlSubQueryExpression query) : base(null, null) {
    expr = query;
  }

  public SqlSubQueryColumn(string alias, SqlSubQueryExpression query) : base(alias, alias) {
    expr = query;
  }

  public override SqlExpression GetExpr() {
    return this.expr;
  }

  public override Object Clone() {
    SqlSubQueryColumn obj = new SqlSubQueryColumn(this.GetAlias(), this.expr);
    obj.Copy(this);
    return obj;
  }

  public override bool Equals(string name) {
    return false;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    return SqlValue.GetNullValueInstance();
  }

  public override bool HasAlias() {
    if (GetAlias() != null) {
      return true;
    } else {
      return false;
    }
  }
}
}

