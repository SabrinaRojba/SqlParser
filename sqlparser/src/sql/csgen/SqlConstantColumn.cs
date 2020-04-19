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


public sealed class SqlConstantColumn : SqlColumn , ISqlElement {
  private readonly SqlValueExpression expr;

  public SqlConstantColumn(SqlValueExpression val) : base(null, null) {
    expr = val;
  }

  public SqlConstantColumn(string alias, SqlValueExpression val) : base(alias, alias) {
    expr = val;
  }

  public override SqlExpression GetExpr() {
    return this.expr;
  }

  public override Object Clone() {
    SqlConstantColumn obj = new SqlConstantColumn(this.GetAlias(), this.expr);
    obj.Copy(this);
    return obj;
  }

  public override bool Equals(string name) {
    return false;
  }

  public override bool IsEvaluatable() {
    return expr != null;
  }

  public override SqlValue Evaluate() {
    if (expr != null) {
      return expr.Evaluate();
    }
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

