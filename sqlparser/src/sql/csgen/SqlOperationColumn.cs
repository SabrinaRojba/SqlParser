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


public sealed class SqlOperationColumn : SqlColumn , ISqlElement {
  private readonly SqlOperationExpression expr;

  public SqlOperationColumn(SqlOperationExpression val) : base(null, null) {
    this.expr = val;
  }

  public SqlOperationColumn(string alias, SqlOperationExpression val) : base(alias, alias) {
    this.expr = val;
  }

  public override SqlExpression GetExpr() {
    return this.expr;
  }

  public override bool Equals(string name) {
    return false;
  }

  public override bool IsEvaluatable() {
    return expr != null && expr.IsEvaluatable();
  }

  public override SqlValue Evaluate() {
    if (IsEvaluatable()) {
      return expr.Evaluate();
    }
    return SqlValue.GetNullValueInstance();
  }

  public override Object Clone() {
    SqlOperationColumn obj = new SqlOperationColumn(this.GetColumnName(), this.expr);
    obj.Copy(this);
    return obj;
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

