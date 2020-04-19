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


public sealed class SqlExpressionList : SqlExpression {
  private readonly SqlExpression[] list;
  public SqlExpressionList(SqlExpression[] list) {
    this.list = list;
  }

  public SqlExpression[] GetExprList() {
    return list;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    return SqlValue.GetNullValueInstance();
  }

  public override Object Clone() {
    SqlExpressionList obj = new SqlExpressionList(list);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlExpression obj) {
  }
}
}

