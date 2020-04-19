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


public sealed class SqlSubQueryExpression : SqlExpression{
  private readonly SqlQueryStatement query;

  public SqlSubQueryExpression(SqlQueryStatement subQuery) {
    this.query = subQuery;
  }

  public SqlQueryStatement GetQuery() {
    return this.query;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    //Do nothing now.
    return SqlValue.GetNullValueInstance();
  }

  public override Object Clone() {
    return new SqlSubQueryExpression((SqlQueryStatement)query.Clone());
  }

  protected override void Copy(SqlExpression obj) {
  }
}
}

