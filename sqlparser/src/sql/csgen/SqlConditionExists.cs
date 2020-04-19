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


public sealed class SqlConditionExists : SqlConditionNode {
  private  readonly SqlQueryStatement subQuery;
  public SqlConditionExists(SqlQueryStatement query) {
    subQuery = query;
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
  }


  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    return SqlValue.GetNullValueInstance();
  }

  public override bool IsLeaf() { return false; }

  public override void Prepare() {
    //Do nothing.
  }

  public SqlQueryStatement GetSubQuery() {
    return subQuery;
  }

  public override Object Clone() {
    return new SqlConditionExists(GetSubQuery());
  }
}
}

