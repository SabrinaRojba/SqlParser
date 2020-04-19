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


public sealed class SqlConditionInSelect : SqlCriteria {
  private  readonly  bool isAll;
  private  readonly  SqlQueryStatement query;
  public SqlConditionInSelect(SqlExpression left, SqlQueryStatement query, bool isAll, ComparisonType type) : base(left, type, new SqlSubQueryExpression(query)) {
    this.query = query;
    this.isAll = isAll;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override bool IsLeaf() {
    return true;
  }

  public override void Prepare() {
    // Do nothing now.
  }

  public override void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
  }

  public SqlQueryStatement GetRightQuery() {
    return this.query;
  }

  public bool IsAll() {
    return this.isAll;
  }

  public override Object Clone() {
    return new SqlConditionInSelect(GetLeft(), GetRightQuery(), IsAll(), GetOperator());
  }
}
}

