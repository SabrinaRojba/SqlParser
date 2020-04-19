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


abstract public class SqlConditionNode : SqlExpression , ISqlElement {
  public abstract bool IsLeaf();
  public abstract void Prepare() ;
  public abstract void Accept(ISqlQueryVisitor visitor) ;

  protected override void Copy(SqlExpression obj) {
  }
}
}

