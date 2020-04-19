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


public abstract class SqlExpression : ISqlCloneable {
  public abstract bool IsEvaluatable();
  public abstract SqlValue Evaluate();
  public abstract Object Clone();
  protected abstract void Copy(SqlExpression obj);
}
}

