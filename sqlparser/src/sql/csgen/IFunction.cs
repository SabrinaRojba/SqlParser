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


public 
interface IFunction {
  Object Evaluate(Object[] parameters) ;
  string GetName();
  int GetReturnDataType();
  int GetMaxArgs();
  int GetMinArgs();
  bool IsServerSideFunction();
  bool IsAggregateFunction();
}
}

