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


public  interface IFormula {
  SqlFormulaColumn GetColumn();
  IFunction GetFunction();
  Object Evaluate(Object data) ;
}
}

