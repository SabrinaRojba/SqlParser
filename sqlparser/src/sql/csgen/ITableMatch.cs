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
interface ITableMatch {
  SqlTable Create(SqlTable t);
  bool Accept(SqlTable t, TablePartType type);
  bool Unwind(SqlTable t, TablePartType type);
}
}

