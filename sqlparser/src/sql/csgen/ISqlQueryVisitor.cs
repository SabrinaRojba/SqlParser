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


public interface ISqlQueryVisitor {
  void Visit(SqlTable table) ;
  void Visit(SqlStatement stmt) ;
  void Visit(SqlColumn column) ;
  void Visit(SqlConditionNode criteria) ;
}
}

