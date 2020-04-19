using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



namespace CData.Sql {


public interface ISqlElementComparable  <T> where T : ISqlElement {
   int CompareTo(T o);
}
}

