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

public enum ComparisonType
{
NONE,
EQUAL,
BIGGER_EQUAL,
BIGGER,
SMALLER_EQUAL,
SMALLER,
NOT_EQUAL,
IS_NOT,
/* IS NOT */IS,
/* IS */FALSE,
/* WHERE FALSE*/LIKE,
IN,
/* IN (,,)*/NOT_IN,
CONTAINS,
CUSTOM,
}

}

