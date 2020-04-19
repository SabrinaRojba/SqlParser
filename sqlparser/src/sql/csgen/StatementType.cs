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


public enum StatementType
{
/*UNPARSED means no statement was parsed, XML is for UPDATEGRAM, and
UNKNOWN means unable to parse, assume storec proc or unsupported statement */UNPARSED,
SELECT,
SELECTINTO,
DELETE,
INSERT,
UPSERT,
UPDATE,
CACHE,
CHECKCACHE,
REPLICATE,
KILL,
GETDELETED,
METAQUERY,
MEMORYQUERY,
LOADMEMORYQUERY,
QUERYCACHE,
RESET,
EXEC,
CALL,
CREATE,
DROP,
COMMIT,
XML,
ALTERTABLE,
SP,
MERGE,
UNKNOWN}

}

