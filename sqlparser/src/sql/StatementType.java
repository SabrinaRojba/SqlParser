//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public enum StatementType {
  /*UNPARSED means no statement was parsed, XML is for UPDATEGRAM, and
UNKNOWN means unable to parse, assume storec proc or unsupported statement */
  UNPARSED,
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
  UNKNOWN
}
