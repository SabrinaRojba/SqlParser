//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

/*#public#*/ /*@*/public/*@*/
interface ITableMatch {
  SqlTable create(SqlTable t);
  boolean accept(SqlTable t, TablePartType type);
  boolean unwind(SqlTable t, TablePartType type);
}

