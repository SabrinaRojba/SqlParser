//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

/*#public#*/ /*@*/public/*@*/
interface IFunction {
  Object evaluate(Object[] parameters) throws Exception;
  String getName();
  int getReturnDataType();
  int getMaxArgs();
  int getMinArgs();
  boolean isServerSideFunction();
  boolean isAggregateFunction();
}

