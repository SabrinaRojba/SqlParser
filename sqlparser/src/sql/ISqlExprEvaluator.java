//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
import rssbus.RSBException;
/*#public#*/ /*@*/public/*@*/ interface ISqlExprEvaluator {
  SqlValue evaluate(SqlExpression expr) throws RSBException;
}