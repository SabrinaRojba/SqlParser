//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

/*#public#*/ /*@*/public/*@*/ interface IFormula {
  SqlFormulaColumn getColumn();
  IFunction getFunction();
  Object evaluate(Object data) throws Exception;
}

