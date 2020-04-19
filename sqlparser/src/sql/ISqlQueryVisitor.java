//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import rssbus.RSBException;

/*#public#*/ /*@*/public/*@*/interface ISqlQueryVisitor {
  void visit(SqlTable table) throws RSBException;
  void visit(SqlStatement stmt) throws RSBException;
  void visit(SqlColumn column) throws RSBException;
  void visit(SqlConditionNode criteria) throws RSBException;
}
