//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlXmlStatement extends SqlStatement{
  String xml;
  
  public SqlXmlStatement(String xmlString, Dialect dialectProcessor) {
    super(dialectProcessor);
    xml = xmlString;
  }

  private SqlXmlStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }
  
  public String getXmlString(){
    return xml;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {
  }

  @Override
  public Object clone() {
    SqlXmlStatement obj = new SqlXmlStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlXmlStatement o = (SqlXmlStatement)obj;
    this.xml = o.xml;
  }
}
