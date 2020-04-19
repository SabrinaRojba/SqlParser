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


public class SqlXmlStatement : SqlStatement{
  string xml;
  
  public SqlXmlStatement(string xmlString, Dialect dialectProcessor) : base(dialectProcessor) {
    xml = xmlString;
  }

  private SqlXmlStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }
  
  public string GetXmlString(){
    return xml;
  }

  public override void Accept(ISqlQueryVisitor visitor) {
  }

  public override Object Clone() {
    SqlXmlStatement obj = new SqlXmlStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlXmlStatement o = (SqlXmlStatement)obj;
    this.xml = o.xml;
  }
}
}

