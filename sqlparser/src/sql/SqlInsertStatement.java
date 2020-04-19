//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlInsertStatement extends SqlXsertStatement {
  public SqlInsertStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
    Verb = "INSERT";
  }

  public SqlInsertStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(tokenizer, dialectProcessor);
    Verb = "INSERT";
  }

  @Override
  public Object clone() {
    SqlInsertStatement obj = new SqlInsertStatement(null);
    obj.copy(this);
    return obj;
  }
}


