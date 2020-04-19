//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlUpsertStatement extends SqlXsertStatement {
  public SqlUpsertStatement(Dialect dialectProcessor){
    super(dialectProcessor);
    Verb = "UPSERT";
  }
  public SqlUpsertStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    super(tokenizer, dialectProcessor);
    Verb = "UPSERT";
  }

  @Override
  public Object clone() {
    SqlUpsertStatement obj = new SqlUpsertStatement(null);
    obj.copy(this);
    return obj;
  }
}
