//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlPassThroughStatement extends SqlStatement{
  private String _sqlText;

  public SqlPassThroughStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlPassThroughStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    this._sqlText = tokenizer.getInputText();
    while(!tokenizer.EOF()) {
      tokenizer.NextToken();
    }
  }

  @Override
  public void accept(ISqlQueryVisitor visitor) throws Exception {
    
  }

  @Override
  public Object clone() {
    SqlPassThroughStatement obj = new SqlPassThroughStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlPassThroughStatement o = (SqlPassThroughStatement) obj;
    this._sqlText = o._sqlText;
  }
}
