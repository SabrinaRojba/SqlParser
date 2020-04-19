//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.SqlExceptions;

public class SqlCommitStatement extends SqlStatement {
  public SqlCommitStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public SqlCommitStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    super(dialectProcessor);
    tokenizer.EnsureNextIdentifier("COMMIT");
    SqlToken next = tokenizer.NextToken();
    if(!next.IsEmpty()){
      throw SqlExceptions.Exception("QueryException", SqlExceptions.UNEXPECTED_TOKEN, next.Value);
    }
  }

  @Override
  public Object clone() {
    SqlCommitStatement obj = new SqlCommitStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
  }
}
