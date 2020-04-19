//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;
import core.SqlExceptions;

public class SqlDropStatement extends SqlStatement {
  private boolean DropIfExist = false;
  public SqlDropStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public boolean getDropIfExist() {
    return DropIfExist;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public SqlDropStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    super(dialectProcessor);
    tokenizer.NextToken();
    tokenizer.EnsureNextIdentifier("TABLE");
    parseTableName(tokenizer);
  }

  @Override
  public Object clone() {
    SqlDropStatement obj = new SqlDropStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlDropStatement o = (SqlDropStatement)obj;
    this.DropIfExist = o.DropIfExist;
  }

  private void parseTableName(SqlTokenizer tokenizer) throws Exception {
    SqlToken tk = tokenizer.LookaheadToken2();
    if(tk.equals("IF")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("EXISTS");
      this.DropIfExist = true;
      tk = tokenizer.LookaheadToken2();
    }
    if (tk.IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    String[] period = ParserCore.parsePeriodTableName(tokenizer, this.getDialectProcessor());
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }
}

