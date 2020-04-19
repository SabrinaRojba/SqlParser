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

public class SqlCreateTableStatement extends SqlStatement {
  private SqlCollection<SqlColumnDefinition> columnDefinitions = new SqlCollection<SqlColumnDefinition>();
  private boolean createIfNotExists = false;
  public boolean getCreateIfNotExists() {
    return createIfNotExists;
  }

  public SqlCollection<SqlColumnDefinition> getColumnDefinitions() {
    return columnDefinitions;
  }

  public void setColumnDefinition(SqlCollection<SqlColumnDefinition> columnDefinitions) {
    this.columnDefinitions = columnDefinitions;
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public SqlCreateTableStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlCreateTableStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception{
    super(dialectProcessor);
    tokenizer.NextToken();
    tokenizer.EnsureNextIdentifier("TABLE");
    parseTableName(tokenizer);
    ParserCore.parseColumnDefinitions(tokenizer, this.columnDefinitions, this.dialectProcessor);
  }
  
  private void parseTableName(SqlTokenizer tokenizer) throws Exception {
    SqlToken tk = tokenizer.LookaheadToken2();
    if(tk.equals("IF")){
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("NOT");
      tokenizer.EnsureNextToken("EXISTS");
      this.createIfNotExists = true;
      tk = tokenizer.LookaheadToken2();
    }
    if (tk.IsEmpty()){
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_TABLENAME));
    }
    String[] period = ParserCore.parsePeriodTableName(tokenizer, this.getDialectProcessor());
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  @Override
  public Object clone() {
    SqlCreateTableStatement obj = new SqlCreateTableStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlCreateTableStatement o = (SqlCreateTableStatement)obj;
    this.columnDefinitions = o.columnDefinitions == null ? null : (SqlCollection<SqlColumnDefinition>)o.columnDefinitions.clone();
    this.createIfNotExists = o.createIfNotExists;
  }
}
