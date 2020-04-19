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

/**Syntax:
 *    EXECUTE my_proc @second = 2, @first = 1, @third = 3;
 *    EXEC my_proc @second = 2, @first = 1, @third = 3;
 *    EXECUTE my_proc second = @p1, first = @p2, third = @p3;
 */

public class SqlExecStatement extends SqlStatement {
  private SqlCollection<SqlColumn> Columns;
  public SqlExecStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.NextToken();
    parseStoredProcedureName(tokenizer);
    parseStoredProcedureParameters(tokenizer);
  }

  public SqlExecStatement(SqlTable table, SqlCollection<SqlColumn> columns, Dialect dialectProcessor){
    super(dialectProcessor);
    this.Columns = columns;
    this.table = table;
    this.tableName = table.getName();
  }

  private SqlExecStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public /*#override#*/ void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public /*#override#*/ SqlCollection<SqlColumn> getColumns() {
    if (Columns != null) {
      return Columns;
    } else {
      return super.getColumns();
    }
  }

  @Override
  public Object clone() {
    SqlExecStatement obj = new SqlExecStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlExecStatement o = (SqlExecStatement)obj;
    this.Columns = o.Columns == null ? null : (SqlCollection<SqlColumn>)o.Columns.clone();
  }

  private void parseStoredProcedureName(SqlTokenizer tokenizer) throws Exception {
    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    if (null == period[2] || 0 == period[2].length()) {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_SP_NAME));
    }
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);
  }

  private void parseStoredProcedureParameters(SqlTokenizer tokenizer) throws Exception{
    if(tokenizer.EOF()){
      return;
    } else if (tokenizer.LookaheadToken2().IsEmpty()){
      tokenizer.NextToken();
      return;
    }
    Columns = new SqlCollection<SqlColumn>();
    SqlToken tryNext;
    while (true){
      String name = tokenizer.NextToken().Value;
      tokenizer.EnsureNextToken("=");
      SqlGeneralColumn column = new SqlGeneralColumn(name, ParserCore.readExpression(tokenizer, this));
      this.Columns.add(column);
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals(",")) {
        tokenizer.NextToken();
      } else {
        break;
      }
    }
  }
}

