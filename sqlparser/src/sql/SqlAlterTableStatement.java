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

public class SqlAlterTableStatement extends SqlStatement {
  private boolean _hasIfExists = false;
  private AlterTableAction _alterTableAction;

  public SqlAlterTableStatement(Dialect dialectProcessor) {
    super(dialectProcessor);
  }

  public SqlAlterTableStatement(boolean hasIfExists, AlterTableAction alterTableAction) {
    super(null);
    this._hasIfExists = hasIfExists;
    this._alterTableAction = alterTableAction;
  }

  public SqlAlterTableStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) throws Exception {
    super(dialectProcessor);
    tokenizer.EnsureNextToken("TABLE");
    this._hasIfExists = ParserCore.parseIFEXISTS(tokenizer);
    String [] period = ParserCore.parsePeriodTableName(tokenizer, this.dialectProcessor);
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);

    if (dialectProcessor != null) {
      this._alterTableAction = (AlterTableAction) dialectProcessor.parseAlterAction(tokenizer);
    }

    if (null == this._alterTableAction) {
      this._alterTableAction = this.parseTableAction(tokenizer);
    }
  }

  public /*#override#*/  void accept(ISqlQueryVisitor visitor) throws Exception {

  }

  public boolean hasIfExists() {
    return this._hasIfExists;
  }

  public AlterTableAction getAlterTableAction() {
    return this._alterTableAction;
  }

  public void setAlterTableAction(AlterTableAction alterTableAction) {
    this._alterTableAction = alterTableAction;
  }

  @Override
  public Object clone() {
    SqlAlterTableStatement obj = new SqlAlterTableStatement(null);
    obj.copy(this);
    return obj;
  }

  @Override
  protected void copy(SqlStatement obj) {
    super.copy(obj);
    SqlAlterTableStatement o = (SqlAlterTableStatement)obj;
    this._hasIfExists = o._hasIfExists;
    this._alterTableAction = o._alterTableAction;
  }

  private AlterTableAction parseTableAction(SqlTokenizer tokenizer) throws Exception {
    boolean hasIfNotExistsForColumn = false;
    boolean hasColumnKeyword = false;
    boolean hasIfExistsForColumn = false;
    SqlCollection<SqlColumnDefinition> columnDefinitions = new SqlCollection<SqlColumnDefinition>();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.equals("ADD")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("COLUMN")) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        hasColumnKeyword = true;
      }

      if (tryNext.equals(SqlToken.Open)) {
        ParserCore.parseColumnDefinitions(tokenizer, columnDefinitions, this.dialectProcessor);
      } else {
        if (tryNext.equals("IF")) {
          tokenizer.NextToken();
          tokenizer.EnsureNextToken("NOT");
          tokenizer.EnsureNextToken("EXISTS");
          hasIfNotExistsForColumn = true;
        }

        SqlColumnDefinition definition = this.dialectProcessor != null ? this.dialectProcessor.parseColumnDefinition(tokenizer) : null;
        if (null == definition) {
          definition = ParserCore.parseOneColumnDefinition(tokenizer, this.dialectProcessor);
        }
        columnDefinitions.add(definition);
      }
      return new AddColumnAction(columnDefinitions,
              hasColumnKeyword,
              hasIfNotExistsForColumn);
    } else if (tryNext.equals("DROP")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("COLUMN");
      hasIfExistsForColumn = ParserCore.parseIFEXISTS(tokenizer);
      tryNext = tokenizer.LookaheadToken2();
      SqlCollection<SqlColumn> columns;
      if (tryNext.equals(SqlToken.Open)) {
        tokenizer.NextToken();
        columns = ParserCore.parseColumns(tokenizer, this);
        tokenizer.EnsureNextToken(SqlToken.Close.Value);
      } else {
        columns = ParserCore.parseColumns(tokenizer, this);
      }
      for (SqlColumn column : columns) {
        SqlColumnDefinition definition = new SqlColumnDefinition();
        definition.ColumnName = column.getColumnName();
        columnDefinitions.add(definition);
      }
      return new DropColumnAction(columnDefinitions, hasIfExistsForColumn);
    } else if (tryNext.equals("ALTER")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("COLUMN");
      SqlColumnDefinition definition = ParserCore.parseOneColumnDefinition(tokenizer, this.dialectProcessor);
      columnDefinitions.add(definition);
      return new AlterColumnAction(columnDefinitions);
    } else if (tryNext.equals("RENAME")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.equals("COLUMN")) {
        tokenizer.EnsureNextToken("COLUMN");
        String c = tokenizer.NextToken().Value;
        tokenizer.EnsureNextToken("TO");
        String cc = tokenizer.NextToken().Value;
        return new ReNameColumnAction(new SqlGeneralColumn(c), new SqlGeneralColumn(cc));
      } else {
        tokenizer.EnsureNextToken("TO");
        String n = tokenizer.NextToken().Value;
        return new ReNameTableAction(new SqlTable(n));
      }
    } else {
      throw tokenizer.MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNRECOGNIZED_KEYWORD, tryNext.Value));
    }
  }
}

