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


public class SqlAlterTableStatement : SqlStatement {
  private bool _hasIfExists = false;
  private AlterTableAction _alterTableAction;

  public SqlAlterTableStatement(Dialect dialectProcessor) : base(dialectProcessor) {
  }

  public SqlAlterTableStatement(bool hasIfExists, AlterTableAction alterTableAction) : base(null) {
    this._hasIfExists = hasIfExists;
    this._alterTableAction = alterTableAction;
  }

  public SqlAlterTableStatement(SqlTokenizer tokenizer, Dialect dialectProcessor) : base(dialectProcessor) {
    tokenizer.EnsureNextToken("TABLE");
    this._hasIfExists = ParserCore.ParseIFEXISTS(tokenizer);
    string [] period = ParserCore.ParsePeriodTableName(tokenizer, this.dialectProcessor);
    this.tableName = period[2];
    this.table = new SqlTable(period[0], period[1], tableName, tableName);

    if (dialectProcessor != null) {
      this._alterTableAction = (AlterTableAction) dialectProcessor.ParseAlterAction(tokenizer);
    }

    if (null == this._alterTableAction) {
      this._alterTableAction = this.ParseTableAction(tokenizer);
    }
  }

  public override  void Accept(ISqlQueryVisitor visitor) {

  }

  public bool HasIfExists() {
    return this._hasIfExists;
  }

  public AlterTableAction GetAlterTableAction() {
    return this._alterTableAction;
  }

  public void SetAlterTableAction(AlterTableAction alterTableAction) {
    this._alterTableAction = alterTableAction;
  }

  public override Object Clone() {
    SqlAlterTableStatement obj = new SqlAlterTableStatement(null);
    obj.Copy(this);
    return obj;
  }

  protected override void Copy(SqlStatement obj) {
    base.Copy(obj);
    SqlAlterTableStatement o = (SqlAlterTableStatement)obj;
    this._hasIfExists = o._hasIfExists;
    this._alterTableAction = o._alterTableAction;
  }

  private AlterTableAction ParseTableAction(SqlTokenizer tokenizer) {
    bool hasIfNotExistsForColumn = false;
    bool hasColumnKeyword = false;
    bool hasIfExistsForColumn = false;
    SqlCollection<SqlColumnDefinition> columnDefinitions = new SqlCollection<SqlColumnDefinition>();
    SqlToken tryNext = tokenizer.LookaheadToken2();
    if (tryNext.Equals("ADD")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("COLUMN")) {
        tokenizer.NextToken();
        tryNext = tokenizer.LookaheadToken2();
        hasColumnKeyword = true;
      }

      if (tryNext.Equals(SqlToken.Open)) {
        ParserCore.ParseColumnDefinitions(tokenizer, columnDefinitions, this.dialectProcessor);
      } else {
        if (tryNext.Equals("IF")) {
          tokenizer.NextToken();
          tokenizer.EnsureNextToken("NOT");
          tokenizer.EnsureNextToken("EXISTS");
          hasIfNotExistsForColumn = true;
        }

        SqlColumnDefinition definition = this.dialectProcessor != null ? this.dialectProcessor.ParseColumnDefinition(tokenizer) : null;
        if (null == definition) {
          definition = ParserCore.ParseOneColumnDefinition(tokenizer, this.dialectProcessor);
        }
        columnDefinitions.Add(definition);
      }
      return new AddColumnAction(columnDefinitions,
              hasColumnKeyword,
              hasIfNotExistsForColumn);
    } else if (tryNext.Equals("DROP")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("COLUMN");
      hasIfExistsForColumn = ParserCore.ParseIFEXISTS(tokenizer);
      tryNext = tokenizer.LookaheadToken2();
      SqlCollection<SqlColumn> columns;
      if (tryNext.Equals(SqlToken.Open)) {
        tokenizer.NextToken();
        columns = ParserCore.ParseColumns(tokenizer, this);
        tokenizer.EnsureNextToken(SqlToken.Close.Value);
      } else {
        columns = ParserCore.ParseColumns(tokenizer, this);
      }
      foreach(SqlColumn column in columns) {
        SqlColumnDefinition definition = new SqlColumnDefinition();
        definition.ColumnName = column.GetColumnName();
        columnDefinitions.Add(definition);
      }
      return new DropColumnAction(columnDefinitions, hasIfExistsForColumn);
    } else if (tryNext.Equals("ALTER")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("COLUMN");
      SqlColumnDefinition definition = ParserCore.ParseOneColumnDefinition(tokenizer, this.dialectProcessor);
      columnDefinitions.Add(definition);
      return new AlterColumnAction(columnDefinitions);
    } else if (tryNext.Equals("RENAME")) {
      tokenizer.NextToken();
      tryNext = tokenizer.LookaheadToken2();
      if (tryNext.Equals("COLUMN")) {
        tokenizer.EnsureNextToken("COLUMN");
        string c = tokenizer.NextToken().Value;
        tokenizer.EnsureNextToken("TO");
        string cc = tokenizer.NextToken().Value;
        return new ReNameColumnAction(new SqlGeneralColumn(c), new SqlGeneralColumn(cc));
      } else {
        tokenizer.EnsureNextToken("TO");
        string n = tokenizer.NextToken().Value;
        return new ReNameTableAction(new SqlTable(n));
      }
    } else {
      throw tokenizer.MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNRECOGNIZED_KEYWORD, tryNext.Value));
    }
  }
}
}

