using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;



using RSSBus;
using RSSBus.core;
using CData.Sql;
using System.Data;
namespace CData.Sql {


public class OracleDialect : Dialect {
  private RebuildOptions _rebuildOptions = RebuildOptions.Oracle;
  //https://docs.oracle.com/cd/B28359_01/server.111/b28286/sql_elements001.htm#SQLRF50950
  //Oracle Built-in Datatypes
  //TODO Douglas the "TIMESTAMP (precision) WITH LOCAL TIME ZONE", "TIMESTAMP (precision) WITH TIME ZONE" flag will be supported later.
  private static string[] SUPPORTED_TYPES = {"VARCHAR2", "NVARCHAR2", "NUMBER", "FLOAT",
      "LONG", "DATE", "BINARY_FLOAT", "BINARY_DOUBLE",
      "TIMESTAMP", "INTERVAL YEAR", "INTERVAL DAY", "RAW",
      "LONG RAW", "ROWID", "UROWID", "CHAR",
      "NCHAR", "CLOB", "NCLOB", "BLOB",
      "BFILE"
  };

  public void SetRebuildOptions(RebuildOptions rebuildOptions) {
    this._rebuildOptions = rebuildOptions;
  }

  public override RebuildOptions GetRebuildOptions() {
    return this._rebuildOptions;
  }

  public override string BuildTableAlias(SqlTable table) {
    ByteBuffer clause = new ByteBuffer();
    if (table.HasAlias()) {
      clause.Append(" ");
      clause.Append(this._rebuildOptions.QuoteIdentifierWithDot(table.GetAlias()));
    }
    return clause.ToString();
  }

  public override string ParseIdentifierName(SqlToken token) {
    if (token.WasQuoted) {
      return token.Value;
    } else {
      return Utilities.ToUpper(token.Value);
    }
  }

  public override string BuildAlterAction(AlterAction tableAction) {
    if (tableAction is AlterTableAction) {
      AlterTableAction alterTableAction = (AlterTableAction) tableAction;
      SqlAlterOptions options = alterTableAction.GetAlterOption();
      SqlCollection<SqlColumnDefinition> columnDefinitions = alterTableAction.GetColumnDefinitions();
      if (options == SqlAlterOptions.ADD_COLUMN) {
        if (columnDefinitions.Size() != 1) {
          throw new RSBException("OracleDialect", "Add column action must have one column definition.");
        }
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" ADD (");
        builder.Append(BuildColumnDefinition(columnDefinitions.Get(0)));
        builder.Append(")");
        return builder.ToString();
      } else if (options == SqlAlterOptions.ALTER_COLUMN) {
        if (columnDefinitions.Size() != 1) {
          throw new RSBException("OracleDialect", "Alter column action must have one column definition.");
        }
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" MODIFY (");
        SqlColumnDefinition columnDefinition = columnDefinitions.Get(0);
        builder.Append(this._rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(columnDefinition.ColumnName, this._rebuildOptions)));
        builder.Append(" ").Append(DialectUtilities.AppendDataType(columnDefinition.DataType, columnDefinition.ColumnSize, columnDefinition.Scale));
        builder.Append(")");
        return builder.ToString();
      } else if (options == SqlAlterOptions.DROP_COLUMN) {
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" DROP COLUMN");
        if (columnDefinitions.Size() != 1) {
          throw new RSBException("OracleDialect", "Drop column action must have one column definition.");
        }
        SqlColumnDefinition columnDefinition = columnDefinitions.Get(0);
        builder.Append(" ").Append(this._rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(columnDefinition.ColumnName, this._rebuildOptions)));
        return builder.ToString();
      } else if (options == SqlAlterOptions.RENAME_TABLE) {
        ReNameTableAction renameTableAction = (ReNameTableAction)alterTableAction;
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" RENAME TO");
        builder.Append(" ").Append(this._rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(renameTableAction.GetNameToTable().GetName(), this._rebuildOptions)));
        return builder.ToString();
      } else if (options == SqlAlterOptions.RENAME_COLUMN) {
        ReNameColumnAction renameColumnAction = (ReNameColumnAction)alterTableAction;
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" RENAME COLUMN");
        builder.Append(" ").Append(this._rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(renameColumnAction.GetSrcColumn().GetColumnName(), this._rebuildOptions)));
        builder.Append(" TO ").Append(this._rebuildOptions.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(renameColumnAction.GetNameToColumn().GetColumnName(), this._rebuildOptions)));
        return builder.ToString();
      }
    }
    return base.BuildAlterAction(tableAction);
  }

  public override AlterAction ParseAlterAction(SqlTokenizer tokenizer) {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.Equals("ADD")) {
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("(");
      definitions.Add(ParseColumnDefinition(tokenizer));
      tokenizer.EnsureNextToken(")");
      return new AddColumnAction(definitions, false, false);
    } if (next.Equals("MODIFY")) {
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("(");
      next = tokenizer.NextToken();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.ParseIdentifierName(next, this);
      DataTypeDefinition dataTypeDefinition = DialectUtilities.ParseDataType(tokenizer, SUPPORTED_TYPES);
      sqlColumnDefinition.DataType = dataTypeDefinition.GetDataType();
      if (dataTypeDefinition.GetFactors().Length >= 1) {
        sqlColumnDefinition.ColumnSize = dataTypeDefinition.GetFactors()[0];
      }
      if (dataTypeDefinition.GetFactors().Length >= 2) {
        sqlColumnDefinition.Scale = dataTypeDefinition.GetFactors()[1];
      }
      definitions.Add(sqlColumnDefinition);
      tokenizer.EnsureNextToken(")");
      return new AlterColumnAction(definitions);
    } else if (next.Equals("DROP")) {
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("COLUMN");
      next = tokenizer.NextToken();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.ParseIdentifierName(next, this);;
      definitions.Add(sqlColumnDefinition);
      return new DropColumnAction(definitions, false);
    } else if (next.Equals("RENAME")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.Equals("TO")) {
        //Rename table action
        tokenizer.NextToken();
        next = tokenizer.NextToken();
        string renameToTableName = DialectUtilities.ParseIdentifierName(next, this);
        return new ReNameTableAction(new SqlTable(renameToTableName));
      } else if (next.Equals("COLUMN")) {
        //Rename column action
        string catalog = null;
        string schema = null;
        string tableName = null;
        string oldColumnName = null;
        string newColumnName = null;
        tokenizer.NextToken();
        string[] identities = ParserCore.ParsePeriodTableName(tokenizer, this);
        if (identities.Length == 3) {
          schema = identities[0];
          tableName = identities[1];
          oldColumnName = identities[2];
        } else if (identities.Length == 2) {
          tableName = identities[0];
          oldColumnName = identities[1];
        } else if (identities.Length == 1) {
          oldColumnName = identities[0];
        } else {
          throw new RSBException("OracleDialect", "Invalid column name : " + Utilities.Join(",", identities));
        }

        tokenizer.EnsureNextToken("TO");
        next = tokenizer.NextToken();
        newColumnName = DialectUtilities.ParseIdentifierName(next, this);
        return new ReNameColumnAction(new SqlGeneralColumn(new SqlTable(catalog, schema, tableName), oldColumnName), new SqlGeneralColumn(newColumnName));
      }
    }
    return base.ParseAlterAction(tokenizer);
  }

  public override string BuildColumnDefinition(SqlColumnDefinition definition) {
    return DialectUtilities.BuildColumnDefinition(definition, this._rebuildOptions);
  }

  public override SqlColumnDefinition ParseColumnDefinition(SqlTokenizer tokenizer) {
    return DialectUtilities.ParseColumnDefinition(tokenizer, SUPPORTED_TYPES, this);
  }
}
}

