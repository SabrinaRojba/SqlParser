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


public class MySQLDialect : Dialect {
  private static RebuildOptions REBUILD_OPTIONS = RebuildOptions.MySQL;
  //https://dev.mysql.com/doc/refman/5.7/en/create-table.html
  //data_type
  //TODO Douglas the "UNSIGNED", "ZEROFILL", [CHARACTER SET charset_name] [COLLATE collation_name] flag and spatial_type will be supported it later.
  private static string[] SUPPORTED_TYPES = {"BIT", "TINYINT", "SMALLINT", "MEDIUMINT",
      "INT", "INTEGER", "BIGINT", "REAL",
      "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC",
      "DATE", "TIME", "TIMESTAMP", "DATETIME",
      "YEAR", "CHAR", "VARCHAR", "BINARY",
      "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB",
      "LONGBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT",
      "LONGTEXT", "ENUM", "SET", "JSON"
  };

  public override string BuildAlterAction(AlterAction tableAction) {
    if (tableAction is AlterTableAction) {
      AlterTableAction alterTableAction = (AlterTableAction) tableAction;
      SqlCollection<SqlColumnDefinition> columnDefinitions = alterTableAction.GetColumnDefinitions();
      SqlAlterOptions options = alterTableAction.GetAlterOption();
      if (options == SqlAlterOptions.ADD_COLUMN) {
        if (columnDefinitions.Size() != 1) {
          throw new RSBException("MySQLDialect", "Add column action must have one column definition.");
        }
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" ADD COLUMN");
        builder.Append(" ").Append(BuildColumnDefinition(columnDefinitions.Get(0)));
        return builder.ToString();
      } else if (alterTableAction.GetAlterOption() == SqlAlterOptions.ALTER_COLUMN) {
        if (columnDefinitions.Size() != 1) {
          throw new RSBException("MySQLDialect", "Alter column action must have one column definition.");
        }
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" CHANGE COLUMN");
        SqlColumnDefinition columnDefinition = columnDefinitions.Get(0);
        builder.Append(" ").Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(columnDefinition.ColumnName, REBUILD_OPTIONS)));
        //Use the same name here, because our does not support change name.
        builder.Append(" ").Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(columnDefinition.ColumnName, REBUILD_OPTIONS)));
        builder.Append(" ").Append(DialectUtilities.AppendDataType(columnDefinition.DataType, columnDefinition.ColumnSize, columnDefinition.Scale));
        return builder.ToString();
      } else if (options == SqlAlterOptions.DROP_COLUMN) {
        if (columnDefinitions.Size() != 1) {
          throw new RSBException("MySQLDialect", "Drop column action must have one column definition.");
        }
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" DROP COLUMN");
        SqlColumnDefinition columnDefinition = columnDefinitions.Get(0);
        builder.Append(" ").Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(columnDefinition.ColumnName, REBUILD_OPTIONS)));
        return builder.ToString();
      } else if (options == SqlAlterOptions.RENAME_TABLE) {
        ReNameTableAction renameTableAction = (ReNameTableAction)alterTableAction;
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" RENAME TO");
        builder.Append(" ");
        builder.Append(BuildTableName(renameTableAction.GetNameToTable()));
        return builder.ToString();
      } else if (options == SqlAlterOptions.RENAME_COLUMN) {
        ReNameColumnAction renameColumnAction = (ReNameColumnAction)alterTableAction;
        ByteBuffer builder = new ByteBuffer();
        builder.Append(" RENAME COLUMN");
        builder.Append(" ").Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(renameColumnAction.GetSrcColumn().GetColumnName(), REBUILD_OPTIONS)));
        builder.Append(" TO ").Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(renameColumnAction.GetNameToColumn().GetColumnName(), REBUILD_OPTIONS)));
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
      next = tokenizer.LookaheadToken2();
      if (next.Equals("COLUMN")) {
        tokenizer.NextToken();
      }
      definitions.Add(ParseColumnDefinition(tokenizer));
      return new AddColumnAction(definitions, false, false);
    } if (next.Equals("CHANGE")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.Equals("COLUMN")) {
        tokenizer.NextToken();
        next = tokenizer.LookaheadToken2();
      }
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.ParseIdentifierName(next, this);
      //Not support change column name now.
      tokenizer.NextToken();
      DataTypeDefinition dataTypeDefinition = DialectUtilities.ParseDataType(tokenizer, SUPPORTED_TYPES);
      sqlColumnDefinition.DataType = dataTypeDefinition.GetDataType();
      if (dataTypeDefinition.GetFactors().Length >= 1) {
        sqlColumnDefinition.ColumnSize = dataTypeDefinition.GetFactors()[0];
      }
      definitions.Add(sqlColumnDefinition);
      return new AlterColumnAction(definitions);
    } else if (next.Equals("DROP")) {
      tokenizer.NextToken();
      next = tokenizer.NextToken();
      if (next.Equals("COLUMN")) {
        next = tokenizer.NextToken();
      }
      bool hasIfExistsForColumn = false;
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.ParseIdentifierName(next, this);;
      definitions.Add(sqlColumnDefinition);
      return new DropColumnAction(definitions, hasIfExistsForColumn);
    } else if (next.Equals("RENAME")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.Equals("TO")) {
        //Rename table action
        tokenizer.NextToken();
        string[] period = ParserCore.ParsePeriodTableName(tokenizer, this);
        return new ReNameTableAction(new SqlTable(period[0], period[1], period[2], period[2]));
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
          throw new RSBException("MySQLDialect", "Invalid column name : " + Utilities.Join(",", identities));
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
    ByteBuffer builder = new ByteBuffer();
    builder.Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(definition.ColumnName, REBUILD_OPTIONS)));
    builder.Append(" ").Append(DialectUtilities.AppendDataType(definition.DataType, definition.ColumnSize, definition.Scale));
    if (!Utilities.IsNullOrEmpty(definition.AutoIncrement)) {
      builder.Append(" ").Append("AUTO_INCREMENT");
    }
    return builder.ToString();
  }

  public override string BuildTableConstraint(SqlCollection<SqlColumnDefinition> definitions) {
    ByteBuffer keys = new ByteBuffer();
    foreach(SqlColumnDefinition col in definitions) {
      if (col.IsKey) {
        if (keys.Length > 0) {
          keys.Append(", ");
        }
        keys.Append(REBUILD_OPTIONS.QuoteIdentifierWithDot(BuilderCore.EncodeIdentifier(col.ColumnName, REBUILD_OPTIONS)));
      }
    }
    if (keys.Length > 0) {
      keys.Insert(0, "PRIMARY KEY(");
      keys.Append(")");
      return keys.ToString();
    }
    return base.BuildTableConstraint(definitions);
  }

  public override SqlColumnDefinition ParseColumnDefinition(SqlTokenizer tokenizer) {
    SqlToken next = tokenizer.NextToken();
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
    next = tokenizer.LookaheadToken2();
    if (next.Equals("AUTO_INCREMENT")) {
      tokenizer.NextToken();
      sqlColumnDefinition.AutoIncrement = "AUTO_INCREMENT";
    }
    if (next.Equals("PRIMARY")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("KEY");
      sqlColumnDefinition.IsKey = true;
    }
    return sqlColumnDefinition;
  }

  public override string WriteTerm(SqlExpression column) {
    if (column is SqlFormulaColumn) {
      SqlFormulaColumn formulaCol = (SqlFormulaColumn)column;
      if (Utilities.EqualIgnoreCase("MATCH", formulaCol.GetColumnName())) {
        SqlCollection<SqlExpression> parameters = formulaCol.GetParameters();
        if (parameters != null && formulaCol.GetParameters().Size() >= 3) {
          SqlTable table = ((SqlGeneralColumn)parameters.Get(parameters.Size() - 3)).GetTable();
          string matchTable = table != null ? BuildTableName(table) : null;
          string matchText = ((SqlValueExpression)parameters.Get(parameters.Size() - 2)).GetParameterValueAsString();
          string matchMode = ((SqlValueExpression)parameters.Get(parameters.Size() - 1)).GetParameterValueAsString();
          ByteBuffer matchColumns = new ByteBuffer();
          for (int i = 0, columnCount = parameters.Size() - 2; i < columnCount; ++i) {
            if (i > 0) {
              matchColumns.Append(",");
            }
            if (matchTable != null) {
              matchColumns.Append(matchTable).Append(".");
            }
            string columnName = ((SqlGeneralColumn)parameters.Get(i)).GetColumnName();
            matchColumns.Append("`").Append(columnName).Append("`");
          }

          ByteBuffer matchExpre = new ByteBuffer();
          matchExpre.Append("MATCH(").Append(matchColumns.ToString()).Append(") ");
          matchExpre.Append("AGAINST('").Append(Encode(matchText)).Append("' ").Append(matchMode).Append(")");
          return matchExpre.ToString();
        }
      }
    }

    return null;
  }

  public static string Encode(string value) {
    if (!Utilities.IsNullOrEmpty(value)) {
      ByteBuffer encoded = new ByteBuffer();
      for (int i=0; i<value.Length; i++) {
        char c = value[i];
        if (c == '\'' || c == '\\') {
          encoded.Append("\\");
        }
        encoded.Append(c);
      }

      return encoded.ToString();
    }

    return value;
  }

  private string BuildTableName(SqlTable table) {
    SqlBuilder builder = SqlBuilder.CreateBuilder();
    builder.SetBuildOptions(REBUILD_OPTIONS);
    return builder.BuildTable(table);
  }
}
}

