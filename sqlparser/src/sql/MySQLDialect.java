//@
package cdata.sql;
//@
/*#
using RSSBus;
using RSSBus.core;
using CData.Sql;
using System.Data;
namespace CData.Sql {
#*/

import core.BuilderCore;
import core.ParserCore;
import rssbus.RSBException;
import rssbus.oputils.common.Utilities;

public class MySQLDialect extends Dialect {
  private static final RebuildOptions REBUILD_OPTIONS = RebuildOptions.MySQL;
  //https://dev.mysql.com/doc/refman/5.7/en/create-table.html
  //data_type
  //TODO Douglas the "UNSIGNED", "ZEROFILL", [CHARACTER SET charset_name] [COLLATE collation_name] flag and spatial_type will be supported it later.
  private static final String[] SUPPORTED_TYPES = {"BIT", "TINYINT", "SMALLINT", "MEDIUMINT",
      "INT", "INTEGER", "BIGINT", "REAL",
      "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC",
      "DATE", "TIME", "TIMESTAMP", "DATETIME",
      "YEAR", "CHAR", "VARCHAR", "BINARY",
      "VARBINARY", "TINYBLOB", "BLOB", "MEDIUMBLOB",
      "LONGBLOB", "TINYTEXT", "TEXT", "MEDIUMTEXT",
      "LONGTEXT", "ENUM", "SET", "JSON"
  };

  @Override
  public String buildAlterAction(AlterAction tableAction) throws Exception {
    if (tableAction instanceof AlterTableAction) {
      AlterTableAction alterTableAction = (AlterTableAction) tableAction;
      SqlCollection<SqlColumnDefinition> columnDefinitions = alterTableAction.getColumnDefinitions();
      SqlAlterOptions options = alterTableAction.getAlterOption();
      if (options == SqlAlterOptions.ADD_COLUMN) {
        if (columnDefinitions.size() != 1) {
          throw new RSBException("MySQLDialect", "Add column action must have one column definition.");
        }
        StringBuffer builder = new StringBuffer();
        builder.append(" ADD COLUMN");
        builder.append(" ").append(buildColumnDefinition(columnDefinitions.get(0)));
        return builder.toString();
      } else if (alterTableAction.getAlterOption() == SqlAlterOptions.ALTER_COLUMN) {
        if (columnDefinitions.size() != 1) {
          throw new RSBException("MySQLDialect", "Alter column action must have one column definition.");
        }
        StringBuffer builder = new StringBuffer();
        builder.append(" CHANGE COLUMN");
        SqlColumnDefinition columnDefinition = columnDefinitions.get(0);
        builder.append(" ").append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(columnDefinition.ColumnName, REBUILD_OPTIONS)));
        //Use the same name here, because our does not support change name.
        builder.append(" ").append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(columnDefinition.ColumnName, REBUILD_OPTIONS)));
        builder.append(" ").append(DialectUtilities.appendDataType(columnDefinition.DataType, columnDefinition.ColumnSize, columnDefinition.Scale));
        return builder.toString();
      } else if (options == SqlAlterOptions.DROP_COLUMN) {
        if (columnDefinitions.size() != 1) {
          throw new RSBException("MySQLDialect", "Drop column action must have one column definition.");
        }
        StringBuffer builder = new StringBuffer();
        builder.append(" DROP COLUMN");
        SqlColumnDefinition columnDefinition = columnDefinitions.get(0);
        builder.append(" ").append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(columnDefinition.ColumnName, REBUILD_OPTIONS)));
        return builder.toString();
      } else if (options == SqlAlterOptions.RENAME_TABLE) {
        ReNameTableAction renameTableAction = (ReNameTableAction)alterTableAction;
        StringBuffer builder = new StringBuffer();
        builder.append(" RENAME TO");
        builder.append(" ");
        builder.append(buildTableName(renameTableAction.getNameToTable()));
        return builder.toString();
      } else if (options == SqlAlterOptions.RENAME_COLUMN) {
        ReNameColumnAction renameColumnAction = (ReNameColumnAction)alterTableAction;
        StringBuffer builder = new StringBuffer();
        builder.append(" RENAME COLUMN");
        builder.append(" ").append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(renameColumnAction.getSrcColumn().getColumnName(), REBUILD_OPTIONS)));
        builder.append(" TO ").append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(renameColumnAction.getNameToColumn().getColumnName(), REBUILD_OPTIONS)));
        return builder.toString();
      }
    }
    return super.buildAlterAction(tableAction);
  }

  @Override
  public AlterAction parseAlterAction(SqlTokenizer tokenizer) throws Exception {
    SqlToken next = tokenizer.LookaheadToken2();
    if (next.equals("ADD")) {
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.equals("COLUMN")) {
        tokenizer.NextToken();
      }
      definitions.add(parseColumnDefinition(tokenizer));
      return new AddColumnAction(definitions, false, false);
    } if (next.equals("CHANGE")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.equals("COLUMN")) {
        tokenizer.NextToken();
        next = tokenizer.LookaheadToken2();
      }
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.parseIdentifierName(next, this);
      //Not support change column name now.
      tokenizer.NextToken();
      DataTypeDefinition dataTypeDefinition = DialectUtilities.parseDataType(tokenizer, SUPPORTED_TYPES);
      sqlColumnDefinition.DataType = dataTypeDefinition.getDataType();
      if (dataTypeDefinition.getFactors().length >= 1) {
        sqlColumnDefinition.ColumnSize = dataTypeDefinition.getFactors()[0];
      }
      definitions.add(sqlColumnDefinition);
      return new AlterColumnAction(definitions);
    } else if (next.equals("DROP")) {
      tokenizer.NextToken();
      next = tokenizer.NextToken();
      if (next.equals("COLUMN")) {
        next = tokenizer.NextToken();
      }
      boolean hasIfExistsForColumn = false;
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.parseIdentifierName(next, this);;
      definitions.add(sqlColumnDefinition);
      return new DropColumnAction(definitions, hasIfExistsForColumn);
    } else if (next.equals("RENAME")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.equals("TO")) {
        //Rename table action
        tokenizer.NextToken();
        String[] period = ParserCore.parsePeriodTableName(tokenizer, this);
        return new ReNameTableAction(new SqlTable(period[0], period[1], period[2], period[2]));
      } else if (next.equals("COLUMN")) {
        //Rename column action
        String catalog = null;
        String schema = null;
        String tableName = null;
        String oldColumnName = null;
        String newColumnName = null;
        tokenizer.NextToken();
        String[] identities = ParserCore.parsePeriodTableName(tokenizer, this);
        if (identities.length == 3) {
          schema = identities[0];
          tableName = identities[1];
          oldColumnName = identities[2];
        } else if (identities.length == 2) {
          tableName = identities[0];
          oldColumnName = identities[1];
        } else if (identities.length == 1) {
          oldColumnName = identities[0];
        } else {
          throw new RSBException("MySQLDialect", "Invalid column name : " + Utilities.join(",", identities));
        }

        tokenizer.EnsureNextToken("TO");
        next = tokenizer.NextToken();
        newColumnName = DialectUtilities.parseIdentifierName(next, this);
        return new ReNameColumnAction(new SqlGeneralColumn(new SqlTable(catalog, schema, tableName), oldColumnName), new SqlGeneralColumn(newColumnName));
      }
    }
    return super.parseAlterAction(tokenizer);
  }


  @Override
  public String buildColumnDefinition(SqlColumnDefinition definition) throws Exception {
    StringBuilder builder = new StringBuilder();
    builder.append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(definition.ColumnName, REBUILD_OPTIONS)));
    builder.append(" ").append(DialectUtilities.appendDataType(definition.DataType, definition.ColumnSize, definition.Scale));
    if (!Utilities.isNullOrEmpty(definition.AutoIncrement)) {
      builder.append(" ").append("AUTO_INCREMENT");
    }
    return builder.toString();
  }

  @Override
  public String buildTableConstraint(SqlCollection<SqlColumnDefinition> definitions) throws Exception {
    StringBuilder keys = new StringBuilder();
    for (SqlColumnDefinition col : definitions) {
      if (col.IsKey) {
        if (keys.length() > 0) {
          keys.append(", ");
        }
        keys.append(REBUILD_OPTIONS.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(col.ColumnName, REBUILD_OPTIONS)));
      }
    }
    if (keys.length() > 0) {
      keys.insert(0, "PRIMARY KEY(");
      keys.append(")");
      return keys.toString();
    }
    return super.buildTableConstraint(definitions);
  }

  @Override
  public SqlColumnDefinition parseColumnDefinition(SqlTokenizer tokenizer) throws Exception {
    SqlToken next = tokenizer.NextToken();
    SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
    sqlColumnDefinition.ColumnName = DialectUtilities.parseIdentifierName(next, this);
    DataTypeDefinition dataTypeDefinition = DialectUtilities.parseDataType(tokenizer, SUPPORTED_TYPES);
    sqlColumnDefinition.DataType = dataTypeDefinition.getDataType();
    if (dataTypeDefinition.getFactors().length >= 1) {
      sqlColumnDefinition.ColumnSize = dataTypeDefinition.getFactors()[0];
    }
    if (dataTypeDefinition.getFactors().length >= 2) {
      sqlColumnDefinition.Scale = dataTypeDefinition.getFactors()[1];
    }
    next = tokenizer.LookaheadToken2();
    if (next.equals("AUTO_INCREMENT")) {
      tokenizer.NextToken();
      sqlColumnDefinition.AutoIncrement = "AUTO_INCREMENT";
    }
    if (next.equals("PRIMARY")) {
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("KEY");
      sqlColumnDefinition.IsKey = true;
    }
    return sqlColumnDefinition;
  }

  @Override
  public String writeTerm(SqlExpression column) throws Exception {
    if (column instanceof SqlFormulaColumn) {
      SqlFormulaColumn formulaCol = (SqlFormulaColumn)column;
      if (Utilities.equalIgnoreCase("MATCH", formulaCol.getColumnName())) {
        SqlCollection<SqlExpression> parameters = formulaCol.getParameters();
        if (parameters != null && formulaCol.getParameters().size() >= 3) {
          SqlTable table = ((SqlGeneralColumn)parameters.get(parameters.size() - 3)).getTable();
          String matchTable = table != null ? buildTableName(table) : null;
          String matchText = ((SqlValueExpression)parameters.get(parameters.size() - 2)).getParameterValueAsString();
          String matchMode = ((SqlValueExpression)parameters.get(parameters.size() - 1)).getParameterValueAsString();
          StringBuilder matchColumns = new StringBuilder();
          for (int i = 0, columnCount = parameters.size() - 2; i < columnCount; ++i) {
            if (i > 0) {
              matchColumns.append(",");
            }
            if (matchTable != null) {
              matchColumns.append(matchTable).append(".");
            }
            String columnName = ((SqlGeneralColumn)parameters.get(i)).getColumnName();
            matchColumns.append("`").append(columnName).append("`");
          }

          StringBuilder matchExpre = new StringBuilder();
          matchExpre.append("MATCH(").append(matchColumns.toString()).append(") ");
          matchExpre.append("AGAINST('").append(encode(matchText)).append("' ").append(matchMode).append(")");
          return matchExpre.toString();
        }
      }
    }

    return null;
  }

  public static String encode(String value) {
    if (!Utilities.isNullOrEmpty(value)) {
      StringBuilder encoded = new StringBuilder();
      for (int i=0; i<value.length(); i++) {
        char c = value.charAt(i);
        if (c == '\'' || c == '\\') {
          encoded.append("\\");
        }
        encoded.append(c);
      }

      return encoded.toString();
    }

    return value;
  }

  private String buildTableName(SqlTable table) throws Exception {
    SqlBuilder builder = SqlBuilder.createBuilder();
    builder.setBuildOptions(REBUILD_OPTIONS);
    return builder.buildTable(table);
  }
}