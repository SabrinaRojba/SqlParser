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

public class OracleDialect extends Dialect {
  private RebuildOptions _rebuildOptions = RebuildOptions.Oracle;
  //https://docs.oracle.com/cd/B28359_01/server.111/b28286/sql_elements001.htm#SQLRF50950
  //Oracle Built-in Datatypes
  //TODO Douglas the "TIMESTAMP (precision) WITH LOCAL TIME ZONE", "TIMESTAMP (precision) WITH TIME ZONE" flag will be supported later.
  private static final String[] SUPPORTED_TYPES = {"VARCHAR2", "NVARCHAR2", "NUMBER", "FLOAT",
      "LONG", "DATE", "BINARY_FLOAT", "BINARY_DOUBLE",
      "TIMESTAMP", "INTERVAL YEAR", "INTERVAL DAY", "RAW",
      "LONG RAW", "ROWID", "UROWID", "CHAR",
      "NCHAR", "CLOB", "NCLOB", "BLOB",
      "BFILE"
  };

  public void setRebuildOptions(RebuildOptions rebuildOptions) {
    this._rebuildOptions = rebuildOptions;
  }

  @Override
  public RebuildOptions getRebuildOptions() {
    return this._rebuildOptions;
  }

  @Override
  public String buildTableAlias(SqlTable table) throws Exception {
    StringBuilder clause = new StringBuilder();
    if (table.hasAlias()) {
      clause.append(" ");
      clause.append(this._rebuildOptions.quoteIdentifierWithDot(table.getAlias()));
    }
    return clause.toString();
  }

  @Override
  public String parseIdentifierName(SqlToken token) throws Exception {
    if (token.WasQuoted) {
      return token.Value;
    } else {
      return Utilities.toUpperCase(token.Value);
    }
  }

  @Override
  public String buildAlterAction(AlterAction tableAction) throws Exception {
    if (tableAction instanceof AlterTableAction) {
      AlterTableAction alterTableAction = (AlterTableAction) tableAction;
      SqlAlterOptions options = alterTableAction.getAlterOption();
      SqlCollection<SqlColumnDefinition> columnDefinitions = alterTableAction.getColumnDefinitions();
      if (options == SqlAlterOptions.ADD_COLUMN) {
        if (columnDefinitions.size() != 1) {
          throw new RSBException("OracleDialect", "Add column action must have one column definition.");
        }
        StringBuffer builder = new StringBuffer();
        builder.append(" ADD (");
        builder.append(buildColumnDefinition(columnDefinitions.get(0)));
        builder.append(")");
        return builder.toString();
      } else if (options == SqlAlterOptions.ALTER_COLUMN) {
        if (columnDefinitions.size() != 1) {
          throw new RSBException("OracleDialect", "Alter column action must have one column definition.");
        }
        StringBuffer builder = new StringBuffer();
        builder.append(" MODIFY (");
        SqlColumnDefinition columnDefinition = columnDefinitions.get(0);
        builder.append(this._rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(columnDefinition.ColumnName, this._rebuildOptions)));
        builder.append(" ").append(DialectUtilities.appendDataType(columnDefinition.DataType, columnDefinition.ColumnSize, columnDefinition.Scale));
        builder.append(")");
        return builder.toString();
      } else if (options == SqlAlterOptions.DROP_COLUMN) {
        StringBuffer builder = new StringBuffer();
        builder.append(" DROP COLUMN");
        if (columnDefinitions.size() != 1) {
          throw new RSBException("OracleDialect", "Drop column action must have one column definition.");
        }
        SqlColumnDefinition columnDefinition = columnDefinitions.get(0);
        builder.append(" ").append(this._rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(columnDefinition.ColumnName, this._rebuildOptions)));
        return builder.toString();
      } else if (options == SqlAlterOptions.RENAME_TABLE) {
        ReNameTableAction renameTableAction = (ReNameTableAction)alterTableAction;
        StringBuffer builder = new StringBuffer();
        builder.append(" RENAME TO");
        builder.append(" ").append(this._rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(renameTableAction.getNameToTable().getName(), this._rebuildOptions)));
        return builder.toString();
      } else if (options == SqlAlterOptions.RENAME_COLUMN) {
        ReNameColumnAction renameColumnAction = (ReNameColumnAction)alterTableAction;
        StringBuffer builder = new StringBuffer();
        builder.append(" RENAME COLUMN");
        builder.append(" ").append(this._rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(renameColumnAction.getSrcColumn().getColumnName(), this._rebuildOptions)));
        builder.append(" TO ").append(this._rebuildOptions.quoteIdentifierWithDot(BuilderCore.encodeIdentifier(renameColumnAction.getNameToColumn().getColumnName(), this._rebuildOptions)));
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
      tokenizer.EnsureNextToken("(");
      definitions.add(parseColumnDefinition(tokenizer));
      tokenizer.EnsureNextToken(")");
      return new AddColumnAction(definitions, false, false);
    } if (next.equals("MODIFY")) {
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("(");
      next = tokenizer.NextToken();
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
      definitions.add(sqlColumnDefinition);
      tokenizer.EnsureNextToken(")");
      return new AlterColumnAction(definitions);
    } else if (next.equals("DROP")) {
      SqlCollection<SqlColumnDefinition> definitions = new SqlCollection<SqlColumnDefinition>();
      tokenizer.NextToken();
      tokenizer.EnsureNextToken("COLUMN");
      next = tokenizer.NextToken();
      SqlColumnDefinition sqlColumnDefinition = new SqlColumnDefinition();
      sqlColumnDefinition.ColumnName = DialectUtilities.parseIdentifierName(next, this);;
      definitions.add(sqlColumnDefinition);
      return new DropColumnAction(definitions, false);
    } else if (next.equals("RENAME")) {
      tokenizer.NextToken();
      next = tokenizer.LookaheadToken2();
      if (next.equals("TO")) {
        //Rename table action
        tokenizer.NextToken();
        next = tokenizer.NextToken();
        String renameToTableName = DialectUtilities.parseIdentifierName(next, this);
        return new ReNameTableAction(new SqlTable(renameToTableName));
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
          throw new RSBException("OracleDialect", "Invalid column name : " + Utilities.join(",", identities));
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
    return DialectUtilities.buildColumnDefinition(definition, this._rebuildOptions);
  }

  @Override
  public SqlColumnDefinition parseColumnDefinition(SqlTokenizer tokenizer) throws Exception {
    return DialectUtilities.parseColumnDefinition(tokenizer, SUPPORTED_TYPES, this);
  }
}