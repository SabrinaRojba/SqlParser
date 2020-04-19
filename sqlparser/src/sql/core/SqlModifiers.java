package core;
/*#
using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Collections.Generic;
#*/
import cdata.sql.*;
import rssbus.oputils.common.Utilities;

import java.util.ArrayList;
import java.util.List;

final class SqlTableModifier extends SqlModifier {
  public static final int TABLE = 1;
  public static final int SCHEMA = 2;
  public static final int CATALOG = 4;
  public static final int FULL = CATALOG | SCHEMA | TABLE;

  private int _maxTableNameSize;
  private int _tableNamePolicy;
  private String _defaultCatalog;
  private String _defaultSchema;

  public SqlTableModifier() {
    this._maxTableNameSize = -1;
    this._tableNamePolicy = FULL;
    this._defaultCatalog = null;
    this._defaultSchema = null;
  }

  public void setMaxTableNameSize(int val) {
    this._maxTableNameSize = val;
  }

  public void setTableNamePolicy(int val) {
    this._tableNamePolicy = val;
  }

  public void setDefaultCatalog(String val) {
    this._defaultCatalog = val;
  }

  public void setDefaultSchema(String val) {
    this._defaultSchema = val;
  }

//@
  protected SqlTable visit(SqlTable element) throws Exception {
//@
/*#
  protected override SqlTable Visit(SqlTable element) {
#*/
    if (this._clause == CLAUSE_TYPE.TABLE) {
      String catalogName = null;
      String orgCatalogName = element.getCatalog();
      if ((this._tableNamePolicy & CATALOG) > 0) {
        catalogName = orgCatalogName;
        if (catalogName == null) {
          catalogName = this._defaultCatalog;
        }
      }

      String schemaName = null;
      String orgSchemaName = element.getSchema();
      if ((this._tableNamePolicy & SCHEMA) > 0) {
        schemaName = orgSchemaName;
        if (schemaName == null) {
          schemaName = this._defaultSchema;
        }
      }

      String orgTableName = element.getName();
      String tableName = orgTableName;
      if (this._maxTableNameSize > 0) {
        if (tableName != null && tableName.length() > this._maxTableNameSize) {
          tableName = tableName.substring(0, this._maxTableNameSize);
        }
      }

      boolean modified = false;
      if (!Utilities.equalIgnoreCase(orgCatalogName, catalogName)
              || !Utilities.equalIgnoreCase(orgSchemaName, schemaName)
              || !Utilities.equalIgnoreCase(orgTableName, tableName)) {
        modified = true;
      }

      if (modified) {
        element = new SqlTable(catalogName,
                schemaName,
                tableName,
                element.hasAlias() ? element.getAlias() : orgTableName,
                element.getJoin(),
                element.getNestedJoin(),
                element.getQuery(),
                element.getTableValueFunction(),
                element.getCrossApply());
      }
    }

    return super.visit(element);
  }

//@
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    if (element instanceof SqlSubQueryColumn) {
      SqlQueryStatement subQuery = (SqlQueryStatement)((SqlSubQueryExpression)element.getExpr()).getQuery().clone();
      if (this.modify(new SqlParser(subQuery))) {
        element = new SqlSubQueryColumn(element.getAlias(), new SqlSubQueryExpression(subQuery));
      }
    }

    return super.visit(element);
  }
}

class SqlColumnModifier extends SqlModifier {
  private ArrayList<SqlModifierRule> _rules;

  public SqlColumnModifier() {
    this._rules = new ArrayList<SqlModifierRule>();
  }

  public void addRenameRule(String catalog, String schema, String tableName, String columnName, String newColumnName) {
    RenameColumnRule rule = new RenameColumnRule(catalog, schema, tableName, columnName, newColumnName);
    this._rules.add(rule);
  }

  public void addMakeUpAlisaRule(String catalog, String schema, String tableName, String columnName) {
    MakeUpAliasRule rule = new MakeUpAliasRule(catalog, schema, tableName, columnName);
    this._rules.add(rule);
  }

//@
  protected SqlColumn visit(SqlColumn element) throws Exception {
//@
/*#
  protected override SqlColumn Visit(SqlColumn element) {
#*/
    int location = getLocation(element);
    SqlModifierRule[] rules = findRules(location, element);
    for (SqlModifierRule r : rules) {
      element = r.change(element);
    }
    return super.visit(element);
  }

  private SqlModifierRule[] findRules(int location, SqlColumn column) {
    ArrayList<SqlModifierRule> matchedRules = new ArrayList<SqlModifierRule>();
    int count = this._rules.size();
    SqlTable refTable = this._statement.getResolvedTable(column.getTable() == null ? null : column.getTable().getName());
    for(int i = 0; i < count; i ++) {
      SqlModifierRule rule = this._rules.get(i);
      if ((refTable == null && rule.match(location, null, null, null, column.getColumnName()))
          || (refTable != null && rule.match(location, refTable.getCatalog(), refTable.getSchema(), refTable.getName(), column.getColumnName()))) {
        matchedRules.add(rule);
      }
    }
    return matchedRules.toArray(/*@*/new SqlModifierRule[matchedRules.size()]/*@*//*#typeof(SqlModifierRule)#*/);
  }

  private int getLocation(SqlColumn column) {
    int location = SqlModifierRule.UNKNOWN;
    if (this._statement.getColumns().indexOf(column) > -1) {
      location = SqlModifierRule.PROJECTION;
    } else if (this._clause == CLAUSE_TYPE.CRITERIA) {
      location = SqlModifierRule.CRITERIA;
    } else if (this._clause == CLAUSE_TYPE.ORDERBY) {
      location = SqlModifierRule.ORDERBY;
    } else if (this._clause == CLAUSE_TYPE.HAVING) {
      location = SqlModifierRule.HAVING;
    } else if (this._clause == CLAUSE_TYPE.GROUPBY) {
      location = SqlModifierRule.GROUPBY;
    }
    return location;
  }

  class MakeUpAliasRule extends SqlModifierRule {
    private String _catalog;
    private String _schema;
    private String _tableName;
    private String _columnName;

    public MakeUpAliasRule(String catalog, String schema, String tableName, String columnName) {
      this._catalog = catalog;
      this._schema = schema;
      this._tableName = tableName;
      this._columnName = columnName;
    }

    @Override
    /*#internal #*/protected SqlColumn change(SqlColumn column) {
      if (column instanceof SqlGeneralColumn && !(column instanceof SqlWildcardColumn) && !column.hasAlias()) {
        column = new SqlGeneralColumn(column.getTable(), column.getColumnName(), column.getColumnName(), column.getValueExpr());
      }
      return column;
    }

    @Override
    /*#internal #*/protected boolean match(int location, String catalog, String schema, String tableName, String columnName) {
      if (location == SqlModifierRule.PROJECTION
          && (this._catalog == null || Utilities.equalIgnoreCase(catalog, this._catalog))
          && (this._schema == null || Utilities.equalIgnoreCase(schema, this._schema))
          && (this._tableName == null || Utilities.equalIgnoreCase(tableName, this._tableName))
          && (this._columnName == null || Utilities.equalIgnoreCase(columnName, this._columnName))) {
        return true;
      }
      return false;
    }
  }

  class RenameColumnRule extends SqlModifierRule {
    private String _catalog;
    private String _schema;
    private String _tableName;
    private String _columnName;
    private String _newColumnName;

    public RenameColumnRule(String catalog, String schema, String tableName, String columnName, String newColumnName) {
      this._catalog = catalog;
      this._schema = schema;
      this._tableName = tableName;
      this._columnName = columnName;
      this._newColumnName = newColumnName;
    }

    @Override
    /*#internal #*/protected boolean match(int location, String catalog, String schema, String tableName, String columnName) {
      if ((this._catalog == null || Utilities.equalIgnoreCase(catalog, this._catalog))
        && (this._schema == null || Utilities.equalIgnoreCase(schema, this._schema))
        && (this._tableName == null || Utilities.equalIgnoreCase(tableName, this._tableName))
        && Utilities.equalIgnoreCase(columnName, this._columnName)) {
        return true;
      }
      return false;
    }

    @Override
    /*#internal #*/protected SqlColumn change(SqlColumn column) {
      if (column instanceof SqlGeneralColumn && !(column instanceof SqlWildcardColumn)) {
        if (column.hasAlias()) {
          column = new SqlGeneralColumn(column.getTable(), this._newColumnName, column.getAlias(), column.getValueExpr());
        } else {
          column = new SqlGeneralColumn(column.getTable(), this._newColumnName, column.getValueExpr());
        }
      }
      return column;
    }
  }
}

abstract class SqlModifierRule {
  protected static final int UNKNOWN = 0;
  protected static final int PROJECTION = 1;
  protected static final int INFORMULA = 2;
  protected static final int CRITERIA = 3;
  protected static final int GROUPBY = 4;
  protected static final int HAVING = 5;
  protected static final int ORDERBY = 6;

  protected abstract SqlColumn change(SqlColumn column);
  protected abstract boolean match(int location, String catalog, String schema, String tableName, String columnName);
}
