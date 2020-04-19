using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;

namespace RSSBus.core {

using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Collections.Generic;

sealed class SqlTableModifier : SqlModifier {
  public const int TABLE = 1;
  public const int SCHEMA = 2;
  public const int CATALOG = 4;
  public const int FULL = CATALOG | SCHEMA | TABLE;

  private int _maxTableNameSize;
  private int _tableNamePolicy;
  private string _defaultCatalog;
  private string _defaultSchema;

  public SqlTableModifier() {
    this._maxTableNameSize = -1;
    this._tableNamePolicy = FULL;
    this._defaultCatalog = null;
    this._defaultSchema = null;
  }

  public void SetMaxTableNameSize(int val) {
    this._maxTableNameSize = val;
  }

  public void SetTableNamePolicy(int val) {
    this._tableNamePolicy = val;
  }

  public void SetDefaultCatalog(string val) {
    this._defaultCatalog = val;
  }

  public void SetDefaultSchema(string val) {
    this._defaultSchema = val;
  }



  protected override SqlTable Visit(SqlTable element) {

    if (this._clause == CLAUSE_TYPE.TABLE) {
      string catalogName = null;
      string orgCatalogName = element.GetCatalog();
      if ((this._tableNamePolicy & CATALOG) > 0) {
        catalogName = orgCatalogName;
        if (catalogName == null) {
          catalogName = this._defaultCatalog;
        }
      }

      string schemaName = null;
      string orgSchemaName = element.GetSchema();
      if ((this._tableNamePolicy & SCHEMA) > 0) {
        schemaName = orgSchemaName;
        if (schemaName == null) {
          schemaName = this._defaultSchema;
        }
      }

      string orgTableName = element.GetName();
      string tableName = orgTableName;
      if (this._maxTableNameSize > 0) {
        if (tableName != null && tableName.Length > this._maxTableNameSize) {
          tableName = RSSBus.core.j2cs.Converter.GetSubstring(tableName, 0, this._maxTableNameSize);
        }
      }

      bool modified = false;
      if (!Utilities.EqualIgnoreCase(orgCatalogName, catalogName)
              || !Utilities.EqualIgnoreCase(orgSchemaName, schemaName)
              || !Utilities.EqualIgnoreCase(orgTableName, tableName)) {
        modified = true;
      }

      if (modified) {
        element = new SqlTable(catalogName,
                schemaName,
                tableName,
                element.HasAlias() ? element.GetAlias() : orgTableName,
                element.GetJoin(),
                element.GetNestedJoin(),
                element.GetQuery(),
                element.GetTableValueFunction(),
                element.GetCrossApply());
      }
    }

    return base.Visit(element);
  }



  protected override SqlColumn Visit(SqlColumn element) {

    if (element is SqlSubQueryColumn) {
      SqlQueryStatement subQuery = (SqlQueryStatement)((SqlSubQueryExpression)element.GetExpr()).GetQuery().Clone();
      if (this.Modify(new SqlParser(subQuery))) {
        element = new SqlSubQueryColumn(element.GetAlias(), new SqlSubQueryExpression(subQuery));
      }
    }

    return base.Visit(element);
  }
}

class SqlColumnModifier : SqlModifier {
  private JavaArrayList<SqlModifierRule> _rules;

  public SqlColumnModifier() {
    this._rules = new JavaArrayList<SqlModifierRule>();
  }

  public void AddRenameRule(string catalog, string schema, string tableName, string columnName, string newColumnName) {
    RenameColumnRule rule = new RenameColumnRule(catalog, schema, tableName, columnName, newColumnName);
    this._rules.Add(rule);
  }

  public void AddMakeUpAlisaRule(string catalog, string schema, string tableName, string columnName) {
    MakeUpAliasRule rule = new MakeUpAliasRule(catalog, schema, tableName, columnName);
    this._rules.Add(rule);
  }



  protected override SqlColumn Visit(SqlColumn element) {

    int location = GetLocation(element);
    SqlModifierRule[] rules = FindRules(location, element);
    foreach(SqlModifierRule r in rules) {
      element = r.Change(element);
    }
    return base.Visit(element);
  }

  private SqlModifierRule[] FindRules(int location, SqlColumn column) {
    JavaArrayList<SqlModifierRule> matchedRules = new JavaArrayList<SqlModifierRule>();
    int count = this._rules.Size();
    SqlTable refTable = this._statement.GetResolvedTable(column.GetTable() == null ? null : column.GetTable().GetName());
    for(int i = 0; i < count; i ++) {
      SqlModifierRule rule = this._rules.Get(i);
      if ((refTable == null && rule.Match(location, null, null, null, column.GetColumnName()))
          || (refTable != null && rule.Match(location, refTable.GetCatalog(), refTable.GetSchema(), refTable.GetName(), column.GetColumnName()))) {
        matchedRules.Add(rule);
      }
    }
    return matchedRules.ToArray(typeof(SqlModifierRule));
  }

  private int GetLocation(SqlColumn column) {
    int location = SqlModifierRule.UNKNOWN;
    if (this._statement.GetColumns().IndexOf(column) > -1) {
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

  internal class MakeUpAliasRule : SqlModifierRule {
    private string _catalog;
    private string _schema;
    private string _tableName;
    private string _columnName;

    public MakeUpAliasRule(string catalog, string schema, string tableName, string columnName) {
      this._catalog = catalog;
      this._schema = schema;
      this._tableName = tableName;
      this._columnName = columnName;
    }

     override
    internal protected SqlColumn Change(SqlColumn column) {
      if (column is SqlGeneralColumn && !(column is SqlWildcardColumn) && !column.HasAlias()) {
        column = new SqlGeneralColumn(column.GetTable(), column.GetColumnName(), column.GetColumnName(), column.GetValueExpr());
      }
      return column;
    }

     override
    internal protected bool Match(int location, string catalog, string schema, string tableName, string columnName) {
      if (location == SqlModifierRule.PROJECTION
          && (this._catalog == null || Utilities.EqualIgnoreCase(catalog, this._catalog))
          && (this._schema == null || Utilities.EqualIgnoreCase(schema, this._schema))
          && (this._tableName == null || Utilities.EqualIgnoreCase(tableName, this._tableName))
          && (this._columnName == null || Utilities.EqualIgnoreCase(columnName, this._columnName))) {
        return true;
      }
      return false;
    }
  }

  internal class RenameColumnRule : SqlModifierRule {
    private string _catalog;
    private string _schema;
    private string _tableName;
    private string _columnName;
    private string _newColumnName;

    public RenameColumnRule(string catalog, string schema, string tableName, string columnName, string newColumnName) {
      this._catalog = catalog;
      this._schema = schema;
      this._tableName = tableName;
      this._columnName = columnName;
      this._newColumnName = newColumnName;
    }

     override
    internal protected bool Match(int location, string catalog, string schema, string tableName, string columnName) {
      if ((this._catalog == null || Utilities.EqualIgnoreCase(catalog, this._catalog))
        && (this._schema == null || Utilities.EqualIgnoreCase(schema, this._schema))
        && (this._tableName == null || Utilities.EqualIgnoreCase(tableName, this._tableName))
        && Utilities.EqualIgnoreCase(columnName, this._columnName)) {
        return true;
      }
      return false;
    }

     override
    internal protected SqlColumn Change(SqlColumn column) {
      if (column is SqlGeneralColumn && !(column is SqlWildcardColumn)) {
        if (column.HasAlias()) {
          column = new SqlGeneralColumn(column.GetTable(), this._newColumnName, column.GetAlias(), column.GetValueExpr());
        } else {
          column = new SqlGeneralColumn(column.GetTable(), this._newColumnName, column.GetValueExpr());
        }
      }
      return column;
    }
  }
}

abstract class SqlModifierRule {
  internal protected const int UNKNOWN = 0;
  internal protected const int PROJECTION = 1;
  internal protected const int INFORMULA = 2;
  internal protected const int CRITERIA = 3;
  internal protected const int GROUPBY = 4;
  internal protected const int HAVING = 5;
  internal protected const int ORDERBY = 6;

  internal protected abstract SqlColumn Change(SqlColumn column);
  internal protected abstract bool Match(int location, string catalog, string schema, string tableName, string columnName);
}
}

