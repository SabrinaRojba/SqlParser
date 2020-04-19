//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;

namespace CData.Sql {
#*/

import core.ParserCore;
import rssbus.oputils.common.Utilities;

public abstract class SqlStatement implements ISqlElement, ISqlCloneable {
  protected Dialect dialectProcessor;
  protected SqlCollection<String> comments = new SqlCollection<String>();
  protected String tableName;
  protected SqlTable table;
  protected SqlCollection<SqlValueExpression> parasList = new SqlCollection<SqlValueExpression>();
  protected int parentParasNumber = 0;
  protected SqlCollection<SqlTable> _withClause = null;

  protected SqlStatement(Dialect dialectProcessor) {
    this.dialectProcessor = dialectProcessor;
  }

  public abstract void accept(ISqlQueryVisitor visitor) throws Exception;

  public Dialect getDialectProcessor() {
    return this.dialectProcessor;
  }

  public /*#virtual#*/ SqlTable getTable() {
    return table;
  }

  public SqlCollection<String> getComments() {
    return this.comments;
  }

  public /*#virtual#*/ SqlCollection<SqlColumn> getColumns() {
    return new SqlCollection<SqlColumn>();
  }

  public /*#virtual#*/ void setColumns(SqlCollection<SqlColumn> columns) {
  }

  public /*#virtual#*/ void setWithClause(SqlCollection<SqlTable> withes) {
    this._withClause = withes;
  }

  public /*#virtual#*/ SqlCollection<SqlTable> getWithClause() {return this._withClause;}

  public /*#virtual#*/ SqlConditionNode getCriteria() {
    return null;
  }

  public /*#virtual #*/void setCriteria(SqlConditionNode cond) {
  }

  public /*#virtual #*/void addCondition(SqlConditionNode cond) {
  }

  public SqlCollection<SqlColumn> getColumns(String name, String alias) {
    SqlCollection<SqlColumn> filteredIn = new SqlCollection<SqlColumn>();
    SqlCollection<SqlColumn> exps = getColumns();
    if (exps != null) {
      for (SqlColumn exp : exps) {
        boolean match = false;
        if (name != null) {
          match |= exp.equals(name);
        }
        if (alias != null) {
          String expAlias = exp.getAlias();
          match |=  (alias.equalsIgnoreCase(expAlias));
        }
        if (match) {
          filteredIn.add(exp);
        }
      }
    }
    return filteredIn;
  }

  public SqlCollection<String> GetColumnNames() {
    SqlCollection<String> colNames = new SqlCollection<String>();
    for (SqlColumn col : this.getColumns()) {
      colNames.add(col.getColumnName());
    }
    return colNames;
  }

  public SqlColumn getResolvedColumn(String tableOrAlias, String nameOrAlias) {
    if (nameOrAlias == null) {
      return null;
    }
    SqlTable resolvedTable = null;
    if (tableOrAlias != null && tableOrAlias.length() > 0) {
      resolvedTable = getResolvedTable(tableOrAlias);
      if (resolvedTable != null && resolvedTable.isNestedQueryTable()) {
        SqlQueryStatement nestedQuery = resolvedTable.getQuery();
        return nestedQuery.getResolvedColumn(null, nameOrAlias);
      }
    }
    for (SqlColumn column : this.getColumns()) {
      String name = column.getColumnName();
      String alias = column.getAlias();
      SqlTable t = column.getTable();
      boolean tableMatch = false;
      if (resolvedTable != null) {
        String resolvedName = resolvedTable.getName();
        String resolvedAlias = resolvedTable.getAlias();
        if (t != null) {
          if (tableOrAlias.equals(t.getName()) || tableOrAlias.equals(t.getAlias())) {
            tableMatch = true;
          } else if (resolvedName.equals(t.getName()) || resolvedName.equals(t.getAlias())) {
            tableMatch = true;
          } else if (resolvedAlias.equals(t.getName()) || resolvedAlias.equals(t.getAlias())) {
            tableMatch = true;
          } else {
            tableMatch = false;
          }
        } else {
          tableMatch = true;
        }
      } else {
        tableMatch = true;
      }
      if (!tableMatch) {
        continue;
      }
      if (nameOrAlias.equalsIgnoreCase(name) || nameOrAlias.equalsIgnoreCase(alias)) {
        return (SqlColumn)column.clone();
      }
    }
    return null;
  }

  public String getResolvedTableName(String nameOralias) {
    SqlTable t = getResolvedTable(nameOralias);
    if (t != null) {
      if (t.getQuery() != null) {
        SqlTable subQueryTable = t.getQuery().getTable();
        if (subQueryTable != null) {
          return t.getQuery().getResolvedTableName(subQueryTable.getName());
        }
      } else {
        return t.getName();
      }
    }

    return nameOralias;
  }

  public /*#virtual#*/  SqlTable getResolvedTable(String nameOralias) {
    SqlTable resolved = null;
    if (getTable() != null && nameOralias != null) {
      SqlTable t = getTable();
      if (Utilities.equalIgnoreCase(nameOralias, t.getName()) ||
         Utilities.equalIgnoreCase(nameOralias, t.getAlias())) {
        resolved = t;
      }
    } else {
      resolved = getTable();
    }
    return resolved;
  }

  public SqlCollection<SqlValueExpression> getParameterList() {
    return parasList;
  }

  public int getParentParasNumber() {
    return this.parentParasNumber;
  }

  public String getTableName() {
    if (!ParserCore.isValidTableName(tableName)) {
      if (getTable() != null) {
        SqlTable t = getTable().resolve();
        if (t != null && t.getName() != null) {
          return t.getName();
        } else {
          return "";
        }
      } else {
        return "";
      }
    } else {
      return tableName;
    }
  }

  public /*#virtual #*/void setTable(SqlTable t) {
    this.table = t;
  }

  public void setTableName(String name) {
    this.tableName = name;
  }

  public boolean isJoinQuery() {
    if (this instanceof SqlSelectStatement) {
      return ParserCore.isJoinStatement((SqlSelectStatement)this);
    }

    return false;
  }

  public abstract Object clone();

  protected /*#virtual #*/void copy(SqlStatement obj) {
    this.dialectProcessor = obj.dialectProcessor;
    this.comments = obj.comments == null ? null : (SqlCollection<String>)obj.comments.clone();
    this.tableName = obj.tableName;
    this.table = obj.table;
    this._withClause = obj._withClause;
    if (obj.parasList == null) {
      this.parasList = null;
    } else {
      this.parasList =  new SqlCollection<SqlValueExpression>();
      for (SqlValueExpression p : obj.parasList) {
        this.parasList.add(p);
      }
    }
  }

  public String toString() {
    try {
      SqlBuilder bld = SqlBuilder.createBuilder();
      bld.setStatement(this);
      return bld.build();
    } catch(Exception ex) {
      return Utilities.getExceptionMessage(ex);
    }
  }

}
