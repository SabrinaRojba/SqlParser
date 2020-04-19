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


public abstract class SqlStatement : ISqlElement, ISqlCloneable {
  protected Dialect dialectProcessor;
  protected SqlCollection<string> comments = new SqlCollection<string>();
  protected string tableName;
  protected SqlTable table;
  protected SqlCollection<SqlValueExpression> parasList = new SqlCollection<SqlValueExpression>();
  protected int parentParasNumber = 0;
  protected SqlCollection<SqlTable> _withClause = null;

  protected SqlStatement(Dialect dialectProcessor) {
    this.dialectProcessor = dialectProcessor;
  }

  public abstract void Accept(ISqlQueryVisitor visitor) ;

  public Dialect GetDialectProcessor() {
    return this.dialectProcessor;
  }

  public virtual SqlTable GetTable() {
    return table;
  }

  public SqlCollection<string> GetComments() {
    return this.comments;
  }

  public virtual SqlCollection<SqlColumn> GetColumns() {
    return new SqlCollection<SqlColumn>();
  }

  public virtual void SetColumns(SqlCollection<SqlColumn> columns) {
  }

  public virtual void SetWithClause(SqlCollection<SqlTable> withes) {
    this._withClause = withes;
  }

  public virtual SqlCollection<SqlTable> GetWithClause() {return this._withClause;}

  public virtual SqlConditionNode GetCriteria() {
    return null;
  }

  public virtual void SetCriteria(SqlConditionNode cond) {
  }

  public virtual void AddCondition(SqlConditionNode cond) {
  }

  public SqlCollection<SqlColumn> GetColumns(string name, string alias) {
    SqlCollection<SqlColumn> filteredIn = new SqlCollection<SqlColumn>();
    SqlCollection<SqlColumn> exps = GetColumns();
    if (exps != null) {
      foreach(SqlColumn exp in exps) {
        bool match = false;
        if (name != null) {
          match |= exp.Equals(name);
        }
        if (alias != null) {
          string expAlias = exp.GetAlias();
          match |=  (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(alias, expAlias));
        }
        if (match) {
          filteredIn.Add(exp);
        }
      }
    }
    return filteredIn;
  }

  public SqlCollection<string> GetColumnNames() {
    SqlCollection<string> colNames = new SqlCollection<string>();
    foreach(SqlColumn col in this.GetColumns()) {
      colNames.Add(col.GetColumnName());
    }
    return colNames;
  }

  public SqlColumn GetResolvedColumn(string tableOrAlias, string nameOrAlias) {
    if (nameOrAlias == null) {
      return null;
    }
    SqlTable resolvedTable = null;
    if (tableOrAlias != null && tableOrAlias.Length > 0) {
      resolvedTable = GetResolvedTable(tableOrAlias);
      if (resolvedTable != null && resolvedTable.IsNestedQueryTable()) {
        SqlQueryStatement nestedQuery = resolvedTable.GetQuery();
        return nestedQuery.GetResolvedColumn(null, nameOrAlias);
      }
    }
    foreach(SqlColumn column in this.GetColumns()) {
      string name = column.GetColumnName();
      string alias = column.GetAlias();
      SqlTable t = column.GetTable();
      bool tableMatch = false;
      if (resolvedTable != null) {
        string resolvedName = resolvedTable.GetName();
        string resolvedAlias = resolvedTable.GetAlias();
        if (t != null) {
          if (tableOrAlias.Equals(t.GetName()) || tableOrAlias.Equals(t.GetAlias())) {
            tableMatch = true;
          } else if (resolvedName.Equals(t.GetName()) || resolvedName.Equals(t.GetAlias())) {
            tableMatch = true;
          } else if (resolvedAlias.Equals(t.GetName()) || resolvedAlias.Equals(t.GetAlias())) {
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
      if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(nameOrAlias, name) || RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(nameOrAlias, alias)) {
        return (SqlColumn)column.Clone();
      }
    }
    return null;
  }

  public string GetResolvedTableName(string nameOralias) {
    SqlTable t = GetResolvedTable(nameOralias);
    if (t != null) {
      if (t.GetQuery() != null) {
        SqlTable subQueryTable = t.GetQuery().GetTable();
        if (subQueryTable != null) {
          return t.GetQuery().GetResolvedTableName(subQueryTable.GetName());
        }
      } else {
        return t.GetName();
      }
    }

    return nameOralias;
  }

  public virtual  SqlTable GetResolvedTable(string nameOralias) {
    SqlTable resolved = null;
    if (GetTable() != null && nameOralias != null) {
      SqlTable t = GetTable();
      if (Utilities.EqualIgnoreCase(nameOralias, t.GetName()) ||
         Utilities.EqualIgnoreCase(nameOralias, t.GetAlias())) {
        resolved = t;
      }
    } else {
      resolved = GetTable();
    }
    return resolved;
  }

  public SqlCollection<SqlValueExpression> GetParameterList() {
    return parasList;
  }

  public int GetParentParasNumber() {
    return this.parentParasNumber;
  }

  public string GetTableName() {
    if (!ParserCore.IsValidTableName(tableName)) {
      if (GetTable() != null) {
        SqlTable t = GetTable().Resolve();
        if (t != null && t.GetName() != null) {
          return t.GetName();
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

  public virtual void SetTable(SqlTable t) {
    this.table = t;
  }

  public void SetTableName(string name) {
    this.tableName = name;
  }

  public bool IsJoinQuery() {
    if (this is SqlSelectStatement) {
      return ParserCore.IsJoinStatement((SqlSelectStatement)this);
    }

    return false;
  }

  public abstract Object Clone();

  protected virtual void Copy(SqlStatement obj) {
    this.dialectProcessor = obj.dialectProcessor;
    this.comments = obj.comments == null ? null : (SqlCollection<string>)obj.comments.Clone();
    this.tableName = obj.tableName;
    this.table = obj.table;
    this._withClause = obj._withClause;
    if (obj.parasList == null) {
      this.parasList = null;
    } else {
      this.parasList =  new SqlCollection<SqlValueExpression>();
      foreach(SqlValueExpression p in obj.parasList) {
        this.parasList.Add(p);
      }
    }
  }

  public string ToString() {
    try {
      SqlBuilder bld = SqlBuilder.CreateBuilder();
      bld.SetStatement(this);
      return bld.Build();
    } catch(Exception ex) {
      return Utilities.GetExceptionMessage(ex);
    }
  }

}
}

