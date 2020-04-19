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

sealed class SqlColumnSourceChecker : ISqlQueryVisitor {
  private readonly SqlCollection<SqlTable> _sourceTables = new SqlCollection<SqlTable>();
  private readonly TableFinder nestedTableFinder;
  private readonly TableFinder sourceTableFinder;

  public SqlColumnSourceChecker() {
    this.nestedTableFinder = new TableFinder(this._sourceTables, new NQTableMatcher());
    this.sourceTableFinder = new TableFinder(this._sourceTables, new SimpleSourceMatcher());
  }

  public void Visit(SqlTable table) {

  }

  public void Visit(SqlStatement stmt) {
    if (stmt is SqlSelectStatement) {
      SqlCollection<SqlTable> tables = ((SqlSelectStatement) stmt).GetTables();
      this._sourceTables.Clear();
      foreach(SqlTable sourceTable in tables) {
        try {
          sourceTable.Accept(nestedTableFinder);
          sourceTable.Accept(sourceTableFinder);
        } catch (Exception ex) {
          throw new RSBException("SqlColumnSourceChecker", Utilities.GetExceptionMessage(ex));
        }
      }
    }
  }

  public void Visit(SqlFormulaColumn column) {
    foreach(SqlExpression p in column.GetParameters()) {
      this.Visit(p);
    }
  }

  public void Visit(SqlExpression expr) {
    if (expr is SqlColumn) {
      this.Visit((SqlColumn) expr);
    } else if (expr is SqlConditionNode) {
      this.Visit((SqlConditionNode) expr);
    } else if (expr is SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) expr;
      this.Visit(opExpr.GetLeft());
      this.Visit(opExpr.GetRight());
    }
  }


  public void Visit(SqlColumn column) {
    if (column is SqlFormulaColumn) {
      this.Visit((SqlFormulaColumn) column);
      return;
    } else if (column is SqlWildcardColumn) {
      return;
    } else if (!(column is SqlGeneralColumn)) {
      return;
    }

    SqlGeneralColumn c = (SqlGeneralColumn) column;

    SqlCollection<SqlTable> targetSet = new SqlCollection<SqlTable>();

    if (c.GetTable() != null) {
      SqlUtilities.FetchElement(this._sourceTables,
          targetSet,
          new TableAliasComparable<SqlTable>(c.GetTable().GetAlias()));
    } else {
      targetSet = this._sourceTables;
    }

    SqlCollection<SqlColumn> mapped = new SqlCollection<SqlColumn>();
    foreach(SqlTable target in targetSet) {
      if (target.IsSourceTable()) {
        return;
      }
      if (!target.IsNestedQueryTable()) {
        continue;
      }

      SqlUtilities.FetchElement(target.GetQuery().GetColumns(),
          mapped,
          new ColumnSourceComparable<SqlColumn>(c.GetColumnName()));

      if (mapped.Size() > 0) {
        break;
      }
    }

    if (0 == mapped.Size()) {
      throw SqlExceptions.Exception(null, SqlExceptions.UNKNOWN_COLUMN_IN_FIELDS_LIST, c.GetAlias());
    }
  }

  public void Visit(SqlConditionNode criteria) {

  }

  class NQTableMatcher : ITableMatch {

    public SqlTable Create(SqlTable t) {
      if (t.IsNestedJoinTable()) {
        SqlTable nj = t.GetNestedJoin();
        do {
          if (nj == null) {
            break;
          }

          if (nj.IsNestedQueryTable()) {
            return new SqlTable(nj.GetQuery(), t.GetAlias());
          }

          if (nj.IsNestedJoinTable()) {
            nj = nj.GetNestedJoin();
            continue;
          }

          break;
        } while (true);
      }
      return new SqlTable(t.GetQuery(), t.GetAlias());
    }

    public bool Accept(SqlTable t, TablePartType type) {
      if (TablePartType.NestedQuery == type) {
        return true;
      }

      // ((SELECT...)) t0 is equal to (SELECT...) t0
      if (TablePartType.NestedJoin == type) {
        SqlTable nj = t.GetNestedJoin();
        do {
          if (null == nj) {
            break;
          }

          if (nj.IsNestedQueryTable() ) {
            return true;
          }

          if (nj.IsNestedJoinTable() && !nj.HasJoin()) {
            nj = nj.GetNestedJoin();
            continue;
          }
          break;
        } while (true);
      }

      return false;
    }

    public bool Unwind(SqlTable t, TablePartType type) {
      if (TablePartType.NestedJoin == type) {
        return true;
      }
      return false;
    }
  }

  class SimpleSourceMatcher : ITableMatch {

    public SqlTable Create(SqlTable t) {
      return new SqlTable(null, null, t.GetName(), t.GetAlias());
    }

    public bool Accept(SqlTable t, TablePartType type) {
      return TablePartType.SimpleTable == type;
    }

    public bool Unwind(SqlTable t, TablePartType type) {
      if (type == TablePartType.NestedJoin) {
        return true;
      }
      return false;
    }
  }

  class TableAliasComparable<T>  : ISqlElementComparable<T> where T : ISqlElement {
    private readonly string _alias;

    public TableAliasComparable(string alias) {
      this._alias = alias;
    }

    public int CompareTo(T o) {
      if (null == o) return -1;

      if (Utilities.EqualIgnoreCase(this._alias, ((SqlTable)(ISqlElement)o).GetAlias())) {
        return 0;
      }

      return -1;
    }
  }

  sealed class ColumnSourceComparable<T>  : ISqlElementComparable<T> where T : SqlColumn {
    private readonly string _popName;
    public ColumnSourceComparable(string popName) {
      this._popName = popName;
    }

    public int CompareTo(T o) {
      if (null == o) return -1;

      if (Utilities.EqualIgnoreCase(this._popName, o.GetAlias())) {
        return 0;
      }

      if (o is SqlWildcardColumn) {
        return 0;
      }

      return -1;
    }
  }
}
}

