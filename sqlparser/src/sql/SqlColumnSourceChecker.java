//@
package cdata.sql;
import rssbus.RSBException;
import rssbus.oputils.common.Utilities;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
import core.SqlExceptions;

final class SqlColumnSourceChecker implements ISqlQueryVisitor {
  private final SqlCollection<SqlTable> _sourceTables = new SqlCollection<SqlTable>();
  private final TableFinder nestedTableFinder;
  private final TableFinder sourceTableFinder;

  public SqlColumnSourceChecker() throws Exception {
    this.nestedTableFinder = new TableFinder(this._sourceTables, new NQTableMatcher());
    this.sourceTableFinder = new TableFinder(this._sourceTables, new SimpleSourceMatcher());
  }

  public void visit(SqlTable table) throws RSBException {

  }

  public void visit(SqlStatement stmt) throws RSBException {
    if (stmt instanceof SqlSelectStatement) {
      SqlCollection<SqlTable> tables = ((SqlSelectStatement) stmt).getTables();
      this._sourceTables.clear();
      for (SqlTable sourceTable : tables) {
        try {
          sourceTable.accept(nestedTableFinder);
          sourceTable.accept(sourceTableFinder);
        } catch (Exception ex) {
          throw new RSBException("SqlColumnSourceChecker", Utilities.getExceptionMessage(ex));
        }
      }
    }
  }

  public void visit(SqlFormulaColumn column) throws RSBException {
    for (SqlExpression p : column.getParameters()) {
      this.visit(p);
    }
  }

  public void visit(SqlExpression expr) throws RSBException {
    if (expr instanceof SqlColumn) {
      this.visit((SqlColumn) expr);
    } else if (expr instanceof SqlConditionNode) {
      this.visit((SqlConditionNode) expr);
    } else if (expr instanceof SqlOperationExpression) {
      SqlOperationExpression opExpr = (SqlOperationExpression) expr;
      this.visit(opExpr.getLeft());
      this.visit(opExpr.getRight());
    }
  }


  public void visit(SqlColumn column) throws RSBException {
    if (column instanceof SqlFormulaColumn) {
      this.visit((SqlFormulaColumn) column);
      return;
    } else if (column instanceof SqlWildcardColumn) {
      return;
    } else if (!(column instanceof SqlGeneralColumn)) {
      return;
    }

    SqlGeneralColumn c = (SqlGeneralColumn) column;

    SqlCollection<SqlTable> targetSet = new SqlCollection<SqlTable>();

    if (c.getTable() != null) {
      SqlUtilities.fetchElement(this._sourceTables,
          targetSet,
          new TableAliasComparable<SqlTable>(c.getTable().getAlias()));
    } else {
      targetSet = this._sourceTables;
    }

    SqlCollection<SqlColumn> mapped = new SqlCollection<SqlColumn>();
    for (SqlTable target : targetSet) {
      if (target.isSourceTable()) {
        return;
      }
      if (!target.isNestedQueryTable()) {
        continue;
      }

      SqlUtilities.fetchElement(target.getQuery().getColumns(),
          mapped,
          new ColumnSourceComparable<SqlColumn>(c.getColumnName()));

      if (mapped.size() > 0) {
        break;
      }
    }

    if (0 == mapped.size()) {
      throw SqlExceptions.Exception(null, SqlExceptions.UNKNOWN_COLUMN_IN_FIELDS_LIST, c.getAlias());
    }
  }

  public void visit(SqlConditionNode criteria) throws RSBException {

  }

  class NQTableMatcher implements ITableMatch {

    public SqlTable create(SqlTable t) {
      if (t.isNestedJoinTable()) {
        SqlTable nj = t.getNestedJoin();
        do {
          if (nj == null) {
            break;
          }

          if (nj.isNestedQueryTable()) {
            return new SqlTable(nj.getQuery(), t.getAlias());
          }

          if (nj.isNestedJoinTable()) {
            nj = nj.getNestedJoin();
            continue;
          }

          break;
        } while (true);
      }
      return new SqlTable(t.getQuery(), t.getAlias());
    }

    public boolean accept(SqlTable t, TablePartType type) {
      if (TablePartType.NestedQuery == type) {
        return true;
      }

      // ((SELECT...)) t0 is equal to (SELECT...) t0
      if (TablePartType.NestedJoin == type) {
        SqlTable nj = t.getNestedJoin();
        do {
          if (null == nj) {
            break;
          }

          if (nj.isNestedQueryTable() ) {
            return true;
          }

          if (nj.isNestedJoinTable() && !nj.hasJoin()) {
            nj = nj.getNestedJoin();
            continue;
          }
          break;
        } while (true);
      }

      return false;
    }

    public boolean unwind(SqlTable t, TablePartType type) {
      if (TablePartType.NestedJoin == type) {
        return true;
      }
      return false;
    }
  }

  class SimpleSourceMatcher implements ITableMatch {

    public SqlTable create(SqlTable t) {
      return new SqlTable(null, null, t.getName(), t.getAlias());
    }

    public boolean accept(SqlTable t, TablePartType type) {
      return TablePartType.SimpleTable == type;
    }

    public boolean unwind(SqlTable t, TablePartType type) {
      if (type == TablePartType.NestedJoin) {
        return true;
      }
      return false;
    }
  }

  class TableAliasComparable/*#<T>#*/ /*@*/<T extends SqlTable> implements /*@*/ /*#:#*/ ISqlElementComparable<T> /*#where T : ISqlElement#*/ {
    private final String _alias;

    public TableAliasComparable(String alias) {
      this._alias = alias;
    }

    public int compareTo(T o) {
      if (null == o) return -1;

      if (Utilities.equalIgnoreCase(this._alias, ((SqlTable)(ISqlElement)o).getAlias())) {
        return 0;
      }

      return -1;
    }
  }

  final class ColumnSourceComparable/*#<T>#*/ /*@*/<T extends SqlColumn> implements/*@*/ /*#:#*/ ISqlElementComparable<T> /*#where T : SqlColumn#*/ {
    private final String _popName;
    public ColumnSourceComparable(String popName) {
      this._popName = popName;
    }

    public int compareTo(T o) {
      if (null == o) return -1;

      if (Utilities.equalIgnoreCase(this._popName, o.getAlias())) {
        return 0;
      }

      if (o instanceof SqlWildcardColumn) {
        return 0;
      }

      return -1;
    }
  }
}
