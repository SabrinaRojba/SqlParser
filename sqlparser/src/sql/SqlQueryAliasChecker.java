//@
package cdata.sql;
import core.SqlExceptions;
import rssbus.RSBException;
import rssbus.oputils.common.Utilities;
//@

/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

final class SqlQueryAliasChecker implements ISqlQueryVisitor {
  public static final String RESOLVE_TABLE_ALIAS_EXCEPTION_CODE = "SqlQueryChecker";
  private final SqlCollection<SqlTable> _flattenTables;

  public SqlQueryAliasChecker(SqlCollection<SqlTable> tableReferences) {
    this._flattenTables = tableReferences;
  }

  public void visit(SqlTable table) throws RSBException {

    SqlCollection<SqlTable> leftRelations = new SqlCollection<SqlTable>();
    TableFinder left_collector = new TableFinder(leftRelations, new TableOnlyMatcher());

    //check only table part
    SqlTableRelationChecker.checkOnlyTablePart(table);
    try {
      table.accept(left_collector);
    } catch (Exception ex) {
      throw new RSBException("SqlQueryChecker", Utilities.getExceptionMessage(ex));
    }

    //check joins.
    while (table.hasJoin()) {
      SqlJoin j = table.getJoin();
      SqlCollection<SqlTable> rightRelations = new SqlCollection<SqlTable>();
      TableFinder right_collector = new TableFinder(rightRelations, new TableOnlyMatcher());
      try {
        j.getTable().accept(right_collector);
      } catch (Exception ex) {
        throw new RSBException("SqlQueryChecker", Utilities.getExceptionMessage(ex));
      }
      if (j.getCondition() != null) {
        SqlTableRelationChecker onChecker = new SqlTableRelationChecker(leftRelations, rightRelations);
        try {
          j.getCondition().accept(onChecker);
        } catch (Exception ex) {
          throw new RSBException("SqlQueryChecker", Utilities.getExceptionMessage(ex));
        }
      }
      table = j.getTable();
      SqlTableRelationChecker.checkOnlyTablePart(table);
      leftRelations.addAll(rightRelations);
    }
  }

  public void visit(SqlStatement stmt) throws RSBException {
  }

  public void visit(SqlColumn column) throws RSBException {
    if (null == column.getTable()) return;

    boolean valid = checkOwnerOfColumn(column.getTable().getAlias(), this._flattenTables);
    if (!valid) {
      throw SqlExceptions.Exception(RESOLVE_TABLE_ALIAS_EXCEPTION_CODE, SqlExceptions.NO_TABLE_REFERENCE_INCLUDE_IN_FROM, column.getTable().getAlias());
    }
  }

  public void visit(SqlConditionNode criteria) throws RSBException {
    if (criteria instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria) criteria;
      if (c.getLeft() instanceof SqlColumn) {
        visit((SqlColumn)c.getLeft());
      }
      if (c.getRight() instanceof SqlColumn) {
        visit((SqlColumn)c.getRight());
      }
    }
  }

  public static boolean checkOwnerOfColumn(String owner, SqlCollection<SqlTable> flatten) throws RSBException {
    boolean valid = false;
    for (SqlTable t : flatten) {
      if (Utilities.equalIgnoreCaseInvariant(owner, t.getAlias())) {
        valid = true;
        break;
      }
      if (Utilities.equalIgnoreCaseInvariant(owner, t.getName())) {
        valid = true;
        break;
      }
    }
    return valid;
  }
}

final class SqlTableRelationChecker implements ISqlQueryVisitor {
  private final SqlCollection<SqlTable> _leftRelations;
  private final SqlCollection<SqlTable> _rightRelations;
  public SqlTableRelationChecker(SqlCollection<SqlTable> left, SqlCollection<SqlTable> right) {
    this._leftRelations = left;
    this._rightRelations = right;
  }

  public void visit(SqlTable table) throws RSBException {
  }

  public void visit(SqlStatement stmt) throws RSBException {

  }

  public void visit(SqlColumn column) throws RSBException {

  }

  public void visit(SqlConditionNode criteria) throws RSBException {
    if (criteria instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria) criteria;
      if (!(c.getLeft() instanceof SqlColumn )) {
        return;
      }

      if (!(c.getRight() instanceof SqlColumn )) {
        return;
      }

      SqlColumn l = (SqlColumn) c.getLeft();
      SqlColumn r = (SqlColumn) c.getRight();

      boolean lv = null == l.getTable()? true : SqlQueryAliasChecker.checkOwnerOfColumn(l.getTable().getAlias(), this._leftRelations) || SqlQueryAliasChecker.checkOwnerOfColumn(l.getTable().getAlias(), this._rightRelations);

      if (!lv) {
        throw SqlExceptions.Exception(SqlQueryAliasChecker.RESOLVE_TABLE_ALIAS_EXCEPTION_CODE,
            SqlExceptions.NO_TABLE_REFERENCE_INCLUDE_IN_FROM, l.getTable().getAlias());
      }

      boolean rv = null == r.getTable()? true : SqlQueryAliasChecker.checkOwnerOfColumn(r.getTable().getAlias(), this._leftRelations) || SqlQueryAliasChecker.checkOwnerOfColumn(r.getTable().getAlias(), this._rightRelations);

      if (!rv) {
        throw SqlExceptions.Exception(SqlQueryAliasChecker.RESOLVE_TABLE_ALIAS_EXCEPTION_CODE,
            SqlExceptions.NO_TABLE_REFERENCE_INCLUDE_IN_FROM, r.getTable().getAlias());
      }
    }
  }

  public static void checkOnlyTablePart(SqlTable table) throws RSBException {
    if (table.isNestedQueryTable()) {
      SqlQueryStatement nq = table.getQuery();
      try {
        checkTableReference(nq);
      } catch (Exception ex) {
        throw new RSBException("SqlTableRelationChecker", Utilities.getExceptionMessage(ex));
      }
    } else if (table.isNestedJoinTable()) {
      SqlTable nj = table.getNestedJoin();
      try {
        SqlCollection<SqlColumn> TEMP_COLUMNS = new SqlCollection<SqlColumn>();
        TEMP_COLUMNS.add(new SqlWildcardColumn());
        SqlSelectStatement TEMP_NJ_QUERY = new SqlSelectStatement(TEMP_COLUMNS,
            null,
            null,
            new SqlCollection<SqlOrderSpec>(),
            new SqlCollection<SqlExpression>(),
            false,
            nj,
            new SqlCollection<SqlValueExpression>(),
            false,
            null,
            null,
            false,
            null);
        checkTableReference(TEMP_NJ_QUERY);
      } catch (Exception ex) {
        throw new RSBException("SqlTableRelationChecker", Utilities.getExceptionMessage(ex));
      }
    }
  }

  private static void checkTableReference(SqlQueryStatement queryStatement) throws Exception {
    SqlCollection<SqlTable> references = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(references, new TableAliasMatcher());
    queryStatement.accept(collector);
    queryStatement.accept(new SqlQueryAliasChecker(references));
  }
}
