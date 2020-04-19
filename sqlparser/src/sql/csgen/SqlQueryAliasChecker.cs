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


sealed class SqlQueryAliasChecker : ISqlQueryVisitor {
  public const string RESOLVE_TABLE_ALIAS_EXCEPTION_CODE = "SqlQueryChecker";
  private readonly SqlCollection<SqlTable> _flattenTables;

  public SqlQueryAliasChecker(SqlCollection<SqlTable> tableReferences) {
    this._flattenTables = tableReferences;
  }

  public void Visit(SqlTable table) {

    SqlCollection<SqlTable> leftRelations = new SqlCollection<SqlTable>();
    TableFinder left_collector = new TableFinder(leftRelations, new TableOnlyMatcher());

    //check only table part
    SqlTableRelationChecker.CheckOnlyTablePart(table);
    try {
      table.Accept(left_collector);
    } catch (Exception ex) {
      throw new RSBException("SqlQueryChecker", Utilities.GetExceptionMessage(ex));
    }

    //check joins.
    while (table.HasJoin()) {
      SqlJoin j = table.GetJoin();
      SqlCollection<SqlTable> rightRelations = new SqlCollection<SqlTable>();
      TableFinder right_collector = new TableFinder(rightRelations, new TableOnlyMatcher());
      try {
        j.GetTable().Accept(right_collector);
      } catch (Exception ex) {
        throw new RSBException("SqlQueryChecker", Utilities.GetExceptionMessage(ex));
      }
      if (j.GetCondition() != null) {
        SqlTableRelationChecker onChecker = new SqlTableRelationChecker(leftRelations, rightRelations);
        try {
          j.GetCondition().Accept(onChecker);
        } catch (Exception ex) {
          throw new RSBException("SqlQueryChecker", Utilities.GetExceptionMessage(ex));
        }
      }
      table = j.GetTable();
      SqlTableRelationChecker.CheckOnlyTablePart(table);
      leftRelations.AddAll(rightRelations);
    }
  }

  public void Visit(SqlStatement stmt) {
  }

  public void Visit(SqlColumn column) {
    if (null == column.GetTable()) return;

    bool valid = CheckOwnerOfColumn(column.GetTable().GetAlias(), this._flattenTables);
    if (!valid) {
      throw SqlExceptions.Exception(RESOLVE_TABLE_ALIAS_EXCEPTION_CODE, SqlExceptions.NO_TABLE_REFERENCE_INCLUDE_IN_FROM, column.GetTable().GetAlias());
    }
  }

  public void Visit(SqlConditionNode criteria) {
    if (criteria is SqlCriteria) {
      SqlCriteria c = (SqlCriteria) criteria;
      if (c.GetLeft() is SqlColumn) {
        Visit((SqlColumn)c.GetLeft());
      }
      if (c.GetRight() is SqlColumn) {
        Visit((SqlColumn)c.GetRight());
      }
    }
  }

  public static bool CheckOwnerOfColumn(string owner, SqlCollection<SqlTable> flatten) {
    bool valid = false;
    foreach(SqlTable t in flatten) {
      if (Utilities.EqualIgnoreCaseInvariant(owner, t.GetAlias())) {
        valid = true;
        break;
      }
      if (Utilities.EqualIgnoreCaseInvariant(owner, t.GetName())) {
        valid = true;
        break;
      }
    }
    return valid;
  }
}

sealed class SqlTableRelationChecker : ISqlQueryVisitor {
  private readonly SqlCollection<SqlTable> _leftRelations;
  private readonly SqlCollection<SqlTable> _rightRelations;
  public SqlTableRelationChecker(SqlCollection<SqlTable> left, SqlCollection<SqlTable> right) {
    this._leftRelations = left;
    this._rightRelations = right;
  }

  public void Visit(SqlTable table) {
  }

  public void Visit(SqlStatement stmt) {

  }

  public void Visit(SqlColumn column) {

  }

  public void Visit(SqlConditionNode criteria) {
    if (criteria is SqlCriteria) {
      SqlCriteria c = (SqlCriteria) criteria;
      if (!(c.GetLeft() is SqlColumn )) {
        return;
      }

      if (!(c.GetRight() is SqlColumn )) {
        return;
      }

      SqlColumn l = (SqlColumn) c.GetLeft();
      SqlColumn r = (SqlColumn) c.GetRight();

      bool lv = null == l.GetTable()? true : SqlQueryAliasChecker.CheckOwnerOfColumn(l.GetTable().GetAlias(), this._leftRelations) || SqlQueryAliasChecker.CheckOwnerOfColumn(l.GetTable().GetAlias(), this._rightRelations);

      if (!lv) {
        throw SqlExceptions.Exception(SqlQueryAliasChecker.RESOLVE_TABLE_ALIAS_EXCEPTION_CODE,
            SqlExceptions.NO_TABLE_REFERENCE_INCLUDE_IN_FROM, l.GetTable().GetAlias());
      }

      bool rv = null == r.GetTable()? true : SqlQueryAliasChecker.CheckOwnerOfColumn(r.GetTable().GetAlias(), this._leftRelations) || SqlQueryAliasChecker.CheckOwnerOfColumn(r.GetTable().GetAlias(), this._rightRelations);

      if (!rv) {
        throw SqlExceptions.Exception(SqlQueryAliasChecker.RESOLVE_TABLE_ALIAS_EXCEPTION_CODE,
            SqlExceptions.NO_TABLE_REFERENCE_INCLUDE_IN_FROM, r.GetTable().GetAlias());
      }
    }
  }

  public static void CheckOnlyTablePart(SqlTable table) {
    if (table.IsNestedQueryTable()) {
      SqlQueryStatement nq = table.GetQuery();
      try {
        CheckTableReference(nq);
      } catch (Exception ex) {
        throw new RSBException("SqlTableRelationChecker", Utilities.GetExceptionMessage(ex));
      }
    } else if (table.IsNestedJoinTable()) {
      SqlTable nj = table.GetNestedJoin();
      try {
        SqlCollection<SqlColumn> TEMP_COLUMNS = new SqlCollection<SqlColumn>();
        TEMP_COLUMNS.Add(new SqlWildcardColumn());
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
        CheckTableReference(TEMP_NJ_QUERY);
      } catch (Exception ex) {
        throw new RSBException("SqlTableRelationChecker", Utilities.GetExceptionMessage(ex));
      }
    }
  }

  private static void CheckTableReference(SqlQueryStatement queryStatement) {
    SqlCollection<SqlTable> references = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(references, new TableAliasMatcher());
    queryStatement.Accept(collector);
    queryStatement.Accept(new SqlQueryAliasChecker(references));
  }
}
}

