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

sealed class RemoveDistinctVisitor : ISqlQueryVisitor {
  private IDataMetadata _dataMetaData;

  public RemoveDistinctVisitor(IDataMetadata dataMetadata) {
    this._dataMetaData = dataMetadata;
  }

  public void Visit(SqlTable table) {
    if (table.IsNestedQueryTable()) {
      SqlQueryStatement nq = table.GetQuery();
      try {
        nq.Accept(new RemoveDistinctVisitor(this._dataMetaData));
      } catch (Exception ex){;}
    }

    if (table.IsNestedJoinTable()) {
      SqlTable nj = table.GetNestedJoin();
      this.Visit(nj);
    }

    if (table.HasJoin()) {
      SqlJoin join = table.GetJoin();
      this.Visit(join.GetTable());
    }
  }

  public void Visit(SqlStatement stmt) {
    if (!(stmt is SqlSelectStatement)) {
      return;
    }

    SqlSelectStatement query = (SqlSelectStatement) stmt;

    if (!query.IsDistinct() && 0 == query.GetGroupBy().Size()) {
      return;
    }

    SqlCollection<SqlColumn> NoUniqueColumns = new SqlCollection<SqlColumn>();
    SqlTable table = stmt.GetTable();

    if (!table.IsSourceTable()) return;

    ColumnInfo[] columns = new ColumnInfo[0];
    try {
      columns = this._dataMetaData.GetTableMetadata(table.GetCatalog(), table.GetSchema(), table.GetName());
    } catch (Exception ex) {
      ;
    }

    ISqlElementComparable<SqlColumn> uniqueChecker = new UniqueColumnCheck<SqlColumn>(columns);
    SqlUtilities.FetchElement(stmt.GetColumns(), NoUniqueColumns, uniqueChecker);

    if (NoUniqueColumns.Size() == 0) {
      query.SetDistinct(false);
    }

    SqlCollection<SqlColumn> input = new SqlCollection<SqlColumn>();
    SqlCollection<SqlExpression> groupBy = new SqlCollection<SqlExpression>();

    for (int i = 0 ; i < query.GetGroupBy().Size(); ++i) {
      input.Clear();
      NoUniqueColumns.Clear();

      SqlExpression expr = query.GetGroupBy().Get(i);
      if (expr is SqlColumn) {
        input.Add((SqlColumn) expr);
      }

      if (0 == input.Size()) {
        groupBy.Add(expr);
        continue;
      }

      SqlUtilities.FetchElement(input, NoUniqueColumns, uniqueChecker);
      if (NoUniqueColumns.Size() > 0) {
        groupBy.Add(expr);
      }
    }

    if (groupBy.Size() != query.GetGroupBy().Size()) {
      query.SetGroupByClause(groupBy, query.GetEachGroupBy());
    }
  }

  public void Visit(SqlColumn column) {

  }

  public void Visit(SqlConditionNode criteria) {
    SqlQueryStatement nq = null;
    if (criteria is SqlConditionExists) {
      SqlConditionExists exists = (SqlConditionExists) criteria;
      nq = exists.GetSubQuery();
    }

    if (criteria is SqlConditionInSelect) {
      SqlConditionInSelect inSelect = (SqlConditionInSelect) criteria;
      nq = inSelect.GetRightQuery();
    }

    if (nq == null) {
      return;
    }

    try {
      nq.Accept(new RemoveDistinctVisitor(this._dataMetaData));
    } catch (Exception ex){;}
  }

  internal sealed class UniqueColumnCheck<T>  : ISqlElementComparable<T> where T : SqlColumn {
    private readonly ColumnInfo[] _columnsMeta;

    public UniqueColumnCheck(ColumnInfo[] columnsMeta) {
      this._columnsMeta = columnsMeta;
    }

    public int CompareTo(T o) {
      if (!(o is SqlGeneralColumn)) {
        return 0;
      }

      ColumnInfo columnInfo = this.Find(o.GetColumnName());

      if (null == columnInfo) {
        return 0;
      }

      if (!columnInfo.GetIsKey()) {
        return 0;
      }

      return -1;
    }

    private ColumnInfo Find(string name) {
      foreach(ColumnInfo c in this._columnsMeta) {
        if (Utilities.EqualIgnoreCase(name, c.GetColumnName())) {
          return c;
        }
      }
      return null;
    }
  }
}
}

