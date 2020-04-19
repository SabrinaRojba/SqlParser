package core;
/*#
using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Collections.Generic;
#*/
import cdata.sql.*;
import rssbus.RSBException;
import rssbus.oputils.common.Utilities;

final class RemoveDistinctVisitor implements ISqlQueryVisitor {
  private IDataMetadata _dataMetaData;

  public RemoveDistinctVisitor(IDataMetadata dataMetadata) {
    this._dataMetaData = dataMetadata;
  }

  public void visit(SqlTable table) throws RSBException {
    if (table.isNestedQueryTable()) {
      SqlQueryStatement nq = table.getQuery();
      try {
        nq.accept(new RemoveDistinctVisitor(this._dataMetaData));
      } catch (Exception ex){;}
    }

    if (table.isNestedJoinTable()) {
      SqlTable nj = table.getNestedJoin();
      this.visit(nj);
    }

    if (table.hasJoin()) {
      SqlJoin join = table.getJoin();
      this.visit(join.getTable());
    }
  }

  public void visit(SqlStatement stmt) throws RSBException {
    if (!(stmt instanceof SqlSelectStatement)) {
      return;
    }

    SqlSelectStatement query = (SqlSelectStatement) stmt;

    if (!query.isDistinct() && 0 == query.getGroupBy().size()) {
      return;
    }

    SqlCollection<SqlColumn> NoUniqueColumns = new SqlCollection<SqlColumn>();
    SqlTable table = stmt.getTable();

    if (!table.isSourceTable()) return;

    ColumnInfo[] columns = new ColumnInfo[0];
    try {
      columns = this._dataMetaData.getTableMetadata(table.getCatalog(), table.getSchema(), table.getName());
    } catch (Exception ex) {
      ;
    }

    ISqlElementComparable<SqlColumn> uniqueChecker = new UniqueColumnCheck<SqlColumn>(columns);
    SqlUtilities.fetchElement(stmt.getColumns(), NoUniqueColumns, uniqueChecker);

    if (NoUniqueColumns.size() == 0) {
      query.setDistinct(false);
    }

    SqlCollection<SqlColumn> input = new SqlCollection<SqlColumn>();
    SqlCollection<SqlExpression> groupBy = new SqlCollection<SqlExpression>();

    for (int i = 0 ; i < query.getGroupBy().size(); ++i) {
      input.clear();
      NoUniqueColumns.clear();

      SqlExpression expr = query.getGroupBy().get(i);
      if (expr instanceof SqlColumn) {
        input.add((SqlColumn) expr);
      }

      if (0 == input.size()) {
        groupBy.add(expr);
        continue;
      }

      SqlUtilities.fetchElement(input, NoUniqueColumns, uniqueChecker);
      if (NoUniqueColumns.size() > 0) {
        groupBy.add(expr);
      }
    }

    if (groupBy.size() != query.getGroupBy().size()) {
      query.setGroupByClause(groupBy, query.getEachGroupBy());
    }
  }

  public void visit(SqlColumn column) throws RSBException {

  }

  public void visit(SqlConditionNode criteria) throws RSBException {
    SqlQueryStatement nq = null;
    if (criteria instanceof SqlConditionExists) {
      SqlConditionExists exists = (SqlConditionExists) criteria;
      nq = exists.getSubQuery();
    }

    if (criteria instanceof SqlConditionInSelect) {
      SqlConditionInSelect inSelect = (SqlConditionInSelect) criteria;
      nq = inSelect.getRightQuery();
    }

    if (nq == null) {
      return;
    }

    try {
      nq.accept(new RemoveDistinctVisitor(this._dataMetaData));
    } catch (Exception ex){;}
  }

  final class UniqueColumnCheck/*#<T>#*/ /*@*/<T extends SqlColumn> implements/*@*/ /*#:#*/ ISqlElementComparable<T> /*#where T : SqlColumn#*/ {
    private final ColumnInfo[] _columnsMeta;

    public UniqueColumnCheck(ColumnInfo[] columnsMeta) {
      this._columnsMeta = columnsMeta;
    }

    public int compareTo(T o) {
      if (!(o instanceof SqlGeneralColumn)) {
        return 0;
      }

      ColumnInfo columnInfo = this.find(o.getColumnName());

      if (null == columnInfo) {
        return 0;
      }

      if (!columnInfo.getIsKey()) {
        return 0;
      }

      return -1;
    }

    private ColumnInfo find(String name) {
      for (ColumnInfo c : this._columnsMeta) {
        if (Utilities.equalIgnoreCase(name, c.getColumnName())) {
          return c;
        }
      }
      return null;
    }
  }
}

