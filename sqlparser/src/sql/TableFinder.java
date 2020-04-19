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

final class TableFinder implements ISqlQueryVisitor {
  private final SqlCollection<SqlTable> _collector;
  private final ITableMatch _matcher;
  public TableFinder(SqlCollection<SqlTable> collector, ITableMatch matcher) {
    this._collector = collector;
    this._matcher = matcher;
  }

  public void visit(SqlTable table) throws RSBException {
    if (table.isNestedQueryTable()) {
      if (this._matcher.accept(table, TablePartType.NestedQuery)) {
        this._collector.add(this._matcher.create(table));
      }

      if (this._matcher.unwind(table, TablePartType.NestedQuery)) {
        SqlQueryStatement nq = table.getQuery();
        SqlCollection<SqlTable> NQ_REFERENCES = new SqlCollection<SqlTable>();
        TableFinder NQ_COLLECTOR = new TableFinder(NQ_REFERENCES, this._matcher);
        try {
          nq.accept(NQ_COLLECTOR);
        } catch (Exception ex) {
          throw new RSBException("TableCollector", Utilities.getExceptionMessage(ex));
        }
        this._collector.addAll(NQ_REFERENCES);
      }
    } else if (table.isNestedJoinTable()) {
      if (this._matcher.accept(table, TablePartType.NestedJoin)) {
        this._collector.add(this._matcher.create(table));
      }

      if (this._matcher.unwind(table, TablePartType.NestedJoin)) {
        SqlTable nj = table.getNestedJoin();
        SqlCollection<SqlTable> NJ_REFERENCES = new SqlCollection<SqlTable>();
        TableFinder NJ_COLLECTOR = new TableFinder(NJ_REFERENCES, this._matcher);
        try {
          nj.accept(NJ_COLLECTOR);
        } catch (Exception ex) {
          throw new RSBException("TableCollector", Utilities.getExceptionMessage(ex));
        }
        this._collector.addAll(NJ_REFERENCES);
      }
    } else {
      if (this._matcher.accept(table, TablePartType.SimpleTable)) {
        this._collector.add(this._matcher.create(table));
      }
      // THIS IS A HACK so that SqlQueryAliasChecker resolves
      // table aliases in the query correctly to columns that are tied
      // to our cross apply statements.
      SqlCrossApply ca = table.getCrossApply();
      while ( ca != null ) {
        SqlTable pseudoTable = ca.getPseudoTable();
        if (this._matcher.accept(pseudoTable, TablePartType.SimpleTable)) {
          this._collector.add(this._matcher.create(pseudoTable));
        }
        ca = ca.getCrossApply();
      }
    }

    if (table.hasJoin()) {
      SqlJoin j = table.getJoin();
      SqlTable right = j.getTable();
      this.visit(right);
    }
  }

  public void visit(SqlStatement stmt) throws RSBException {
    //do nothing.
  }

  public void visit(SqlColumn column) throws RSBException {
    //do nothing.
  }

  public void visit(SqlConditionNode criteria) throws RSBException {
    if (criteria instanceof SqlConditionExists) {
      SqlQueryStatement sq = ((SqlConditionExists) criteria).getSubQuery();
      try {
        sq.accept(this);
      } catch (Exception ex) {
        throw new RSBException("TableCollector", Utilities.getExceptionMessage(ex));
      }
    } else if (criteria instanceof SqlConditionInSelect) {
      SqlQueryStatement sq = ((SqlConditionInSelect) criteria).getRightQuery();
      try {
        sq.accept(this);
      } catch (Exception ex) {
        throw new RSBException("TableCollector", Utilities.getExceptionMessage(ex));
      }
    }
  }
}
