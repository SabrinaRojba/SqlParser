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


sealed class TableFinder : ISqlQueryVisitor {
  private readonly SqlCollection<SqlTable> _collector;
  private readonly ITableMatch _matcher;
  public TableFinder(SqlCollection<SqlTable> collector, ITableMatch matcher) {
    this._collector = collector;
    this._matcher = matcher;
  }

  public void Visit(SqlTable table) {
    if (table.IsNestedQueryTable()) {
      if (this._matcher.Accept(table, TablePartType.NestedQuery)) {
        this._collector.Add(this._matcher.Create(table));
      }

      if (this._matcher.Unwind(table, TablePartType.NestedQuery)) {
        SqlQueryStatement nq = table.GetQuery();
        SqlCollection<SqlTable> NQ_REFERENCES = new SqlCollection<SqlTable>();
        TableFinder NQ_COLLECTOR = new TableFinder(NQ_REFERENCES, this._matcher);
        try {
          nq.Accept(NQ_COLLECTOR);
        } catch (Exception ex) {
          throw new RSBException("TableCollector", Utilities.GetExceptionMessage(ex));
        }
        this._collector.AddAll(NQ_REFERENCES);
      }
    } else if (table.IsNestedJoinTable()) {
      if (this._matcher.Accept(table, TablePartType.NestedJoin)) {
        this._collector.Add(this._matcher.Create(table));
      }

      if (this._matcher.Unwind(table, TablePartType.NestedJoin)) {
        SqlTable nj = table.GetNestedJoin();
        SqlCollection<SqlTable> NJ_REFERENCES = new SqlCollection<SqlTable>();
        TableFinder NJ_COLLECTOR = new TableFinder(NJ_REFERENCES, this._matcher);
        try {
          nj.Accept(NJ_COLLECTOR);
        } catch (Exception ex) {
          throw new RSBException("TableCollector", Utilities.GetExceptionMessage(ex));
        }
        this._collector.AddAll(NJ_REFERENCES);
      }
    } else {
      if (this._matcher.Accept(table, TablePartType.SimpleTable)) {
        this._collector.Add(this._matcher.Create(table));
      }
      // THIS IS A HACK so that SqlQueryAliasChecker resolves
      // table aliases in the query correctly to columns that are tied
      // to our cross apply statements.
      SqlCrossApply ca = table.GetCrossApply();
      while ( ca != null ) {
        SqlTable pseudoTable = ca.GetPseudoTable();
        if (this._matcher.Accept(pseudoTable, TablePartType.SimpleTable)) {
          this._collector.Add(this._matcher.Create(pseudoTable));
        }
        ca = ca.GetCrossApply();
      }
    }

    if (table.HasJoin()) {
      SqlJoin j = table.GetJoin();
      SqlTable right = j.GetTable();
      this.Visit(right);
    }
  }

  public void Visit(SqlStatement stmt) {
    //do nothing.
  }

  public void Visit(SqlColumn column) {
    //do nothing.
  }

  public void Visit(SqlConditionNode criteria) {
    if (criteria is SqlConditionExists) {
      SqlQueryStatement sq = ((SqlConditionExists) criteria).GetSubQuery();
      try {
        sq.Accept(this);
      } catch (Exception ex) {
        throw new RSBException("TableCollector", Utilities.GetExceptionMessage(ex));
      }
    } else if (criteria is SqlConditionInSelect) {
      SqlQueryStatement sq = ((SqlConditionInSelect) criteria).GetRightQuery();
      try {
        sq.Accept(this);
      } catch (Exception ex) {
        throw new RSBException("TableCollector", Utilities.GetExceptionMessage(ex));
      }
    }
  }
}
}

