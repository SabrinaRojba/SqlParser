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


public sealed class SqlCrossApply : ISqlCloneable {
  private readonly SqlFormulaColumn tableValuedFunction;
  private readonly string alias;
  private readonly SqlCollection<SqlColumnDefinition> columns;
  private readonly SqlCrossApply crossApply;

  public SqlCrossApply(SqlFormulaColumn tableValuedFunction, string alias, SqlCollection<SqlColumnDefinition> columns, SqlCrossApply crossApply) {
    this.tableValuedFunction = tableValuedFunction;
    this.alias = alias;
    this.columns = columns;
    this.crossApply = crossApply;
  }

  public SqlFormulaColumn GetTableValuedFunction() { return this.tableValuedFunction; }
  public string GetAlias() {
    return this.alias;
  }
  public SqlCrossApply GetCrossApply() { return this.crossApply; }

  public SqlCollection<SqlColumnDefinition> GetColumnDefinitions() {
    return this.columns;
  }

  public SqlColumnDefinition GetColumnDef(string name) {
    foreach(SqlColumnDefinition def in columns) {
      if (Utilities.EqualIgnoreCase(name, def.ColumnName)) {
        return def;
      }
    }
    return null;
  }

  public SqlColumnDefinition GetColumnDef(SqlColumn col) {
    SqlTable table = col.GetTable();
    string tableAlias = "";
    if ( table != null ) {
      tableAlias = table.GetAlias();
    }
    SqlColumnDefinition def = null;
    if (!Utilities.IsNullOrEmpty(tableAlias)) {
      // if the column is using a table alias, and
      // it doesn't match our alias, we're done
      if ( Utilities.EqualIgnoreCase(tableAlias, this.alias) ) {
        def = this.GetColumnDef(col.GetColumnName());
      }
    } else {
      // no alias
      def = this.GetColumnDef(col.GetColumnName());
    }
    return def;
  }


  public bool DeclaresColumn(SqlColumn col, bool deep) {
    SqlColumnDefinition def = GetColumnDef(col);
    if ( def == null && deep && this.crossApply != null) {
      return this.crossApply.DeclaresColumn(col, deep);
    }
    return def != null;
  }

  public SqlCrossApply AddCrossApply(SqlCrossApply apply) {
    return new SqlCrossApply(this.tableValuedFunction, this.alias, this.columns, apply);
  }

  public Object Clone() {
    return new SqlCrossApply(this.tableValuedFunction, this.alias, this.columns, this.crossApply);
  }

  public SqlTable GetPseudoTable() {
    return SqlTable.CreatePseudoTable(this.alias);
  }
}
}

