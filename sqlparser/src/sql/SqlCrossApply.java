//@
package cdata.sql;
//@

import rssbus.oputils.common.Utilities;

import java.util.Hashtable;

/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlCrossApply implements ISqlCloneable {
  private final SqlFormulaColumn tableValuedFunction;
  private final String alias;
  private final SqlCollection<SqlColumnDefinition> columns;
  private final SqlCrossApply crossApply;

  public SqlCrossApply(SqlFormulaColumn tableValuedFunction, String alias, SqlCollection<SqlColumnDefinition> columns, SqlCrossApply crossApply) {
    this.tableValuedFunction = tableValuedFunction;
    this.alias = alias;
    this.columns = columns;
    this.crossApply = crossApply;
  }

  public SqlFormulaColumn getTableValuedFunction() { return this.tableValuedFunction; }
  public String getAlias() {
    return this.alias;
  }
  public SqlCrossApply getCrossApply() { return this.crossApply; }

  public SqlCollection<SqlColumnDefinition> getColumnDefinitions() {
    return this.columns;
  }

  public SqlColumnDefinition getColumnDef(String name) {
    for (SqlColumnDefinition def : columns) {
      if (Utilities.equalIgnoreCase(name, def.ColumnName)) {
        return def;
      }
    }
    return null;
  }

  public SqlColumnDefinition getColumnDef(SqlColumn col) {
    SqlTable table = col.getTable();
    String tableAlias = "";
    if ( table != null ) {
      tableAlias = table.getAlias();
    }
    SqlColumnDefinition def = null;
    if (!Utilities.isNullOrEmpty(tableAlias)) {
      // if the column is using a table alias, and
      // it doesn't match our alias, we're done
      if ( Utilities.equalIgnoreCase(tableAlias, this.alias) ) {
        def = this.getColumnDef(col.getColumnName());
      }
    } else {
      // no alias
      def = this.getColumnDef(col.getColumnName());
    }
    return def;
  }


  public boolean declaresColumn(SqlColumn col, boolean deep) {
    SqlColumnDefinition def = getColumnDef(col);
    if ( def == null && deep && this.crossApply != null) {
      return this.crossApply.declaresColumn(col, deep);
    }
    return def != null;
  }

  public SqlCrossApply addCrossApply(SqlCrossApply apply) {
    return new SqlCrossApply(this.tableValuedFunction, this.alias, this.columns, apply);
  }

  public Object clone() {
    return new SqlCrossApply(this.tableValuedFunction, this.alias, this.columns, this.crossApply);
  }

  public SqlTable getPseudoTable() {
    return SqlTable.createPseudoTable(this.alias);
  }
}
