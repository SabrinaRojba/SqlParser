//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import core.ParserCore;
import rssbus.oputils.common.Utilities;

public final class SqlTable implements ISqlElement, ISqlCloneable {
  private final static String PSEUDO_TABLE_INDICATOR = "____";
  private final String catalog, schema, name, alias;
  private final SqlJoin join;
  private final SqlTable nestedJoin;
  private final SqlQueryStatement query;
  private final SqlFormulaColumn tableValueFunction;
  private final SqlCrossApply crossApply;
  private SqlTableMeta metaData;

  public SqlTable(SqlFormulaColumn valueFunction) {
    this(null, null, valueFunction.getColumnName(), valueFunction.getColumnName(), null, null, null, valueFunction, null);
  }
  
  public SqlTable(String name) {
    this(null, null, name, name, null, null, null, null, null);
  }

  public SqlTable(String name, String alias) {
    this(null, null, name, alias, null, null, null, null, null);
  }

  public SqlTable(String catalog, String schema, String name) {
    this(catalog, schema, name, name, null, null, null, null, null);
  }

  public SqlTable(String catalog, String schema, String name, String alias) {
    this(catalog, schema, name, alias, null, null, null, null, null);
  }

  public SqlTable(SqlQueryStatement query, String name, String alias) {
    this(null, null, name, alias, null, null, query, null, null);
  }

  public SqlTable(SqlQueryStatement query, String alias) {
    this(null, null, ParserCore.DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX, alias, null, null, query, null, null);
  }

  public SqlTable(String catalog, String schema, String name, String alias, SqlJoin join) {
    this(catalog, schema, name, alias, join, null, null, null, null);
  }

  public SqlTable(String alias, SqlTable nestedJoin) {
    this(null, null, ParserCore.DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX, alias, null, nestedJoin, null, null, null);
  }

  public SqlTable(String catalog, String schema, String name, String alias, SqlTable nestedJoin) {
    this(catalog, schema, name, alias, null, nestedJoin, null, null, null);
  }

  public SqlTable(String catalog, String schema, String name, String alias, SqlCrossApply crossApply) {
    this(catalog, schema, name, alias, null, null, null, null, crossApply);
  }
  public SqlTable(String catalog, String schema, String name, String alias, SqlJoin join, SqlTable nestedJoin, SqlQueryStatement query, SqlFormulaColumn valueFun) {
    this(catalog, schema, name, alias, join, nestedJoin, query, valueFun, null);
  }
  public SqlTable(String catalog, String schema, String name, String alias, SqlJoin join, SqlTable nestedJoin, SqlQueryStatement query, SqlFormulaColumn valueFun, SqlCrossApply crossApply) {
    this.catalog = catalog;
    this.schema = schema;
    this.name = name;
    this.alias = alias;
    this.join = join;
    this.nestedJoin = nestedJoin;
    this.query = query;
    this.tableValueFunction = valueFun;
    this.crossApply = crossApply;
  }

  // This is a hack used by Cross Apply to signal this is a table
  // that's not real (embedded expression), causing ExecModel
  // to "ignore" validating the source table the table when
  // deciding to push columns
  public static SqlTable createPseudoTable(String alias) {
    // We really should be used something that is unique here.
    return new SqlTable(PSEUDO_TABLE_INDICATOR, alias);
  }

  public Object clone() {
    SqlTable obj = new SqlTable(this.catalog,
        this.schema,
        this.name,
        this.alias,
        this.join != null? (SqlJoin) this.join.clone() : null,
        this.nestedJoin != null ? (SqlTable) this.nestedJoin.clone() : null,
        this.query != null ? (SqlQueryStatement) this.query.clone() : null,
        this.tableValueFunction,
        this.crossApply);
    obj.metaData = metaData;
    return obj;
  }

  public String getCatalog() {
    return this.catalog;
  }

  public String getSchema() {
    return this.schema;
  }

  public String getName() {
    return this.name;
  }

  public String getAlias() {
    return this.alias;
  }

  public SqlJoin getJoin() {
    return this.join;
  }

  public SqlTable getNestedJoin() {
    return nestedJoin;
  }

  public SqlQueryStatement getQuery() {
    return query;
  }

  public SqlFormulaColumn getTableValueFunction() {
    return this.tableValueFunction;
  }

  public String getFullName() {
    return getFullName('\0');
  }

  public String getFullName(char quotes) {
    return getFullName(quotes, quotes);
  }

  public String getFullName(char openQuote, char closeQuote) {
    StringBuffer fullName = new StringBuffer();
    if (catalog != null) {
      if (openQuote != '\0') fullName.append(openQuote);
      fullName.append(catalog);
      if (closeQuote != '\0') fullName.append(closeQuote);
    }
    if (schema != null) {
      if (fullName.length() > 0) {
        fullName.append(".");
      }
      if (openQuote != '\0') fullName.append(openQuote);
      fullName.append(schema);
      if (closeQuote != '\0') fullName.append(closeQuote);
    }
    if (name != null) {
      if (fullName.length() > 0) {
        fullName.append(".");
      }
      if (openQuote != '\0') fullName.append(openQuote);
      fullName.append(name);
      if (closeQuote != '\0') fullName.append(closeQuote);
    }
    return fullName.toString();
  }

  public String getValidName() {
    // For NESTED_QUERY table, we must use the alias name.
    String name = this.getName();
    if (!ParserCore.isValidTableName(name)) {
      if (this.alias != null) {
        name = this.alias;
      }
    }

    return name;
  }

  public SqlTableMeta getMetaData() {
    return metaData;
  }

  public SqlTable resolve() {
    if (this.nestedJoin != null) {
      return this.nestedJoin.resolve();
    } else if (this.query != null) {
      SqlTable queryTable = this.query.getTable();
      if (queryTable != null) {
        return queryTable.resolve();
      }
    }

    return this;
  }

  public String getResolveNestedTableExprName() {
    if (this.nestedJoin != null) {
      return this.nestedJoin.getResolveNestedTableExprName();
    } else {
      return getName();
    }
  }

  public String getResolveNestedTableExprAlias() {
    //(A AS A1 JOIN B), (A A1 JOIN B), return A1
    if (this.nestedJoin != null) {
      if (Utilities.equalIgnoreCase(getName(), getAlias())) {
        return this.nestedJoin.getResolveNestedTableExprAlias();
      }
    }
    return getAlias();
  }

  public SqlCrossApply getCrossApply() { return this.crossApply; }

  public void accept(ISqlQueryVisitor visitor) throws Exception {
    visitor.visit(this);
  }

  public boolean isNestedQueryTable() {
    return this.query != null;
  }

  public boolean isNestedJoinTable() {
    return this.nestedJoin != null;
  }

  public boolean isFunctionValueTable() {
    return this.tableValueFunction != null;
  }

  public boolean isSourceTable() {
    return !isNestedQueryTable() && !isNestedJoinTable() && !hasJoin();
  }

  public boolean isPseudoTable() { return Utilities.equalIgnoreCaseInvariant(this.name, PSEUDO_TABLE_INDICATOR); }

  public boolean hasJoin() {
    return this.getJoin() != null;
  }

  public boolean hasCrossApply() { return this.crossApply != null; }

  public boolean hasAlias() {
    boolean hasAlias = this.getName() == null && this.getAlias() != null;
    hasAlias = hasAlias || (this.getName() != null && this.getAlias() != null && !Utilities.equalIgnoreCaseInvariant(this.getName(), this.getAlias()));
    return hasAlias;
  }
}
