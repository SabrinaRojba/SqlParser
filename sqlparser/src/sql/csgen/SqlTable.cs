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


public sealed class SqlTable : ISqlElement, ISqlCloneable {
  private readonly static string PSEUDO_TABLE_INDICATOR = "____";
  private readonly string catalog, schema, name, alias;
  private readonly SqlJoin join;
  private readonly SqlTable nestedJoin;
  private readonly SqlQueryStatement query;
  private readonly SqlFormulaColumn tableValueFunction;
  private readonly SqlCrossApply crossApply;
  private SqlTableMeta metaData;

  public SqlTable(SqlFormulaColumn valueFunction) : this(null, null, valueFunction.GetColumnName(), valueFunction.GetColumnName(), null, null, null, valueFunction, null) {
  }
  
  public SqlTable(string name) : this(null, null, name, name, null, null, null, null, null) {
  }

  public SqlTable(string name, string alias) : this(null, null, name, alias, null, null, null, null, null) {
  }

  public SqlTable(string catalog, string schema, string name) : this(catalog, schema, name, name, null, null, null, null, null) {
  }

  public SqlTable(string catalog, string schema, string name, string alias) : this(catalog, schema, name, alias, null, null, null, null, null) {
  }

  public SqlTable(SqlQueryStatement query, string name, string alias) : this(null, null, name, alias, null, null, query, null, null) {
  }

  public SqlTable(SqlQueryStatement query, string alias) : this(null, null, ParserCore.DERIVED_NESTED_QUERY_TABLE_NAME_PREFIX, alias, null, null, query, null, null) {
  }

  public SqlTable(string catalog, string schema, string name, string alias, SqlJoin join) : this(catalog, schema, name, alias, join, null, null, null, null) {
  }

  public SqlTable(string alias, SqlTable nestedJoin) : this(null, null, ParserCore.DERIVED_NESTED_JOIN_TABLE_NAME_PREFIX, alias, null, nestedJoin, null, null, null) {
  }

  public SqlTable(string catalog, string schema, string name, string alias, SqlTable nestedJoin) : this(catalog, schema, name, alias, null, nestedJoin, null, null, null) {
  }

  public SqlTable(string catalog, string schema, string name, string alias, SqlCrossApply crossApply) : this(catalog, schema, name, alias, null, null, null, null, crossApply) {
  }
  public SqlTable(string catalog, string schema, string name, string alias, SqlJoin join, SqlTable nestedJoin, SqlQueryStatement query, SqlFormulaColumn valueFun) : this(catalog, schema, name, alias, join, nestedJoin, query, valueFun, null) {
  }
  public SqlTable(string catalog, string schema, string name, string alias, SqlJoin join, SqlTable nestedJoin, SqlQueryStatement query, SqlFormulaColumn valueFun, SqlCrossApply crossApply) {
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
  public static SqlTable CreatePseudoTable(string alias) {
    // We really should be used something that is unique here.
    return new SqlTable(PSEUDO_TABLE_INDICATOR, alias);
  }

  public Object Clone() {
    SqlTable obj = new SqlTable(this.catalog,
        this.schema,
        this.name,
        this.alias,
        this.join != null? (SqlJoin) this.join.Clone() : null,
        this.nestedJoin != null ? (SqlTable) this.nestedJoin.Clone() : null,
        this.query != null ? (SqlQueryStatement) this.query.Clone() : null,
        this.tableValueFunction,
        this.crossApply);
    obj.metaData = metaData;
    return obj;
  }

  public string GetCatalog() {
    return this.catalog;
  }

  public string GetSchema() {
    return this.schema;
  }

  public string GetName() {
    return this.name;
  }

  public string GetAlias() {
    return this.alias;
  }

  public SqlJoin GetJoin() {
    return this.join;
  }

  public SqlTable GetNestedJoin() {
    return nestedJoin;
  }

  public SqlQueryStatement GetQuery() {
    return query;
  }

  public SqlFormulaColumn GetTableValueFunction() {
    return this.tableValueFunction;
  }

  public string GetFullName() {
    return GetFullName('\0');
  }

  public string GetFullName(char quotes) {
    return GetFullName(quotes, quotes);
  }

  public string GetFullName(char openQuote, char closeQuote) {
    ByteBuffer fullName = new ByteBuffer();
    if (catalog != null) {
      if (openQuote != '\0') fullName.Append(openQuote);
      fullName.Append(catalog);
      if (closeQuote != '\0') fullName.Append(closeQuote);
    }
    if (schema != null) {
      if (fullName.Length > 0) {
        fullName.Append(".");
      }
      if (openQuote != '\0') fullName.Append(openQuote);
      fullName.Append(schema);
      if (closeQuote != '\0') fullName.Append(closeQuote);
    }
    if (name != null) {
      if (fullName.Length > 0) {
        fullName.Append(".");
      }
      if (openQuote != '\0') fullName.Append(openQuote);
      fullName.Append(name);
      if (closeQuote != '\0') fullName.Append(closeQuote);
    }
    return fullName.ToString();
  }

  public string GetValidName() {
    // For NESTED_QUERY table, we must use the alias name.
    string name = this.GetName();
    if (!ParserCore.IsValidTableName(name)) {
      if (this.alias != null) {
        name = this.alias;
      }
    }

    return name;
  }

  public SqlTableMeta GetMetaData() {
    return metaData;
  }

  public SqlTable Resolve() {
    if (this.nestedJoin != null) {
      return this.nestedJoin.Resolve();
    } else if (this.query != null) {
      SqlTable queryTable = this.query.GetTable();
      if (queryTable != null) {
        return queryTable.Resolve();
      }
    }

    return this;
  }

  public string GetResolveNestedTableExprName() {
    if (this.nestedJoin != null) {
      return this.nestedJoin.GetResolveNestedTableExprName();
    } else {
      return GetName();
    }
  }

  public string GetResolveNestedTableExprAlias() {
    //(A AS A1 JOIN B), (A A1 JOIN B), return A1
    if (this.nestedJoin != null) {
      if (Utilities.EqualIgnoreCase(GetName(), GetAlias())) {
        return this.nestedJoin.GetResolveNestedTableExprAlias();
      }
    }
    return GetAlias();
  }

  public SqlCrossApply GetCrossApply() { return this.crossApply; }

  public void Accept(ISqlQueryVisitor visitor) {
    visitor.Visit(this);
  }

  public bool IsNestedQueryTable() {
    return this.query != null;
  }

  public bool IsNestedJoinTable() {
    return this.nestedJoin != null;
  }

  public bool IsFunctionValueTable() {
    return this.tableValueFunction != null;
  }

  public bool IsSourceTable() {
    return !IsNestedQueryTable() && !IsNestedJoinTable() && !HasJoin();
  }

  public bool IsPseudoTable() { return Utilities.EqualIgnoreCaseInvariant(this.name, PSEUDO_TABLE_INDICATOR); }

  public bool HasJoin() {
    return this.GetJoin() != null;
  }

  public bool HasCrossApply() { return this.crossApply != null; }

  public bool HasAlias() {
    bool hasAlias = this.GetName() == null && this.GetAlias() != null;
    hasAlias = hasAlias || (this.GetName() != null && this.GetAlias() != null && !Utilities.EqualIgnoreCaseInvariant(this.GetName(), this.GetAlias()));
    return hasAlias;
  }
}
}

