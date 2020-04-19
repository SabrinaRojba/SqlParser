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
using System.Collections.Generic;
namespace CData.Sql {

public sealed class SqlUtilities {
  public const string CACHE_SUFFIX = "#Cache";
  public const string DELETED_SUFFIX = "#Deleted";
  public const string TEMP_SUFFIX = "#Temp";

  public const string CDATA_SQL_VARCHAR = "VARCHAR";
  public const string CDATA_SQL_BIT = "BIT";
  public const string CDATA_SQL_BINARY = "BINARY";
  public const string CDATA_SQL_BIGINT = "BIGINT";
  public const string CDATA_SQL_INTEGER = "INTEGER";
  public const string CDATA_SQL_SMALLINT = "SMALLINT";
  public const string CDATA_SQL_FLOAT = "FLOAT";
  public const string CDATA_SQL_DOUBLE = "DOUBLE";
  public const string CDATA_SQL_DECIMAL = "DECIMAL";
  public const string CDATA_SQL_DATE = "DATE";
  public const string CDATA_SQL_TIME = "TIME";
  public const string CDATA_SQL_TIMESTAMP = "TIMESTAMP";

  public static string GetSqlType(int typeCode) {
    if (typeCode == ColumnInfo.DATA_TYPE_BINARY) {
      return CDATA_SQL_BINARY;
    } else if (typeCode == ColumnInfo.DATA_TYPE_VARBINARY) {
      return CDATA_SQL_BINARY;
    } else if (typeCode == ColumnInfo.DATA_TYPE_BLOB) {
      return CDATA_SQL_BINARY;
    } else if (typeCode == ColumnInfo.DATA_TYPE_BOOLEAN) {
      return CDATA_SQL_BIT;
    } else if (typeCode == ColumnInfo.DATA_TYPE_DATE) {
      return CDATA_SQL_DATE;
    } else if (typeCode == ColumnInfo.DATA_TYPE_TIMESTAMP) {
      return CDATA_SQL_TIMESTAMP;
    } else if (typeCode == ColumnInfo.DATA_TYPE_TIME) {
      return CDATA_SQL_TIME;
    } else if (typeCode == ColumnInfo.DATA_TYPE_DECIMAL) {
      return CDATA_SQL_DECIMAL;
    } else if (typeCode == ColumnInfo.DATA_TYPE_DOUBLE) {
      return CDATA_SQL_DOUBLE;
    } else if (typeCode == ColumnInfo.DATA_TYPE_TINYINT) {
      return CDATA_SQL_SMALLINT;
    } else if (typeCode == ColumnInfo.DATA_TYPE_SMALLINT) {
      return CDATA_SQL_SMALLINT;
    } else if (typeCode == ColumnInfo.DATA_TYPE_INTEGER) {
      return CDATA_SQL_INTEGER;
    } else if (typeCode == ColumnInfo.DATA_TYPE_BIGINT) {
      return CDATA_SQL_BIGINT;
    } else if (typeCode == ColumnInfo.DATA_TYPE_FLOAT) {
      return CDATA_SQL_FLOAT;
    } else if (typeCode == ColumnInfo.DATA_TYPE_VARCHAR) {
      return CDATA_SQL_VARCHAR;
    } else {
      return CDATA_SQL_VARCHAR;
    }
  }
  
  public static bool IsCacheTable(string tableName) {
    return tableName.ToLower().EndsWith(CACHE_SUFFIX.ToLower());
  }

  public static bool IsTempTable(string tableName) {
    return tableName.ToLower().EndsWith(TEMP_SUFFIX.ToLower());
  }

  public static bool IsDeletedTable(string tableName){
    return tableName.ToLower().EndsWith(DELETED_SUFFIX.ToLower());
  }

  public static bool IsKnownAggragation(string formulaName) {
    bool isAggragation = false;
    string [] knownAggragation = new string [] {"AVG", "SUM", "MIN", "MAX", "COUNT", "STDEV", "STDEVP", "VAR", "VARP", "COUNT_BIG"};
    if (formulaName != null) {
      foreach(String name in knownAggragation) {
        if (RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(name, formulaName)) {
          isAggragation = true;
          break;
        }
      }
    }
    return isAggragation;
  }

  public static bool IsSimpleTable(SqlTable table) {
    return !table.HasJoin() && !table.IsNestedQueryTable() && !table.IsNestedJoinTable() && !table.IsFunctionValueTable();
  }

  public static SqlCollection<SqlTable> GetSourceTables(SqlStatement stmt) {
    SqlCollection<SqlTable> sources = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(sources, new SourceTableMatcher());
    stmt.Accept(collector);
    return sources;
  }

  public static SqlCollection<SqlTable> GetTables(SqlStatement stmt, ITableMatch matcher) {
    SqlCollection<SqlTable> sources = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(sources, matcher);
    stmt.Accept(collector);
    return sources;
  }

  public static   void FetchElement<T> (SqlCollection<T> inSets, SqlCollection<T> outSets, ISqlElementComparable<T> comparable)  where T : ISqlElement {
    foreach(T t in inSets) {
      if (0 == comparable.CompareTo(t)) {
        outSets.Add(t);
      }
    }
  }

  public static void CheckQuerySemantic(SqlQueryStatement queryStatement) {
    if (queryStatement.GetTable() == null) return;

    SqlCollection<SqlTable> references = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(references, new TableAliasMatcher());
    queryStatement.Accept(collector);
    queryStatement.Accept(new SqlQueryAliasChecker(references));

    queryStatement.Accept(new SqlColumnSourceChecker());
  }

  public static SqlTable BuildTempTable(SqlTable sqlTable) {
    return new SqlTable(sqlTable.GetCatalog(), sqlTable.GetSchema(), sqlTable.GetName() + TEMP_SUFFIX);
  }

  public static void CollectParameters(SqlConditionNode criteria, IList<QueryParameter> collector) {
    if (criteria == null || collector == null) {
      return;
    }

    if (criteria is SqlCriteria) {
      SqlCriteria c = (SqlCriteria) criteria;
      if (c.IsRightParameter()) {
        //TODO: Should we support const, function and more?
        if (c.GetLeft() is SqlGeneralColumn) {
          collector.Add(new QueryParameter(((SqlGeneralColumn) c.GetLeft()).GetColumnName(), null));
        }
      } else if (c.IsLeftParameter()) {
        if (c.GetRight() is SqlGeneralColumn) {
          collector.Add(new QueryParameter(((SqlGeneralColumn) c.GetRight()).GetColumnName(), null));
        }
      }
    } else if (criteria is SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      if (c.GetLeft() is SqlConditionNode) {
        CollectParameters((SqlConditionNode)c.GetLeft(), collector);
      }
      if (c.GetRight() is SqlConditionNode) {
        CollectParameters((SqlConditionNode)c.GetRight(), collector);
      }
    } else if (criteria is SqlConditionNot) {
      SqlExpression c = ((SqlConditionNot) criteria).GetCondition();
      if (c is SqlConditionNode) {
        CollectParameters((SqlConditionNode) c, collector);
      }
    }
  }
}

sealed class SourceTableMatcher : ITableMatch {

  public SqlTable Create(SqlTable table) {
    return new SqlTable(table.GetCatalog(),
            table.GetSchema(),
            table.GetName());
  }

  public bool Accept(SqlTable t, TablePartType type) {
    return (TablePartType.SimpleTable  == type
            && ParserCore.IsValidTableName(t.GetName()));
  }

  public bool Unwind(SqlTable t, TablePartType type) {
    return TablePartType.NestedQuery == type
        || TablePartType.NestedJoin == type;
  }
}

sealed class TableAliasMatcher : ITableMatch {

  public SqlTable Create(SqlTable table) {
    return new SqlTable(table.GetCatalog(),
            table.GetSchema(),
            table.GetName(),
            table.GetAlias());
  }

  public bool Accept(SqlTable t, TablePartType type) {
    return true;
  }

  public bool Unwind(SqlTable t, TablePartType type) {
    return true;
  }
}

sealed class TableOnlyMatcher : ITableMatch {

  public SqlTable Create(SqlTable table) {
    return new SqlTable(table.GetCatalog(),
            table.GetSchema(),
            table.GetName(),
            table.GetAlias());
  }

  public bool Accept(SqlTable t, TablePartType type) {
    return type != TablePartType.JoinPart;
  }

  public bool Unwind(SqlTable t, TablePartType type) {
    return true;
  }
}
}

