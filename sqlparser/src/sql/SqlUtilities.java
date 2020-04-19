//@
package cdata.sql;
import core.ParserCore;

import java.util.List;
//@

/*#
using RSSBus.core;
using RSSBus;
using System.Collections.Generic;
namespace CData.Sql {
#*/
public final class SqlUtilities {
  public static final String CACHE_SUFFIX = "#Cache";
  public static final String DELETED_SUFFIX = "#Deleted";
  public static final String TEMP_SUFFIX = "#Temp";

  public static final String CDATA_SQL_VARCHAR = "VARCHAR";
  public static final String CDATA_SQL_BIT = "BIT";
  public static final String CDATA_SQL_BINARY = "BINARY";
  public static final String CDATA_SQL_BIGINT = "BIGINT";
  public static final String CDATA_SQL_INTEGER = "INTEGER";
  public static final String CDATA_SQL_SMALLINT = "SMALLINT";
  public static final String CDATA_SQL_FLOAT = "FLOAT";
  public static final String CDATA_SQL_DOUBLE = "DOUBLE";
  public static final String CDATA_SQL_DECIMAL = "DECIMAL";
  public static final String CDATA_SQL_DATE = "DATE";
  public static final String CDATA_SQL_TIME = "TIME";
  public static final String CDATA_SQL_TIMESTAMP = "TIMESTAMP";

  public static String getSqlType(int typeCode) {
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
  
  public static boolean isCacheTable(String tableName) {
    return tableName.toLowerCase().endsWith(CACHE_SUFFIX.toLowerCase());
  }

  public static boolean isTempTable(String tableName) {
    return tableName.toLowerCase().endsWith(TEMP_SUFFIX.toLowerCase());
  }

  public static boolean isDeletedTable(String tableName){
    return tableName.toLowerCase().endsWith(DELETED_SUFFIX.toLowerCase());
  }

  public static boolean isKnownAggragation(String formulaName) {
    boolean isAggragation = false;
    String [] knownAggragation = new String [] {"AVG", "SUM", "MIN", "MAX", "COUNT", "STDEV", "STDEVP", "VAR", "VARP", "COUNT_BIG"};
    if (formulaName != null) {
      for (String name : knownAggragation) {
        if (name.equalsIgnoreCase(formulaName)) {
          isAggragation = true;
          break;
        }
      }
    }
    return isAggragation;
  }

  public static boolean isSimpleTable(SqlTable table) {
    return !table.hasJoin() && !table.isNestedQueryTable() && !table.isNestedJoinTable() && !table.isFunctionValueTable();
  }

  public static SqlCollection<SqlTable> getSourceTables(SqlStatement stmt) throws Exception {
    SqlCollection<SqlTable> sources = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(sources, new SourceTableMatcher());
    stmt.accept(collector);
    return sources;
  }

  public static SqlCollection<SqlTable> getTables(SqlStatement stmt, ITableMatch matcher) throws Exception {
    SqlCollection<SqlTable> sources = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(sources, matcher);
    stmt.accept(collector);
    return sources;
  }

  public static /*@*/<T extends ISqlElement> void fetchElement/*@*/ /*# void FetchElement<T>#*/ (SqlCollection<T> inSets, SqlCollection<T> outSets, ISqlElementComparable<T> comparable) /*# where T : ISqlElement #*/{
    for (T t : inSets) {
      if (0 == comparable.compareTo(t)) {
        outSets.add(t);
      }
    }
  }

  public static void checkQuerySemantic(SqlQueryStatement queryStatement) throws Exception {
    if (queryStatement.getTable() == null) return;

    SqlCollection<SqlTable> references = new SqlCollection<SqlTable>();
    TableFinder collector = new TableFinder(references, new TableAliasMatcher());
    queryStatement.accept(collector);
    queryStatement.accept(new SqlQueryAliasChecker(references));

    queryStatement.accept(new SqlColumnSourceChecker());
  }

  public static SqlTable buildTempTable(SqlTable sqlTable) throws Exception {
    return new SqlTable(sqlTable.getCatalog(), sqlTable.getSchema(), sqlTable.getName() + TEMP_SUFFIX);
  }

  public static void collectParameters(SqlConditionNode criteria, List<QueryParameter> collector) {
    if (criteria == null || collector == null) {
      return;
    }

    if (criteria instanceof SqlCriteria) {
      SqlCriteria c = (SqlCriteria) criteria;
      if (c.isRightParameter()) {
        //TODO: Should we support const, function and more?
        if (c.getLeft() instanceof SqlGeneralColumn) {
          collector.add(new QueryParameter(((SqlGeneralColumn) c.getLeft()).getColumnName(), null));
        }
      } else if (c.isLeftParameter()) {
        if (c.getRight() instanceof SqlGeneralColumn) {
          collector.add(new QueryParameter(((SqlGeneralColumn) c.getRight()).getColumnName(), null));
        }
      }
    } else if (criteria instanceof SqlCondition) {
      SqlCondition c = (SqlCondition)criteria;
      if (c.getLeft() instanceof SqlConditionNode) {
        collectParameters((SqlConditionNode)c.getLeft(), collector);
      }
      if (c.getRight() instanceof SqlConditionNode) {
        collectParameters((SqlConditionNode)c.getRight(), collector);
      }
    } else if (criteria instanceof SqlConditionNot) {
      SqlExpression c = ((SqlConditionNot) criteria).getCondition();
      if (c instanceof SqlConditionNode) {
        collectParameters((SqlConditionNode) c, collector);
      }
    }
  }
}

final class SourceTableMatcher implements ITableMatch {

  public SqlTable create(SqlTable table) {
    return new SqlTable(table.getCatalog(),
            table.getSchema(),
            table.getName());
  }

  public boolean accept(SqlTable t, TablePartType type) {
    return (TablePartType.SimpleTable  == type
            && ParserCore.isValidTableName(t.getName()));
  }

  public boolean unwind(SqlTable t, TablePartType type) {
    return TablePartType.NestedQuery == type
        || TablePartType.NestedJoin == type;
  }
}

final class TableAliasMatcher implements ITableMatch {

  public SqlTable create(SqlTable table) {
    return new SqlTable(table.getCatalog(),
            table.getSchema(),
            table.getName(),
            table.getAlias());
  }

  public boolean accept(SqlTable t, TablePartType type) {
    return true;
  }

  public boolean unwind(SqlTable t, TablePartType type) {
    return true;
  }
}

final class TableOnlyMatcher implements ITableMatch {

  public SqlTable create(SqlTable table) {
    return new SqlTable(table.getCatalog(),
            table.getSchema(),
            table.getName(),
            table.getAlias());
  }

  public boolean accept(SqlTable t, TablePartType type) {
    return type != TablePartType.JoinPart;
  }

  public boolean unwind(SqlTable t, TablePartType type) {
    return true;
  }
}

