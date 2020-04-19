//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlJoin implements ISqlCloneable {
  private final JoinType type;
  private final SqlTable table;
  private final SqlConditionNode criteria;
  private final boolean isEach;
  private final boolean hasOuter;

  public SqlJoin(JoinType type, SqlTable table, SqlConditionNode criteria, boolean isEach, boolean hasOuter) {
    this.type = type;
    this.table = table;
    this.criteria = criteria;
    this.isEach = isEach;
    this.hasOuter = hasOuter;
  }

  public SqlJoin(JoinType type, SqlTable table, SqlConditionNode criteria, boolean isEach) {
    this(type, table, criteria, isEach, false);
  }

  public SqlJoin(JoinType type, SqlTable table, SqlConditionNode criteria) {
    this(type, table, criteria, false, false);
  }

  public JoinType getJoinType() {
    return this.type;
  }

  public String getJoinTypeAsString() {
   if (type == JoinType.LEFT) {
     return this.hasOuter ? "LEFT OUTER JOIN" : "LEFT JOIN";
   } else if (type == JoinType.RIGHT) {
     return this.hasOuter ? "RIGHT OUTER JOIN" : "RIGHT JOIN";
   } else if (type == JoinType.FULL) {
     return this.hasOuter ? "FULL OUTER JOIN" : "FULL JOIN";
   } else if (type == JoinType.INNER) {
     return "INNER JOIN";
   } else if (type == JoinType.CROSS) {
     return "CROSS JOIN";
   } else if (type == JoinType.NATURAL) {
     return "NATURAL JOIN";
   } else if (type == JoinType.COMMA) {
     return ",";
   } else if (type == JoinType.LEFT_ANTI) {
     return "LEFT ANTI JOIN";
   } else if (type == JoinType.LEFT_SEMI) {
     return "LEFT SEMI JOIN";
   } else {
     return "";
   }
  }

  public SqlTable getTable() {
    return this.table;
  }

  public SqlConditionNode getCondition() {
    return this.criteria;
  }

  public String getTableName() {
    if (this.table != null) {
      if (this.table.getNestedJoin() != null) {
        return this.table.getResolveNestedTableExprName();
      } else {
        return this.table.getName();
      }
    }
    return null;
  }

  public boolean isEach() {
    return this.isEach;
  }

  public boolean hasOuter() {
    return this.hasOuter;
  }

  public Object clone() {
    return new SqlJoin(this.type, this.table, this.criteria, this.isEach, this.hasOuter);
  }
}
