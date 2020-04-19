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


public sealed class SqlJoin : ISqlCloneable {
  private readonly JoinType type;
  private readonly SqlTable table;
  private readonly SqlConditionNode criteria;
  private readonly bool isEach;
  private readonly bool hasOuter;

  public SqlJoin(JoinType type, SqlTable table, SqlConditionNode criteria, bool isEach, bool hasOuter) {
    this.type = type;
    this.table = table;
    this.criteria = criteria;
    this.isEach = isEach;
    this.hasOuter = hasOuter;
  }

  public SqlJoin(JoinType type, SqlTable table, SqlConditionNode criteria, bool isEach) : this(type, table, criteria, isEach, false) {
  }

  public SqlJoin(JoinType type, SqlTable table, SqlConditionNode criteria) : this(type, table, criteria, false, false) {
  }

  public JoinType GetJoinType() {
    return this.type;
  }

  public string GetJoinTypeAsString() {
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

  public SqlTable GetTable() {
    return this.table;
  }

  public SqlConditionNode GetCondition() {
    return this.criteria;
  }

  public string GetTableName() {
    if (this.table != null) {
      if (this.table.GetNestedJoin() != null) {
        return this.table.GetResolveNestedTableExprName();
      } else {
        return this.table.GetName();
      }
    }
    return null;
  }

  public bool IsEach() {
    return this.isEach;
  }

  public bool HasOuter() {
    return this.hasOuter;
  }

  public Object Clone() {
    return new SqlJoin(this.type, this.table, this.criteria, this.isEach, this.hasOuter);
  }
}
}

