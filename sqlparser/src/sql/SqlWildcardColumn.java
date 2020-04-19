//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

import rssbus.oputils.common.Utilities;
public final class SqlWildcardColumn extends SqlGeneralColumn implements ISqlElement {
  public SqlWildcardColumn() {
    super("*");
  }

  public SqlWildcardColumn(SqlTable table) {
    super(table, "*");
  }

  public /*#override#*/ boolean equals(String name) {
    if (!Utilities.isNullOrEmpty(name)) {
      return name.equals("*");
    }
    return false;
  }

  public /*#override#*/ boolean isEvaluatable() {
    return false;
  }

  public /*#override#*/ SqlValue evaluate() {
    return SqlValue.getNullValueInstance();
  }

  public /*#override#*/ Object clone() {
    SqlWildcardColumn obj = new SqlWildcardColumn(this.table);
    obj.copy(this);
    return obj;
  }
}

