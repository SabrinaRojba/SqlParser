//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public class SqlUpdatability {
  public final static int READ_ONLY = 1;
  public final static int UPDATE = 2;
  private final int _type;
  private final SqlCollection<SqlColumn> _columns;

  public SqlUpdatability(int type) {
    this(type, new SqlCollection<SqlColumn>());
  }

  public SqlUpdatability(int type, SqlCollection<SqlColumn> columns) {
    this._type = type;
    this._columns = columns;
  }

  public int getType() {
    return this._type;
  }

  public SqlCollection<SqlColumn> getColumns() {
    return this._columns;
  }
}

