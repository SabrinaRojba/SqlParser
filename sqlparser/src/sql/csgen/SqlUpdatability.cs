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

public class SqlUpdatability {
  public readonly static int READ_ONLY = 1;
  public readonly static int UPDATE = 2;
  private readonly int _type;
  private readonly SqlCollection<SqlColumn> _columns;

  public SqlUpdatability(int type) : this(type, new SqlCollection<SqlColumn>()) {
  }

  public SqlUpdatability(int type, SqlCollection<SqlColumn> columns) {
    this._type = type;
    this._columns = columns;
  }

  public int GetType() {
    return this._type;
  }

  public SqlCollection<SqlColumn> GetColumns() {
    return this._columns;
  }
}
}

