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


public sealed class SqlWildcardColumn : SqlGeneralColumn , ISqlElement {
  public SqlWildcardColumn() : base("*") {
  }

  public SqlWildcardColumn(SqlTable table) : base(table, "*") {
  }

  public override bool Equals(string name) {
    if (!Utilities.IsNullOrEmpty(name)) {
      return name.Equals("*");
    }
    return false;
  }

  public override bool IsEvaluatable() {
    return false;
  }

  public override SqlValue Evaluate() {
    return SqlValue.GetNullValueInstance();
  }

  public override Object Clone() {
    SqlWildcardColumn obj = new SqlWildcardColumn(this.table);
    obj.Copy(this);
    return obj;
  }
}
}

