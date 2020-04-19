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


public class SqlColumnMeta {
  private ColumnInfo _info;
  public SqlColumnMeta(ColumnInfo info) {
    this._info = info;
  }

  public ColumnInfo GetInfo() {
    return this._info;
  }

  public void SetInfo(ColumnInfo val) {
    this._info = val;
  }
  
  public string DefaultValue = "";
  public string OriginalName;
  public IFormula Formula;
  public bool EvaluateFormula;
  public bool EvaluateConstant = true;
  public SqlValue SqlConstant;
}
}

