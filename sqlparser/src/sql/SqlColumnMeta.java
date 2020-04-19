//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class SqlColumnMeta {
  private ColumnInfo _info;
  public SqlColumnMeta(ColumnInfo info) {
    this._info = info;
  }

  public ColumnInfo getInfo() {
    return this._info;
  }

  public void setInfo(ColumnInfo val) {
    this._info = val;
  }
  
  public String DefaultValue = "";
  public String OriginalName;
  public IFormula Formula;
  public boolean EvaluateFormula;
  public boolean EvaluateConstant = true;
  public SqlValue SqlConstant;
}

