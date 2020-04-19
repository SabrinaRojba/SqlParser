//@
package cdata.sql;
import java.util.Enumeration;
import java.util.Hashtable;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public class NormalizationOptionBuilder {
  private Hashtable<NormalizationOptions, Object> _options = new Hashtable<NormalizationOptions, Object>();

  public NormalizationOptionBuilder config(NormalizationOptions option, Object val) {
    this.setOption(option, val);
    return this;
  }

  public String getConfigAsString(NormalizationOptions configName) {
    return getConfigAsString(configName, "");
  }

  public String getConfigAsString(NormalizationOptions configName, String nullValue) {
    Object objValue = getOption(configName);
    if (objValue instanceof String) {
      return (String)objValue;
    }
    return nullValue;
  }

  public String[] getConfigAsStringArray(NormalizationOptions configName) {
    return this.getConfigAsStringArray(configName, new String[0]);
  }

  public String[] getConfigAsStringArray(NormalizationOptions configName, String[] nullValue) {
    Object objValue = getOption(configName);
    if (objValue instanceof String[]) {
      return ( String[])objValue;
    }
    return nullValue;
  }

  public boolean getConfigAsBoolean(NormalizationOptions configName) {
    return this.getConfigAsBoolean(configName, false);
  }

  public boolean getConfigAsBoolean(NormalizationOptions configName, boolean nullValue) {
    Object objValue = getOption(configName);
    if (objValue instanceof Boolean) {
      return (Boolean)objValue;
    }
    return nullValue;
  }

  public int getConfigAsInteger(NormalizationOptions configName) {
    return this.getConfigAsInteger(configName, -1);
  }

  public int getConfigAsInteger(NormalizationOptions configName, int nullValue) {
    Object objValue = getOption(configName);
    if (objValue instanceof Integer) {
      return (Integer)objValue;
    }
    return nullValue;
  }

  public NormalizationOption build() {
    return new NormalizationOption(this);
  }

  public void clear() {
    this._options.clear();
  }

  public void copy(NormalizationOptionBuilder obj) {
    Enumeration e = obj._options.keys();
    while (e.hasMoreElements()) {
      NormalizationOptions key = (NormalizationOptions)e.nextElement();
      this._options.put(key, obj._options.get(key));
    }
  }

  private void setOption(NormalizationOptions configName, Object configValue) {
    if (!this._options.containsKey(configName)) {
      this._options.put(configName, configValue);
    }
  }

  private Object getOption(NormalizationOptions configName) {
    if (this._options.containsKey(configName)) {
      return this._options.get(configName);
    }
    return null;
  }
}