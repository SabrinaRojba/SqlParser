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


public class NormalizationOptionBuilder {
  private JavaHashtable<NormalizationOptions, Object> _options = new JavaHashtable<NormalizationOptions, Object>();

  public NormalizationOptionBuilder Config(NormalizationOptions option, Object val) {
    this.SetOption(option, val);
    return this;
  }

  public string GetConfigAsString(NormalizationOptions configName) {
    return GetConfigAsString(configName, "");
  }

  public string GetConfigAsString(NormalizationOptions configName, string nullValue) {
    Object objValue = GetOption(configName);
    if (objValue is string) {
      return (string)objValue;
    }
    return nullValue;
  }

  public string[] GetConfigAsStringArray(NormalizationOptions configName) {
    return this.GetConfigAsStringArray(configName, new string[0]);
  }

  public string[] GetConfigAsStringArray(NormalizationOptions configName, string[] nullValue) {
    Object objValue = GetOption(configName);
    if (objValue is string[]) {
      return ( string[])objValue;
    }
    return nullValue;
  }

  public bool GetConfigAsBoolean(NormalizationOptions configName) {
    return this.GetConfigAsBoolean(configName, false);
  }

  public bool GetConfigAsBoolean(NormalizationOptions configName, bool nullValue) {
    Object objValue = GetOption(configName);
    if (objValue is Boolean) {
      return (Boolean)objValue;
    }
    return nullValue;
  }

  public int GetConfigAsInteger(NormalizationOptions configName) {
    return this.GetConfigAsInteger(configName, -1);
  }

  public int GetConfigAsInteger(NormalizationOptions configName, int nullValue) {
    Object objValue = GetOption(configName);
    if (objValue is int) {
      return (int)objValue;
    }
    return nullValue;
  }

  public NormalizationOption Build() {
    return new NormalizationOption(this);
  }

  public void Clear() {
    this._options.Clear();
  }

  public void Copy(NormalizationOptionBuilder obj) {
    JavaEnumeration e = obj._options.Keys();
    while (e.HasMoreElements()) {
      NormalizationOptions key = (NormalizationOptions)e.NextElement();
      this._options.Put(key, obj._options.Get(key));
    }
  }

  private void SetOption(NormalizationOptions configName, Object configValue) {
    if (!this._options.ContainsKey(configName)) {
      this._options.Put(configName, configValue);
    }
  }

  private Object GetOption(NormalizationOptions configName) {
    if (this._options.ContainsKey(configName)) {
      return this._options.Get(configName);
    }
    return null;
  }
}
}

