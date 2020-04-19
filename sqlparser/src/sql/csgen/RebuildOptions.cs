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

public class RebuildOptions {
  public const string DEFAULT_PARA_NAME_PATTERN = "@{0}";
  public static RebuildOptions SQLite = new RebuildOptions("[{0}]", "@{0}", true, false, true, false, false);
  public static RebuildOptions SQL = new RebuildOptions("[{0}]", "@{0}", false, true, false, false, false);
  public static RebuildOptions MySQL = new RebuildOptions("`{0}`", "@{0}", true, false, true, false, false);
  public static RebuildOptions Oracle = new RebuildOptions("\"{0}\"", DEFAULT_PARA_NAME_PATTERN, false, false, false, false, false);
  public static RebuildOptions DB2 = new RebuildOptions("\"{0}\"", "@{0}", true, false, true, false, false);
  public static RebuildOptions PostgreSQL = new RebuildOptions("\"{0}\"", "@{0}", true, false, true, false, false);
  private string identifierQuotePattern;
  private string paraNamePattern;
  private bool storesUppercaseIdentifiers;
  private bool storesLowercaseIdentifiers;
  private bool useLimit;
  private bool useTop;
  private bool useOffset;
  private bool quoteTableForGoogleSQL = false;
  private bool resolveParameterValues = false;

  public RebuildOptions(string identifierQuotePattern,
                        string paraNamePattern,
                        bool useLimit,
                        bool useTop,
                        bool useOffset,
                        bool storesUppercaseIdentifiers,
                        bool storesLowercaseIdentifiers,
                        bool resolveParameterValues) {
    this.identifierQuotePattern = identifierQuotePattern;
    this.paraNamePattern = paraNamePattern;
    this.storesUppercaseIdentifiers = storesUppercaseIdentifiers;
    this.storesLowercaseIdentifiers = storesLowercaseIdentifiers;
    this.useLimit = useLimit;
    this.useTop = useTop;
    this.useOffset = useOffset;
    this.resolveParameterValues = resolveParameterValues;
  }

  public RebuildOptions(string identifierQuotePattern,
                        string paraNamePattern,
                        bool useLimit,
                        bool useTop,
                        bool useOffset,
                        bool storesUppercaseIdentifiers,
                        bool storesLowercaseIdentifiers) : this(identifierQuotePattern,
            paraNamePattern,
            useLimit,
            useTop,
            useOffset,
            storesUppercaseIdentifiers,
            storesLowercaseIdentifiers,
            false) {
  }

  private RebuildOptions() {
  }


  public bool GetUseLimit() {
    return useLimit;
  }
  
  public void SetUseLimit(bool value){
    useLimit = value;
  }

  public bool GetUseTop() {
    return useTop;
  }
  
  public void SetUseTop(bool value){
    useTop = value;
  }
  
  public bool GetUseOffset() {
    return useOffset;
  }
  
  public void SetUseOffset(bool value){
    useOffset = value;
  }
  
  public bool GetStoresUpperCaseIdentifiers(){
    return this.storesUppercaseIdentifiers;
  }
  
  public void SetStoresUpperCaseIdentifiers(bool value){
    this.storesUppercaseIdentifiers = value;
  }
  
  public bool GetStoresLowercaseIdentifiers(){
    return this.storesLowercaseIdentifiers;
  }
  
  public void SetStoresLowercaseIdentifiers(bool value){
    this.storesLowercaseIdentifiers = value;
  }

  public void SetResolveParameterValues(bool value) {
    this.resolveParameterValues = value;
  }

  public bool GetResolveParameterValues() {
    return this.resolveParameterValues;
  }

  public string QuoteIdentifier(string str) {
    if(str == null || str.Length <= 0) return str;
    
    if("*".Equals(str)) return str;
    
    if(GetStoresUpperCaseIdentifiers()){
      str = str.Trim().ToUpper();
    }  else if(GetStoresLowercaseIdentifiers()){
      str = str.Trim().ToLower();
    }
    
    ByteBuffer sb = new ByteBuffer();
    if (quoteTableForGoogleSQL) {
      sb.Append(string.Format(identifierQuotePattern, str));
    } else {
      string[] parts = str.Split('.');
      for(int i=0;i<parts.Length;i++){
        if(i > 0) sb.Append(".");
        sb.Append(string.Format(identifierQuotePattern, parts[i]));
      }
    }
    return sb.ToString();
  }

  public string QuoteIdentifierWithDot(string str) {
    if(str == null || str.Length <= 0) return str;
    if("*".Equals(str)) return str;

    if(GetStoresUpperCaseIdentifiers()){
      str = str.Trim().ToUpper();
    } else if(GetStoresLowercaseIdentifiers()){
      str = str.Trim().ToLower();
    }
    ByteBuffer sb = new ByteBuffer();
    sb.Append(string.Format(identifierQuotePattern, str));
    return sb.ToString();
  }

  public string GetIdentifierQuotePattern(){
    return identifierQuotePattern;
  }
  
  public void SetIdentifierQuotePattern(string pattern){
    identifierQuotePattern = pattern;
  }
  
  public string GetParaNamePattern(){
    return paraNamePattern;
  }
  
  public void SetParaNamePattern(string pattern){
    paraNamePattern = pattern;
  }

  public bool GetQuoteTableForGoogleSQL() {
    return quoteTableForGoogleSQL;
  }

  public void SetQuoteTableForGoogleSQL(bool quoteTableForGoogleSQL) {
    this.quoteTableForGoogleSQL = quoteTableForGoogleSQL;
  }

  public RebuildOptions Clone() {
    RebuildOptions obj = new RebuildOptions();
    obj.identifierQuotePattern = this.identifierQuotePattern;
    obj.paraNamePattern = this.paraNamePattern;
    obj.storesUppercaseIdentifiers = this.storesUppercaseIdentifiers;
    obj.storesLowercaseIdentifiers = this.storesLowercaseIdentifiers;
    obj.useLimit = this.useLimit;
    obj.useTop = this.useTop;
    obj.useOffset = this.useOffset;
    obj.quoteTableForGoogleSQL = this.quoteTableForGoogleSQL;
    return obj;
  }

  public string [] GetOpenCloseQuote() {
    string PATTERN = string.Format(this.identifierQuotePattern, "");
    if (PATTERN != null &&
            PATTERN.Length > 1) {
      return new string[] {PATTERN[0] + "",
              PATTERN[PATTERN.Length - 1] + ""};
    } else {
      return new string [] {"", ""};
    }
  }
}
}

