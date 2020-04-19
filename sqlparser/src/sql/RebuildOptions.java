//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/
public class RebuildOptions {
  public static final String DEFAULT_PARA_NAME_PATTERN = /*#"@{0}"#*//*@*/"?"/*@*/;
  public static final RebuildOptions SQLite = new RebuildOptions(/*@*/"[%s]"/*@*//*#"[{0}]"#*/, "@{0}", true, false, true, false, false);
  public static final RebuildOptions SQL = new RebuildOptions(/*@*/"[%s]"/*@*//*#"[{0}]"#*/, "@{0}", false, true, false, false, false);
  public static final RebuildOptions MySQL = new RebuildOptions(/*@*/"`%s`"/*@*//*#"`{0}`"#*/, "@{0}", true, false, true, false, false);
  public static final RebuildOptions Oracle = new RebuildOptions(/*@*/"\"%s\""/*@*//*#"\"{0}\""#*/, DEFAULT_PARA_NAME_PATTERN, false, false, false, false, false);
  public static final RebuildOptions DB2 = new RebuildOptions(/*@*/"\"%s\""/*@*//*#"\"{0}\""#*/, "@{0}", true, false, true, false, false);
  public static final RebuildOptions PostgreSQL = new RebuildOptions(/*@*/"\"%s\""/*@*//*#"\"{0}\""#*/, "@{0}", true, false, true, false, false);
  private String identifierQuotePattern;
  private String paraNamePattern;
  private boolean storesUppercaseIdentifiers;
  private boolean storesLowercaseIdentifiers;
  private boolean useLimit;
  private boolean useTop;
  private boolean useOffset;
  private boolean quoteTableForGoogleSQL = false;
  private boolean resolveParameterValues = false;

  public RebuildOptions(String identifierQuotePattern,
                        String paraNamePattern,
                        boolean useLimit,
                        boolean useTop,
                        boolean useOffset,
                        boolean storesUppercaseIdentifiers,
                        boolean storesLowercaseIdentifiers,
                        boolean resolveParameterValues) {
    this.identifierQuotePattern = identifierQuotePattern;
    this.paraNamePattern = paraNamePattern;
    this.storesUppercaseIdentifiers = storesUppercaseIdentifiers;
    this.storesLowercaseIdentifiers = storesLowercaseIdentifiers;
    this.useLimit = useLimit;
    this.useTop = useTop;
    this.useOffset = useOffset;
    this.resolveParameterValues = resolveParameterValues;
  }

  public RebuildOptions(String identifierQuotePattern,
                        String paraNamePattern,
                        boolean useLimit,
                        boolean useTop,
                        boolean useOffset,
                        boolean storesUppercaseIdentifiers,
                        boolean storesLowercaseIdentifiers) {
    this(identifierQuotePattern,
            paraNamePattern,
            useLimit,
            useTop,
            useOffset,
            storesUppercaseIdentifiers,
            storesLowercaseIdentifiers,
            false);
  }

  private RebuildOptions() {
  }


  public boolean getUseLimit() {
    return useLimit;
  }
  
  public void setUseLimit(boolean value){
    useLimit = value;
  }

  public boolean getUseTop() {
    return useTop;
  }
  
  public void setUseTop(boolean value){
    useTop = value;
  }
  
  public boolean getUseOffset() {
    return useOffset;
  }
  
  public void setUseOffset(boolean value){
    useOffset = value;
  }
  
  public boolean getStoresUpperCaseIdentifiers(){
    return this.storesUppercaseIdentifiers;
  }
  
  public void setStoresUpperCaseIdentifiers(boolean value){
    this.storesUppercaseIdentifiers = value;
  }
  
  public boolean getStoresLowercaseIdentifiers(){
    return this.storesLowercaseIdentifiers;
  }
  
  public void setStoresLowercaseIdentifiers(boolean value){
    this.storesLowercaseIdentifiers = value;
  }

  public void setResolveParameterValues(boolean value) {
    this.resolveParameterValues = value;
  }

  public boolean getResolveParameterValues() {
    return this.resolveParameterValues;
  }

  public String quoteIdentifier(String str) {
    if(str == null || str.length() <= 0) return str;
    
    if("*".equals(str)) return str;
    
    if(getStoresUpperCaseIdentifiers()){
      str = str.trim().toUpperCase();
    }  else if(getStoresLowercaseIdentifiers()){
      str = str.trim().toLowerCase();
    }
    
    StringBuilder sb = new StringBuilder();
    if (quoteTableForGoogleSQL) {
      sb.append(String.format(identifierQuotePattern, str));
    } else {
      String[] parts = /*@*/str.split("\\.")/*@*//*#str.Split('.')#*/;
      for(int i=0;i<parts.length;i++){
        if(i > 0) sb.append(".");
        sb.append(String.format(identifierQuotePattern, parts[i]));
      }
    }
    return sb.toString();
  }

  public String quoteIdentifierWithDot(String str) {
    if(str == null || str.length() <= 0) return str;
    if("*".equals(str)) return str;

    if(getStoresUpperCaseIdentifiers()){
      str = str.trim().toUpperCase();
    } else if(getStoresLowercaseIdentifiers()){
      str = str.trim().toLowerCase();
    }
    StringBuilder sb = new StringBuilder();
    sb.append(String.format(identifierQuotePattern, str));
    return sb.toString();
  }

  public String getIdentifierQuotePattern(){
    return identifierQuotePattern;
  }
  
  public void setIdentifierQuotePattern(String pattern){
    identifierQuotePattern = pattern;
  }
  
  public String getParaNamePattern(){
    return paraNamePattern;
  }
  
  public void setParaNamePattern(String pattern){
    paraNamePattern = pattern;
  }

  public boolean getQuoteTableForGoogleSQL() {
    return quoteTableForGoogleSQL;
  }

  public void setQuoteTableForGoogleSQL(boolean quoteTableForGoogleSQL) {
    this.quoteTableForGoogleSQL = quoteTableForGoogleSQL;
  }

  public RebuildOptions clone() {
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

  public String [] getOpenCloseQuote() {
    String PATTERN = String.format(this.identifierQuotePattern, "");
    if (PATTERN != null &&
            PATTERN.length() > 1) {
      return new String[] {PATTERN.charAt(0) + "",
              PATTERN.charAt(PATTERN.length() - 1) + ""};
    } else {
      return new String [] {"", ""};
    }
  }
}
