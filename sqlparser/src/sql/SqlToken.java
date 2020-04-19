//@
package cdata.sql;
//@
/*#
using RSSBus.core;
using RSSBus;
namespace CData.Sql {
#*/

public final class SqlToken {
  public final TokenKind Kind;
  public final String Value;
  public final boolean WasQuoted;
  public final String OpenQuote;
  public final String CloseQuote;
  public static final SqlToken None = new SqlToken("", TokenKind.Identifier);
  public static final SqlToken Null = new SqlToken("NULL", TokenKind.Null);
  public static final SqlToken AnonParam = new SqlToken("?", TokenKind.Parameter);
  public static final SqlToken ESCInitiator = new SqlToken("{", TokenKind.ESCInitiator);
  public static final SqlToken ESCTerminator = new SqlToken("}", TokenKind.ESCTerminator);
  public static final SqlToken Open = new SqlToken("(", TokenKind.Open);
  public static final SqlToken Close = new SqlToken(")", TokenKind.Close);
  public static final SqlToken Dot = new SqlToken(".", TokenKind.Dot);

  public SqlToken(String value, TokenKind kind) {
    this(value, false, null, null,kind);
  }

  public SqlToken(String value, boolean quoted, String openQuote, String closeQuote, TokenKind kind) {
    this.Kind = kind;
    this.Value = value;
    this.WasQuoted = quoted;
    this.OpenQuote = openQuote;
    this.CloseQuote = closeQuote;
  }

  public boolean IsEmpty() {
    return this.equals(None);
  }

  public /*#override#*/ String toString() {
    return Value;
  }

  public boolean IsKeyword(String text) {
    if (this.Kind != TokenKind.Identifier) return false;
    if (this.WasQuoted) return false;
    return Value.equalsIgnoreCase(text);
  }

  public boolean IsOperator(String text) {
    if (this.Kind != TokenKind.Operator) return false;
    if (this.WasQuoted) return false;
    return Value.equalsIgnoreCase(text);
  }

  public /*#override#*/ int hashCode() {
    int hash = 7;
    hash = 97 * hash + this.Kind.hashCode();
    hash = 97 * hash + this.Value.toLowerCase().hashCode();
    return hash;
  }

  public /*#override#*/ boolean equals(Object obj) {
    if (obj instanceof SqlToken) {
      SqlToken tk = (SqlToken) obj;
      return Value.equalsIgnoreCase(tk.Value) & tk.Kind == this.Kind;
    }
    if (obj instanceof String) {
      String str = (String) obj;
      return Value.equalsIgnoreCase(str);
    }
    return false;
  }
  //@

  /**
   * @deprecated DO NOT call this method! Use the equals override.
   */
  @Deprecated
  public boolean Equals(Object obj) {
    return this.equals(obj); // Just in case there are still some places that call this method.
  }

  //@
  public boolean IsParameter() {
    return this.equals(SqlToken.AnonParam) || Kind == TokenKind.Parameter ||
            (Kind == TokenKind.Identifier && Value.startsWith("@"));
  }

  public Object GetNativeValue() {
    if (this.equals(Null)) return null;
    if (this.Kind == TokenKind.Bool) {
      if (Value == null) return false;
      else if (getEqualsIgnoreCase("true", Value.trim())) return true;
      else return false;
    }
    return Value;
  }

  private static boolean getEqualsIgnoreCase(String str1, String str2) {
    return str1.equalsIgnoreCase(str2);
  }
}

