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


public sealed class SqlToken {
  public readonly TokenKind Kind;
  public readonly string Value;
  public readonly bool WasQuoted;
  public readonly string OpenQuote;
  public readonly string CloseQuote;
  public static SqlToken None = new SqlToken("", TokenKind.Identifier);
  public static SqlToken Null = new SqlToken("NULL", TokenKind.Null);
  public static SqlToken AnonParam = new SqlToken("?", TokenKind.Parameter);
  public static SqlToken ESCInitiator = new SqlToken("{", TokenKind.ESCInitiator);
  public static SqlToken ESCTerminator = new SqlToken("}", TokenKind.ESCTerminator);
  public static SqlToken Open = new SqlToken("(", TokenKind.Open);
  public static SqlToken Close = new SqlToken(")", TokenKind.Close);
  public static SqlToken Dot = new SqlToken(".", TokenKind.Dot);

  public SqlToken(string value, TokenKind kind) : this(value, false, null, null,kind) {
  }

  public SqlToken(string value, bool quoted, string openQuote, string closeQuote, TokenKind kind) {
    this.Kind = kind;
    this.Value = value;
    this.WasQuoted = quoted;
    this.OpenQuote = openQuote;
    this.CloseQuote = closeQuote;
  }

  public bool IsEmpty() {
    return this.Equals(None);
  }

  public override string ToString() {
    return Value;
  }

  public bool IsKeyword(string text) {
    if (this.Kind != TokenKind.Identifier) return false;
    if (this.WasQuoted) return false;
    return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(Value, text);
  }

  public bool IsOperator(string text) {
    if (this.Kind != TokenKind.Operator) return false;
    if (this.WasQuoted) return false;
    return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(Value, text);
  }

  public override int GetHashCode() {
    int hash = 7;
    hash = 97 * hash + this.Kind.GetHashCode();
    hash = 97 * hash + this.Value.ToLower().GetHashCode();
    return hash;
  }

  public override bool Equals(Object obj) {
    if (obj is SqlToken) {
      SqlToken tk = (SqlToken) obj;
      return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(Value, tk.Value) & tk.Kind == this.Kind;
    }
    if (obj is string) {
      string str = (string) obj;
      return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(Value, str);
    }
    return false;
  }
  
  public bool IsParameter() {
    return this.Equals(SqlToken.AnonParam) || Kind == TokenKind.Parameter ||
            (Kind == TokenKind.Identifier && Value.StartsWith("@"));
  }

  public Object GetNativeValue() {
    if (this.Equals(Null)) return null;
    if (this.Kind == TokenKind.Bool) {
      if (Value == null) return false;
      else if (GetEqualsIgnoreCase("true", Value.Trim())) return true;
      else return false;
    }
    return Value;
  }

  private static bool GetEqualsIgnoreCase(string str1, string str2) {
    return RSSBus.core.j2cs.Converter.GetEqualsIgnoreCase(str1, str2);
  }
}
}

