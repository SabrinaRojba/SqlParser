using System;
using System.Collections;
using System.IO;
using System.Threading;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RSSBus.core.j2cs;

namespace RSSBus.core {

using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Globalization;


public   class SqlTokenizerImpl {
  private static string[] EXPR_OPERATORS = new string[]{"+", "-", "*", "/", ">", "<", "==", ">=", "<=", "!="};
  private static CultureInfo InvariantCulture = CultureInfo.InvariantCulture;
  private string inputText;
  private int currentPos;
  internal protected SqlToken lastToken;
  internal protected bool lastTokenWasQuoted;
  private int lastStatementStart;
  private readonly char escapeChar;

  public SqlTokenizerImpl(string text) : this(text, '\\') {
  }

  public SqlTokenizerImpl(string text, char escapeChar) {
    inputText = text;
    currentPos = 0;
    lastStatementStart = 0;
    lastToken = SqlToken.None;
    this.escapeChar = escapeChar;
  }

  // these are only used for the unit tests
  public string NextTokenStr() {
    return NextToken().Value;
  }

  public string LastTokenStr() {
    return LastToken().Value;
  }

  public SqlToken LastToken() {
    return lastToken;
  }

  public bool LastTokenWasQuoted() {
    return lastTokenWasQuoted;
  }

  public string GetInputText() {
    return inputText;
  }

  public string GetStatementText() {
    int end = currentPos;
    if (end > inputText.Length) end--;
    string stmt = RSSBus.core.j2cs.Converter.GetSubstring(inputText, lastStatementStart, end);

    
    
    stmt = stmt.Trim(' ', '\t', '\r', '\n', ';'); 
    
    return stmt;
  }

  public void MarkStart() {
    SkipWhitespace();
    lastStatementStart = currentPos;
  }

  public int CurrentPosition() {
    return currentPos;
  }

  public SqlToken NextIdentifier() {
    SqlToken tk = NextToken();
    if (tk.IsEmpty() || tk.Kind == TokenKind.Identifier) {
      return tk;
    }
    throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_IDENTIFIER, tk.Kind, tk.Value, CurrentPosition()));
  }

  public void EnsureNextIdentifier(string identifier) {
    SqlToken tk = NextToken();
    if (tk.IsEmpty()) {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_PARAM1, identifier));
    }
    if (!tk.IsKeyword(identifier)) {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_IDENTIFIER, tk.Kind, tk.Value, CurrentPosition()));
    }
  }

  public void EnsureNextToken(string token) {
    SqlToken tk = NextToken();
    if (tk.IsEmpty()) {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.ENDED_BEFORE_PARAM1, token));
    }
    if (!tk.Equals(token)) {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_TOKEN, token, tk.Kind, tk.Value, CurrentPosition()));
    }
  }

  public virtual SqlToken NextToken() {
    SkipWhitespace();
    if (EOF()) {
      lastToken = SqlToken.None;
      lastTokenWasQuoted = false;
      return SqlToken.None;
    }

    char ch = PeekChar();
    lastToken = SqlToken.None;
    lastTokenWasQuoted = false;
    if (ch == 'N') {
      NextChar();
      char ch2 = PeekChar();
      if (ch2 == '\'') {
        ch = ch2;
      } else {
        Backtrack();
      }
    }

    if (ch == '?') {
      NextChar();
      lastToken = SqlToken.AnonParam;
    } else if (IsIdentifierStartChar(ch)) {
      lastToken = ReadIdentifier();
    } else if (IsStringChar(ch)) {
      lastToken = ReadString();
    } else if (char.IsDigit(ch) || (ch == '-' && char.IsDigit(PeekNextChar())) || (ch == '.' && char.IsDigit(PeekNextChar()))) {
      // This could also be a date we read until space or comma
      lastToken = ReadNumberOrDate();
    } else if (ch == '/' && PeekNextChar() == '*') {
      char foo = NextChar();
      foo = NextChar();
      lastToken = ReadMultiLineComment();
      lastToken = NextToken();
    } else if (ch == '-' && PeekNextChar() == '-'
            || ch == '/' && PeekNextChar() == '/') {
      char foo = NextChar();
      foo = NextChar();
      lastToken = ReadLineComment();
      lastToken = NextToken();
    } else if (IsOperatorChar(ch)) {
      lastToken = ReadOperator();
    } else if (ch == '(') {
      NextChar();
      lastToken = SqlToken.Open;
    } else if (ch == ')') {
      NextChar();
      lastToken = SqlToken.Close;
    } else if (ch == '.') {
      NextChar();
      lastToken = SqlToken.Dot;
    } else if (ch == ';') {
      NextChar();
      lastToken = SqlToken.None;
    } else if (ch == '{') {
      NextChar();
      lastToken = SqlToken.ESCInitiator;
    } else if (ch == '}') {
      NextChar();
      lastToken = SqlToken.ESCTerminator;
    } else {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.UNKNOWN_TOKEN, ch, currentPos));
    }
    return lastToken;
  }

  public SqlToken LookaheadToken2() {
    int pos = currentPos;
    SqlToken lastToken2 = lastToken;
    SqlToken next = NextToken();
    currentPos = pos;
    lastToken = lastToken2;
    return next;
  }

  private SqlToken ReadExprOperator() {
    SqlToken nextToken = MatchExprOperator(currentPos);
    if (nextToken != SqlToken.None) {
      lastToken = nextToken;
      currentPos += nextToken.Value.Length;
      return lastToken;
    }
    return SqlToken.None;
  }

  private SqlToken MatchExprOperator(int fromIndex) {
    foreach(String op in EXPR_OPERATORS) {
      if (fromIndex < inputText.Length && RSSBus.core.j2cs.Converter.GetSubstring(inputText, fromIndex).StartsWith(op)) {
        return new SqlToken(op, TokenKind.Operator);
      }
    }
    return SqlToken.None;
  }

  // very hackish, but might be enough for now
  internal static bool IsText(string name) {
    for (int i = 0; i < name.Length; i++) {
      if (char.IsWhiteSpace(name[i])) {
        return true;
      }
    }
    return false;
  }

  public static string Quote(SqlValue expr) {
    return Quote(expr, RebuildOptions.SQLite);
  }

  public static string Quote(SqlValue expr, RebuildOptions rebuildOptions) {
    if (expr != null) {
      if (expr.GetValueType() == SqlValueType.DATETIME) {
        return "'" + expr.GetValueAsString(null) + "'";
      } else if (expr.GetValueType() == SqlValueType.STRING) {
        string str = expr.GetValueAsString("");
        ByteBuffer buf = new ByteBuffer("'");
        for (int i = 0; i < str.Length; i++) {
          char ch = str[i];
          if (ch == '\\') {
            buf.Append('\\');
          } else if (ch == '\'') {
            buf.Append('\'');
          }
          buf.Append(ch);
        }
        buf.Append("'");
        return buf.ToString();
      } else {
        return expr.GetValueAsString("");
      }
    }
    return null;
  }

  public static string Quote(SqlToken token) {
    return Quote(token, RebuildOptions.SQLite);
  }

  public static string Quote(SqlToken token, RebuildOptions rebuildOptions) {
    switch (token.Kind) {
      case TokenKind. Identifier:
        return Bracket(token.Value, rebuildOptions);
      case TokenKind. Number:
      case TokenKind. Null:
      case TokenKind. Bool:
      case TokenKind. UnquotedStr:
        return token.Value;
    }
    string str = token.Value;
    ByteBuffer buf = new ByteBuffer("'");
    for (int i = 0; i < str.Length; i++) {
      char ch = str[i];
      if (ch == '\\') {
        buf.Append('\\');
      } else if (ch == '\'') {
        buf.Append('\'');
      }
      buf.Append(ch);
    }
    buf.Append("'");
    return buf.ToString();
  }

  internal static string Bracket(string str) {
    return Bracket(str, RebuildOptions.SQLite);
  }

  internal static string Bracket(string str, RebuildOptions rebuildOptions) {
    string name = str.Trim();
    if (name != "*") {
      return rebuildOptions.QuoteIdentifier(name);
    }
    return name;
  }

  private void SkipWhitespace() {
    while (!EOF()) {
      char ch = NextChar();
      if (!char.IsWhiteSpace(ch) && (ch != '\0')) {
        Backtrack();
        break;
      }
    }
  }

  private SqlToken ReadIdentifier() {
    char ch = PeekChar();
    bool readQuoted = false;
    SqlToken first = null;
    if (ch == '[') {
      first = new SqlToken(ReadQuoted(), true, "[", "]", TokenKind.Identifier);
      readQuoted = true;
    } else if (ch == '"') {
      first = new SqlToken(ReadDoubleQuoted(), true, "\"", "\"", TokenKind.Identifier);
      readQuoted = true;
    } else if (ch == '`') {
      first = new SqlToken(ReadTickQuoted(), true, "`", "`", TokenKind.Identifier);
      readQuoted = true;
    } else if (ch == '*') {
      // we special case this one
      first = new SqlToken("" + NextChar(), TokenKind.Identifier);
      readQuoted = true;
    } else {
      ByteBuffer token = new ByteBuffer();
      string subName = "";
      while (!EOF()) {
        ch = NextChar();
        if (char.IsWhiteSpace(ch)) {
          Backtrack(); // fixing bad tokenizing for `schema`.table and the space should be eaten in next advance
          // see line 354
          break;
        } else if (ch == '.') {
          Backtrack();
          break;
        } else if (IsIdentifierDelimiter(ch)) {
          Backtrack();
          break;
        }
        token.Append(ch);
      }
      if (token.Length <= 0) first = SqlToken.None;
      string token_str = token.ToString();
      first = new SqlToken(token_str, GetTokenType(token_str.ToUpper()));
    }
    return first;
  }

  private SqlToken ReadString() {
    char ch = PeekChar();
    if (ch == '\'') {
      return new SqlToken(ReadQuoted(), true, "'", "'", TokenKind.Str);
    } else if (ch == '"') {
      return new SqlToken(ReadQuoted(), true, "\"", "\"", TokenKind.Str);
    }
    ByteBuffer token = new ByteBuffer();
    while (!EOF()) {
      ch = NextChar();
      if (char.IsWhiteSpace(ch)) {
        break;
      }
      if (IsStringDelimiter(ch)) {
        Backtrack();
        break;
      }
      token.Append(ch);
    }
    return token.Length > 0
            ? new SqlToken(token.ToString(), TokenKind.Str)
            : SqlToken.None;
  }

  public virtual SqlToken ReadNumberOrDate() {
    SqlToken retVal = SqlToken.None;
    ByteBuffer token = new ByteBuffer();
    bool digital = true;
    bool exponential = false;
    while (!EOF()) {
      char ch = NextChar();
      if (token.Length > 0 && digital && (ch == 'E' || ch == 'e')) {
        exponential = true;
      }
      if (!char.IsDigit(ch) && ch != '.' && token.Length > 0) {
        digital = false;
      }

      bool signOfExponent = exponential && (ch == '+' || ch == '-');
      if (ch == ',' || ch == ')' || ch == '=' || ch == ';' || (token.Length > 0 && ch != '.' && !char.IsLetterOrDigit(ch) && !signOfExponent)) {
        Backtrack();
        break;
      }

      if (char.IsWhiteSpace(ch)) {
        break;
      }
      token.Append(ch);
    }

    if (token.Length > 0) {
      string tokenStr = token.ToString();
      double floatVal = 0;
      long longVal;
      try {
        if (tokenStr.StartsWith("0x")) {
          longVal = Utilities.ToIntFromHex(RSSBus.core.j2cs.Converter.GetSubstring(tokenStr, 2));
        } else {
          floatVal = Utilities.GetValueAsDouble2(tokenStr, 0, InvariantCulture); // CultureInfo is used here to maintain previous functionality that was within SQLParserUtil which just called double.TryParse(value, out)
        }
        if (tokenStr.StartsWith(".") && floatVal != 0) {
          string strFloat;


          IFormatProvider nf = InvariantCulture.NumberFormat;
          strFloat = ((IFormattable)floatVal).ToString(null, nf);

          retVal = new SqlToken(strFloat, TokenKind.Number);
        } else {
          retVal = new SqlToken(tokenStr, TokenKind.Number);
        }
      } catch (Exception e) {
        //Parse double fails, it's not a number
        //String with no quotes around it. It sometimes appears for dates.
        retVal = new SqlToken(token.ToString(), TokenKind.Str);
      }
    }
    return retVal;
  }

  private SqlToken ReadMultiLineComment() {
    SqlToken retVal = SqlToken.None;
    ByteBuffer token = new ByteBuffer();
    while (!EOF()) {
      char ch = NextChar();
      if (ch == '*' && PeekChar() == '/') {
        NextChar(); // move past / 
        break;
      }
      token.Append(ch);
    }
    if (token.Length > 0) {
      retVal = new SqlToken(token.ToString(), TokenKind.Comment);
    }
    return retVal;
  }

  private SqlToken ReadLineComment() {
    SqlToken retVal = SqlToken.None;
    ByteBuffer token = new ByteBuffer();
    while (!EOF()) {
      char ch = NextChar();
      if (ch == '\r' && PeekChar() == '\n') {
        NextChar(); // move past /
        break;
      } else if (ch == '\n' || ch == '\r') {
        break;
      }
      token.Append(ch);
    }
    if (token.Length > 0) {
      retVal = new SqlToken(token.ToString(), TokenKind.Comment);
    }
    return retVal;
  }

  private SqlToken ReadNumber() {
    ByteBuffer token = new ByteBuffer();
    while (!EOF()) {
      char ch = NextChar();
      if (char.IsWhiteSpace(ch)) {
        break;
      }
      if (char.IsDigit(ch) || ch == '.' || ch == '-') {
        token.Append(ch);
      } else {
        Backtrack();
        break;
      }
    }
    return token.Length > 0
            ? new SqlToken(token.ToString(), TokenKind.Number)
            : SqlToken.None;
  }

  private SqlToken ReadOperator() {
    // operators are:
    // (, ), =, !=, <, >, <>, <=>
    if (EOF()) {
      return SqlToken.None;
    }

    char op1 = NextChar();
    if ((op1 == '!' || op1 == '<' || op1 == '>' || op1 == '=' || op1 == '|') & !EOF()) {
      char op2 = NextChar();
      if (op1 == '|' && op2 == '|') return new SqlToken("" + op1 + op2, TokenKind.Operator);
      if (op1 == '!' && op2 == '=') return new SqlToken("" + op1 + op2, TokenKind.Operator);
      if (op1 == '<' && op2 == '>') return new SqlToken("" + op1 + op2, TokenKind.Operator);
      if (op1 == '<' && op2 == '=') {
        int p = this.CurrentPosition();
        char op3 = NextChar();
        if (op3 == '>') {
          return new SqlToken("" + op1 + op2 + op3, TokenKind.Operator);
        } else {
          this.Backtrack(p);
          return new SqlToken("" + op1 + op2, TokenKind.Operator);
        }
      }
      if (op1 == '>' && op2 == '=') return new SqlToken("" + op1 + op2, TokenKind.Operator);
      if (op1 == '=' && op2 == '=') return new SqlToken("" + op1 + op2, TokenKind.Operator);
      Backtrack();
    }
    return new SqlToken("" + op1, TokenKind.Operator);
  }

  private string ReadDoubleQuoted() {
    char ch = NextChar();
    char closeQuote = '"';
    lastTokenWasQuoted = true;
    return ReadUntil(closeQuote);
  }

  private string ReadTickQuoted() {
    char ch = NextChar();
    char closeQuote = '`';
    lastTokenWasQuoted = true;
    return ReadUntil(closeQuote);
  }

  private string ReadQuoted() {
    char ch = NextChar();
    char closeQuote = ']';
    if (ch == '"' || ch == '\'') {
      closeQuote = ch;
    }
    lastTokenWasQuoted = true;
    return ReadUntil(closeQuote);
  }

  private string ReadUntil(char delim) {
    ByteBuffer token = new ByteBuffer();
    int pos = currentPos - 1;
    bool foundDelim = false;
    bool seenEscape = false;
    bool escapeNextQuote = false;
    while (!EOF()) {
      char ch = NextChar();
      if (seenEscape) {
        token.Append(ch);
        seenEscape = false;
      } else {
        if (ch == delim) {
          if (!EOF() && (PeekChar() == delim || escapeNextQuote)) {
            if (!escapeNextQuote) token.Append(ch);
            escapeNextQuote = !escapeNextQuote;
            continue;
          } else {
            foundDelim = true;
            break;
          }
        }
        if (ch == this.escapeChar) {
          seenEscape = true;
        } else {
          token.Append(ch);
        }
      }
    }
    if (!foundDelim) {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.EXPECTED_ENDQUOTE, delim, pos));
    }
    return token.ToString();
  }

  private char PeekChar() {
    return inputText[currentPos];
  }

  private bool IsStringDelimiter(char ch) {
    if (char.IsLetterOrDigit(ch)) return false;
    if (IsStringChar(ch)) return false;
    return true;
  }

  private bool IsIdentifierDelimiter(char ch) {
    // return IsIdentifierChar(ch); TBD - Why are IsIdentifierDelimiter and IsIdentifierChar not exactly opposite? -Amit 
    if (char.IsLetterOrDigit(ch)) return false;
    //return "$@:_-#.*".indexOf(ch) < 0;
    return "$@:#_".IndexOf(ch) < 0;
  }

  private bool IsIdentifierStartChar(char ch) {
    if (char.IsLetter(ch)) return true;
    if (ch == '"') return true;
    return "$@:_[*`".IndexOf(ch) >= 0;
  }

  private bool IsStringChar(char ch) {
    if (char.IsLetter(ch)) return true;
    if (ch == '#') return true;
    return (ch == '\'' || ch == '"');
  }

  private bool IsOperatorChar(char ch) {
    return "!<=>*,+-%/|".IndexOf(ch) >= 0;
  }

  public bool EOF() {
    return currentPos >= inputText.Length;
  }

  private char PeekNextChar() {
    char ch = NextChar();
    ch = PeekChar();
    Backtrack();
    return ch;
  }

  public char NextChar() {
    if (EOF()) {
      throw MalformedSql(SqlExceptions.LocalizedMessage(SqlExceptions.INPUT_READ_ERROR));
    }
    return inputText[currentPos++];
  }

  internal protected void Backtrack() {
    currentPos--;
  }

  public void Backtrack(int pos) {
    currentPos = pos;
  }

  public Exception MalformedSql(string message) {
    throw SqlExceptions.Exception("SqlException", SqlExceptions.MALFORMED_SQL, message, this.inputText);
  }

  private static TokenKind GetTokenType(string token) {
    switch (token[0]) {
      case '@':
        return TokenKind.Parameter;
      case 'C':
        if (token.Equals("CROSS")) {
          return TokenKind.Keyword;
        } else if (token.Equals("COLLATE")) {
          return TokenKind.Keyword;
        } else if (token.Equals("CURRENT_TIMESTAMP")) {
          return TokenKind.Keyword;
        } else if (token.Equals("CURRENT_TIME")) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'D':
        if (token.Equals("DISTINCT")) {
          return TokenKind.Keyword;
        } else if (token.Equals("DESC")) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'E':
        if ("EXCEPT".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("EXISTS".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'F':
        if ("FROM".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("FALSE".Equals(token)) {
          return TokenKind.Bool;
        }
        return TokenKind.Identifier;
      case 'G':
        if ("GROUP".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'H':
        if ("HAVING".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'I':
        if ("INNER".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("INTERSECT".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("IS".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("INTO".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'J':
        if ("JOIN".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'L':
        if ("LIMIT".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("LIKE".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'M':
        if ("MINUS".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'N':
        if ("NOT".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("NATURAL".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("NULL".Equals(token)) {
          return TokenKind.Null;
        }
        return TokenKind.Identifier;
      case 'O':
        if ("ON".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("OFFSET".Equals(token)) {
          return TokenKind.Keyword;
        } else if ("ORDER".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'S':
        if ("SELECT".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'T':
        if ("TRUE".Equals(token)) {
          return TokenKind.Bool;
        }
        return TokenKind.Identifier;
      case 'U':
        if ("UNION".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      case 'W':
        if ("WHERE".Equals(token)) {
          return TokenKind.Keyword;
        }
        return TokenKind.Identifier;
      default:
        return TokenKind.Identifier;
    }
  }
}
}

