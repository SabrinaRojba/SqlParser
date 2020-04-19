#ifndef _SQLTOKENIZER_H_
#define _SQLTOKENIZER_H_

#include "../../../core/native/encoding.h"

// Protocol Errors
#define ERR_SQL_TOKENIZER_PROTOCOL 40100
#define ERR_SQL_TOKENIZER_INPUT_READ_ERROR ERR_SQL_TOKENIZER_PROTOCOL + 1
#define ERR_SQL_TOKENIZER_EXPECTED_ENDQUOTE ERR_SQL_TOKENIZER_PROTOCOL + 2
#define ERR_SQL_TOKENIZER_CHARSET_ERROR ERR_SQL_TOKENIZER_PROTOCOL + 3
#define ERR_SQL_TOKENIZER_UNKNOWN_TOKEN ERR_SQL_TOKENIZER_PROTOCOL + 4
#define ERR_SQL_TOKENIZER_ENDED_BEFORE_PARAM1 ERR_SQL_TOKENIZER_PROTOCOL + 5
#define ERR_SQL_TOKENIZER_EXPECTED_TOKEN ERR_SQL_TOKENIZER_PROTOCOL + 6
#define ERR_SQL_TOKENIZER_EXPECTED_IDENTIFIER ERR_SQL_TOKENIZER_PROTOCOL + 7
#define ERR_SQL_TOKENIZER_BAD_CONVERTS ERR_SQL_TOKENIZER_PROTOCOL + 8

typedef enum _token_kind {
  TK_IDENTIFIER,
  TK_KEYWORD,
  TK_PARAMETER,
  TK_STR,
  TK_UNQUOTEDSTR,
  TK_NUMBER,
  TK_BOOL,
  TK_OPERATOR,
  TK_COMMENT,
  TK_OPEN,
  TK_CLOSE,
  TK_DOT,
  TK_NULL,
  TK_ESCINITIATOR,
  TK_ESCTERMINATOR,
} TOKEN_KIND;

typedef struct _sql_token {
  TOKEN_KIND tkKind;
  DynStr sValue;
  BOOL bWasQuoted;
  _sql_token(LPSTR lpszValue=NULL, TOKEN_KIND k=TK_NULL, BOOL q = FALSE) {
    tkKind = k;
    sValue.Set(lpszValue);
    bWasQuoted = q;
  }

  INT Set(LPSTR lpszValue = NULL, TOKEN_KIND k = TK_NULL, BOOL q = FALSE) {
    tkKind = k;
    sValue.Set(lpszValue);
    bWasQuoted = q;
    return 0;
  }

  INT Set(_sql_token* lpToken) {
    tkKind = lpToken->tkKind;
    sValue.Set(lpToken->sValue.Deref());
    bWasQuoted = lpToken->bWasQuoted;
    return 0;
  }

  BOOL Equals(_sql_token* lpToken) {
    return lpToken && sValue.Equals(lpToken->sValue.Deref()) && tkKind == lpToken->tkKind;
  }

  BOOL Equals(LPSTR lpText) {
    return lpText && sValue.EqualsNoCase(lpText);
  }

  BOOL IsKeyword(LPSTR lpText) {
    if (tkKind != TK_IDENTIFIER) return false;
    if (bWasQuoted) return false;
    return sValue.EqualsNoCase(lpText);
  }

  BOOL IsOperator(LPSTR lpText) {
    if (tkKind != TK_OPERATOR) return false;
    if (bWasQuoted) return false;
    return sValue.EqualsNoCase(lpText);
  }

  BOOL IsParameter() {
    if (tkKind == TK_PARAMETER) return true;
    if (sValue.StartsWith("@") && tkKind == TK_IDENTIFIER) return true;
    return false;
  }

} SQL_TOKEN;

typedef unsigned short UNI_CHAR;

SQL_TOKEN NONE_TOKEN("", TK_IDENTIFIER);
SQL_TOKEN NULL_TOKEN("NULL", TK_NULL);
SQL_TOKEN ANONPARAM_TOKEN("?", TK_PARAMETER);
SQL_TOKEN ESCINITIATO_TOKEN("{", TK_ESCINITIATOR);
SQL_TOKEN ESCTERMINATOR_TOKEN("}", TK_ESCTERMINATOR);
SQL_TOKEN OPEN_TOKEN("(", TK_OPEN);
SQL_TOKEN CLOSE_TOKEN(")", TK_CLOSE);
SQL_TOKEN DOT_TOKEN(".", TK_DOT);

char g_CHARSET_CONVERTS_BUFF[10];
int g_CHARSET_CONVERTS_LENGTH;

#define APPEND_UNI_CHAR(str, uni_char) { \
  g_CHARSET_CONVERTS_LENGTH = ucs2_to_utf8(&uni_char, g_CHARSET_CONVERTS_BUFF); \
  if (g_CHARSET_CONVERTS_LENGTH == 0) return ERR_SQL_TOKENIZER_CHARSET_ERROR; \
  if (ret_code = str.Append(g_CHARSET_CONVERTS_BUFF, g_CHARSET_CONVERTS_LENGTH)) return ret_code; \
}

BOOL SQLTK_IsWhitespace(unsigned short ch) {
  return (ch == ' ' || ch == '\f' || ch == '\n' || ch == '\r' || ch == '\t' || ch == '\v');
}

BOOL SQLTK_IsLetter(unsigned short ch) {
  // Uppercase letters
  if (ch >= 0x41 && ch <= 0x5A) return true;
  // Lowercase letters
  if (ch >= 0x61 && ch <= 0x7A) return true;

  return false;
}

BOOL SQLTK_IsDigit(unsigned short ch) {
  // 0123456789
  if (ch >= 0x30 && ch <= 0x39) return true;
  return false;
}

BOOL SQLTK_IsLetterOrDigit(unsigned short ch) {
  return SQLTK_IsLetter(ch) || SQLTK_IsDigit(ch);
}

class SqlTokenizer {
#define BUFFER_LENGTH 1024 * 100
  UNI_CHAR m_buffer[BUFFER_LENGTH];
  UNI_CHAR* m_lpszSQL;
  UNI_CHAR* m_lpszSQLEnd;
  UNI_CHAR* m_lpszSQLSeek;
  UNI_CHAR* m_lpszAnchor;
  int m_wLastErrorCode;
  SQL_TOKEN m_stLastToken;
  BOOL m_bLastTokenWasQuoted;
public:
  INT InitWithUTF8(LPSTR lpszSQL) {
    int chars = utf8string_to_utf16string(lpszSQL, (UNI_CHAR*)m_buffer);
    m_lpszAnchor = m_lpszSQLSeek = m_lpszSQL = m_buffer;
    m_lpszSQLEnd = m_lpszSQL + chars;
    return 0;
  }

  LPSTR NextTokenStr() {
    return NextToken()->sValue.Deref();
  }

  SQL_TOKEN* LastToken() {
    return &m_stLastToken;
  }

  BOOL LastTokenWasQuoted() {
    return m_bLastTokenWasQuoted;
  }

  UNI_CHAR* Save() {
    return m_lpszSQLSeek;
  }

  void Restore(UNI_CHAR* lpszSeek) {
    m_lpszSQLSeek = lpszSeek;
  }

  INT GetStatementText(DynStr& sText, UNI_CHAR* lpszSeek = NULL) {
    int ret_code;
    if (!lpszSeek) {
      lpszSeek = m_lpszSQLSeek;
    }

    UNI_CHAR sUTF16Part[BUFFER_LENGTH];
    mymemcpy(sUTF16Part, m_lpszAnchor, (char*)lpszSeek - (char*)m_lpszAnchor);
    *(sUTF16Part + (lpszSeek - m_lpszAnchor)) = 0;

    char lpszUTF8Part[BUFFER_LENGTH];
    utf16string_to_utf8string((unsigned short*)sUTF16Part, lpszUTF8Part);
    return sText.Append(lpszUTF8Part);
  }

  INT MarkStart() {
    SkipWhitespace();
    m_lpszAnchor = m_lpszSQLSeek;
    return 0;
  }

  SQL_TOKEN* NextIdentifier() {
    SQL_TOKEN* tk = NextToken();
    if (tk->Equals(&NONE_TOKEN) || tk->tkKind == TK_IDENTIFIER) {
      return tk;
    }

    m_wLastErrorCode = ERR_SQL_TOKENIZER_EXPECTED_IDENTIFIER;
    return &NULL_TOKEN;
  }

  INT EnsureNextIdentifier(LPSTR lpszToken) {
    SQL_TOKEN *lpNextToken = NextToken();
    if (lpNextToken->Equals(&NONE_TOKEN)) {
      return ERR_SQL_TOKENIZER_ENDED_BEFORE_PARAM1;
    }

    if (!lpNextToken->IsKeyword(lpszToken)) {
      return ERR_SQL_TOKENIZER_EXPECTED_TOKEN;
    }

    return 0;
  }

  INT EnsureNextToken(LPSTR lpszToken) {
    SQL_TOKEN *lpNextToken = NextToken();
    if (lpNextToken->Equals(&NONE_TOKEN)) {
      return ERR_SQL_TOKENIZER_ENDED_BEFORE_PARAM1;
    }

    if (!lpNextToken->Equals(lpszToken)) {
      return ERR_SQL_TOKENIZER_EXPECTED_TOKEN;
    }

    return 0;
  }

  SQL_TOKEN* NextToken() {
    int ret_code = 0;
    if (ret_code = SkipWhitespace()) goto error;
    if (IsEOF()) {
      if (ret_code = m_stLastToken.Set(&NONE_TOKEN)) goto error;
      m_bLastTokenWasQuoted = FALSE;
      return &NONE_TOKEN;
    }

    UNI_CHAR ch = PeekChar();
    if (ret_code = m_stLastToken.Set(&NONE_TOKEN)) goto error;
    m_bLastTokenWasQuoted = FALSE;
    if (ch == 'N') {
      NextChar();
      UNI_CHAR ch2 = PeekChar();
      if (ch2 == '\'') {
        ch = ch2;
      }
      else {
        Backtrack();
      }
    }

    if (ch == '?') {
      NextChar();
      if (ret_code = m_stLastToken.Set(&ANONPARAM_TOKEN)) goto error;
    }
    else if (IsIdentifierStartChar(ch)) {
      if (ret_code = ReadIdentifier(m_stLastToken)) goto error;
    }
    else if (IsStringChar(ch)) {
      if (ret_code = ReadString(m_stLastToken)) goto error;
    }
    else if (SQLTK_IsDigit(ch) || (ch == '-' && SQLTK_IsDigit(PeekNextChar())) || (ch == '.' && SQLTK_IsDigit(PeekNextChar()))) {
      // This could also be a date we read until space or comma
      if (ret_code = ReadNumberOrDate(m_stLastToken)) goto error;
    }
    else if (ch == '/' && PeekNextChar() == '*') {
      UNI_CHAR foo;
      if (ret_code = NextChar(foo)) goto error;
      if (ret_code = NextChar(foo)) goto error;
      if (ret_code = ReadMultiLineComment(m_stLastToken)) goto error;
      NextToken();
    }
    else if (ch == '-' && PeekNextChar() == '-'
      || ch == '/' && PeekNextChar() == '/') {
      UNI_CHAR foo;
      if (ret_code = NextChar(foo)) goto error;
      if (ret_code = NextChar(foo)) goto error;
      if (ret_code = ReadLineComment(m_stLastToken)) goto error;
      NextToken();
    }
    else if (IsOperatorChar(ch)) {
      if (ret_code = ReadOperator(m_stLastToken)) goto error;
    }
    else if (ch == '(') {
      NextChar();
      if (ret_code = m_stLastToken.Set("(", TK_OPEN)) goto error;
    }
    else if (ch == ')') {
      NextChar();
      if (ret_code = m_stLastToken.Set(")", TK_CLOSE)) goto error;
    }
    else if (ch == '.') {
      NextChar();
      if (ret_code = m_stLastToken.Set(".", TK_DOT)) goto error;
    }
    else if (ch == ';') {
      NextChar();
      if (ret_code = m_stLastToken.Set(&NONE_TOKEN)) goto error;
    }
    else if (ch == '{') {
      NextChar();
      if (ret_code = m_stLastToken.Set(&ESCINITIATO_TOKEN)) goto error;
    }
    else if (ch == '}') {
      NextChar();
      if (ret_code = m_stLastToken.Set(&ESCTERMINATOR_TOKEN)) goto error;
    }
    else {
      ret_code = ERR_SQL_TOKENIZER_UNKNOWN_TOKEN;
      goto error;
    }


    return &m_stLastToken;
  error:
    m_wLastErrorCode = ret_code;
    return &NULL_TOKEN;
  }

  SQL_TOKEN* LookaheadToken() {
    UNI_CHAR* lpszPos = Save();
    SQL_TOKEN* lpToken = NextToken();
    Restore(lpszPos);
    return lpToken;
  }

  BOOL IsEOF() {
    return m_lpszSQLSeek >= m_lpszSQLEnd;
  }

private:

  INT SkipWhitespace() {
    int ret_code = 0;
    UNI_CHAR ch;
    while (!IsEOF()) {
      if (ret_code = NextChar(ch)) return ret_code;
      if (!SQLTK_IsWhitespace(ch)) {
        Backtrack();
        break;
      }
    }

    return 0;
  }

  INT NextChar() {
    UNI_CHAR ch;
    return NextChar(ch);
  }

  INT NextChar(UNI_CHAR& ch) {
    if (IsEOF()) {
      return ERR_SQL_TOKENIZER_INPUT_READ_ERROR;
    }

    ch = *m_lpszSQLSeek++;
    return 0;
  }

  void Backtrack() {
    if (m_lpszSQLSeek > m_lpszSQL) {
      --m_lpszSQLSeek;
    }
  }

  void Backtrack(UNI_CHAR* lpszPos) {
    m_lpszSQLSeek = lpszPos;
  }

  INT ReadIdentifier(SQL_TOKEN& stToken) {
    int ret_code = 0;
    UNI_CHAR ch = PeekChar();
    BOOL readQuoted = false;
    if (ch == '[') {
      DynStr sTK;
      if (ret_code = ReadQuoted(sTK)) return ret_code;
      if (ret_code = stToken.Set(sTK.Deref(), TK_IDENTIFIER, true)) return ret_code;
      readQuoted = true;
    }
    else if (ch == '"') {
      DynStr sTK;
      if (ret_code = ReadDoubleQuoted(sTK)) return ret_code;
      if (ret_code = stToken.Set(sTK.Deref(), TK_IDENTIFIER, true)) return ret_code;
      readQuoted = true;
    }
    else if (ch == '`') {
      DynStr sTK;
      if (ret_code = ReadTickQuoted(sTK)) return ret_code;
      if (ret_code = stToken.Set(sTK.Deref(), TK_IDENTIFIER, true)) return ret_code;
      readQuoted = true;
    }
    else if (ch == '*') {
      // we special case this one
      UNI_CHAR ch;
      if (ret_code = NextChar(ch)) return ret_code;
      DynStr sTK;
      APPEND_UNI_CHAR(sTK, ch);
      if (ret_code = stToken.Set(sTK.Deref(), TK_IDENTIFIER, true)) return ret_code;
      readQuoted = true;
    }
    else {
      DynStr sToken;
      while (!IsEOF()) {
        if (ret_code = NextChar(ch)) return ret_code;
        if (SQLTK_IsWhitespace(ch)) {
          Backtrack(); // fixing bad tokenizing for `schema`.table and the space should be eaten in next advance
                       // see line 354
          break;
        }
        else if (ch == '.') {
          Backtrack();
          break;
        }
        else if (IsIdentifierDelimiter(ch)) {
          Backtrack();
          break;
        }

        APPEND_UNI_CHAR(sToken, ch);
      }

      if (sToken.Length() <= 0) {
        return stToken.Set(&NONE_TOKEN);
      }

      return stToken.Set(sToken.Deref(), GetTokenType(sToken.Deref()));
    }

    return 0;
  }

  INT ReadString(SQL_TOKEN& stToken) {
    int ret_code;
    UNI_CHAR ch = PeekChar();
    if (ch == '\'' || ch == '"') {
      DynStr sTK;
      if (ret_code = ReadQuoted(sTK)) return ret_code;
      return stToken.Set(sTK.Deref(), TK_STR);
    }

    DynStr sToken;
    while (!IsEOF()) {
      if (ret_code = NextChar(ch)) return ret_code;
      if (SQLTK_IsWhitespace(ch)) {
        break;
      }
      if (IsStringDelimiter(ch)) {
        Backtrack();
        break;
      }

      APPEND_UNI_CHAR(sToken, ch);
    }

    return sToken.Length() > 0
      ? stToken.Set(sToken.Deref(), TK_STR)
      : stToken.Set(&NONE_TOKEN);
  }

  INT ReadNumberOrDate(SQL_TOKEN& stToken) {
    int ret_code;
    if (ret_code = stToken.Set(&NONE_TOKEN)) return ret_code;
    DynStr sToken;
    boolean digital = true;
    boolean exponential = false;
    UNI_CHAR ch;
    while (!IsEOF()) {
      if (ret_code = NextChar(ch)) return ret_code;
      if (sToken.Length() > 0 && digital && (ch == 'E' || ch == 'e')) {
        exponential = true;
      }
      if (!SQLTK_IsDigit(ch) && ch != '.') {
        digital = false;
      }

      boolean signOfExponent = exponential && (ch == '+' || ch == '-');
      if (ch == ',' || ch == ')' || ch == '=' || ch == ';' || (sToken.Length() > 0 && ch != '.' && !SQLTK_IsLetterOrDigit(ch) && !signOfExponent)) {
        Backtrack();
        break;
      }

      if (SQLTK_IsWhitespace(ch)) {
        break;
      }

      APPEND_UNI_CHAR(sToken, ch);
    }

    if (sToken.Length() > 0) {
      DynStr sTokenStr;
      if (ret_code = sTokenStr.Set(sToken.Deref())) return ret_code;
      double floatVal = 0;
      BOOL isHex = FALSE;
      long longVal;
      if (sTokenStr.StartsWith("0x")) {
        longVal = myhextol(sTokenStr.Deref() + 2);
        isHex = TRUE;
      }
      else {
        floatVal = myatof(sTokenStr.Deref());
      }

      if (sTokenStr.StartsWith(".") && !isHex) {
        DynStr s;
        if (ret_code = s.AppendDouble(floatVal)) return ret_code;
        if (ret_code = stToken.Set(s.Deref(), TK_NUMBER)) return ret_code;
      }
      else {
        if (ret_code = stToken.Set(sTokenStr.Deref(), TK_NUMBER)) return ret_code;
      }
    }

    return 0;
  }

  INT ReadMultiLineComment(SQL_TOKEN& stToken) {
    int ret_code;
    if (ret_code = stToken.Set(&NONE_TOKEN)) return ret_code;
    DynStr sToken;
    while (!IsEOF()) {
      UNI_CHAR ch;
      if (ret_code = NextChar(ch)) return ret_code;
      if (ch == '*' && PeekChar() == '/') {
        NextChar(); // move past / 
        break;
      }

      APPEND_UNI_CHAR(sToken, ch);
    }

    if (sToken.Length() > 0) {
      return sToken.Set(sToken.Deref(), TK_COMMENT);
    }

    return 0;
  }

  INT ReadLineComment(SQL_TOKEN& stToken) {
    int ret_code;
    if (ret_code = stToken.Set(&NONE_TOKEN)) return ret_code;
    DynStr sToken;
    while (!IsEOF()) {
      UNI_CHAR ch;
      if (ret_code = NextChar(ch)) return ret_code;
      if (ch == '\r' && PeekChar() == '\n') {
        NextChar(); // move past /
        break;
      }
      else if (ch == '\n' || ch == '\r') {
        break;
      }
      
      APPEND_UNI_CHAR(sToken, ch);
    }

    if (sToken.Length() > 0) {
      return stToken.Set(sToken.Deref(), TK_COMMENT);
    }

    return 0;
  }

  INT ReadNumber(SQL_TOKEN& stToken) {
    DynStr sToken;
    int ret_code;
    while (!IsEOF()) {
      UNI_CHAR ch;
      if (ret_code = NextChar(ch)) return ret_code;
      if (SQLTK_IsWhitespace(ch)) {
        break;
      }
      if (SQLTK_IsDigit(ch) || ch == '.' || ch == '-') {
        APPEND_UNI_CHAR(sToken, ch);
      }
      else {
        Backtrack();
        break;
      }
    }

    return sToken.Length() > 0
      ? stToken.Set(sToken.Deref(), TK_NUMBER)
      : stToken.Set(&NONE_TOKEN);
  }

  INT ReadOperator(SQL_TOKEN& stToken) {
    // operators are:
    // (, ), =, !=, <, >, <>, <=>
    if (IsEOF()) {
      stToken.Set(&NONE_TOKEN);
      return 0;
    }

    int ret_code = 0;
    UNI_CHAR op1;
    if (ret_code = NextChar(op1)) return ret_code;
    if ((op1 == '!' || op1 == '<' || op1 == '>' || op1 == '=') & !IsEOF()) {
      UNI_CHAR op2;
      if (ret_code = NextChar(op2)) return ret_code;
      if (op1 == '!' && op2 == '=') return stToken.Set("!=", TK_OPERATOR);
      if (op1 == '<' && op2 == '>') return stToken.Set("<>", TK_OPERATOR);
      if (op1 == '<' && op2 == '=') {
        UNI_CHAR* p = m_lpszSQLSeek;
        UNI_CHAR op3;
        if (ret_code = NextChar(op3)) return ret_code;
        if (op3 == '>') {
          return stToken.Set("<=>", TK_OPERATOR);
        }
        else {
          Backtrack(p);
          return stToken.Set("<=", TK_OPERATOR);
        }
      }
      if (op1 == '>' && op2 == '=') return stToken.Set(">=", TK_OPERATOR);
      if (op1 == '=' && op2 == '=') return stToken.Set("==", TK_OPERATOR);
      Backtrack();
    }

    DynStr sToken;
    APPEND_UNI_CHAR(sToken, op1);
    return stToken.Set(sToken.Deref(), TK_OPERATOR);
  }

  INT ReadDoubleQuoted(DynStr& sToken) {
    UNI_CHAR ch;
    int ret_code;
    if (ret_code = NextChar(ch)) return ret_code;
    UNI_CHAR closeQuote = '"';
    m_bLastTokenWasQuoted = true;
    return ReadUntil(closeQuote, sToken);
  }

  INT ReadTickQuoted(DynStr& sToken) {
    UNI_CHAR ch;
    int ret_code;
    if (ret_code = NextChar(ch)) return ret_code;
    UNI_CHAR closeQuote = '`';
    m_bLastTokenWasQuoted = true;
    return ReadUntil(closeQuote, sToken);
  }

  INT ReadQuoted(DynStr& sToken) {
    UNI_CHAR ch;
    int ret_code;
    if (ret_code = NextChar(ch)) return ret_code;
    UNI_CHAR closeQuote = ']';
    if (ch == '"' || ch == '\'') {
      closeQuote = ch;
    }

    m_bLastTokenWasQuoted = TRUE;
    return ReadUntil(closeQuote, sToken);
  }

  INT ReadUntil(UNI_CHAR delim, DynStr& sToken) {
    int ret_code = 0;
    BOOL foundDelim = FALSE;
    BOOL seenEscape = FALSE;
    BOOL escapeNextQuote = FALSE;
    sToken.Reset();
    while (!IsEOF()) {
      UNI_CHAR ch;
      if (ret_code = NextChar(ch)) return ret_code;
      if (seenEscape) {
        APPEND_UNI_CHAR(sToken, ch);
        seenEscape = FALSE;
      }
      else {
        if (ch == delim) {
          if (delim == '\'' && !IsEOF() && (PeekChar() == '\'' || escapeNextQuote)) {
            if (!escapeNextQuote) {
              APPEND_UNI_CHAR(sToken, ch);
            }
            escapeNextQuote = !escapeNextQuote;
            continue;
          }
          else {
            foundDelim = true;
            break;
          }
        }
        if (ch == '\\') {
          seenEscape = true;
        }
        else {
          APPEND_UNI_CHAR(sToken, ch);
        }
      }
    }

    if (!foundDelim) {
      return ERR_SQL_TOKENIZER_EXPECTED_ENDQUOTE;
    }

    return 0;
  }

  UNI_CHAR PeekChar() {
    UNI_CHAR ch;
    return *m_lpszSQLSeek;
    return ch;
  }

  BOOL IsStringDelimiter(UNI_CHAR ch) {
    if (SQLTK_IsLetterOrDigit(ch)) return false;
    if (IsStringChar(ch)) return false;
    return true;
  }

  BOOL IsIdentifierDelimiter(UNI_CHAR ch) {
    // return IsIdentifierChar(ch); TBD - Why are IsIdentifierDelimiter and IsIdentifierChar not exactly opposite? -Amit 
    if (SQLTK_IsLetterOrDigit(ch)) return false;
    //return "$@:_-#.*".indexOf(ch) < 0;
    return ch != '$' && ch != '@' && ch != ':' && ch != '#' && ch != '_';
  }

  BOOL IsIdentifierStartChar(UNI_CHAR ch) {
    if (SQLTK_IsLetter(ch)) return true;
    if (ch == '"') return true;
    return ch == '$' || ch == '@' || ch == ':' || ch == '_' || ch == '[' || ch == '*' || ch == '`';
  }

  BOOL IsStringChar(UNI_CHAR ch) {
    if (SQLTK_IsLetter(ch)) return true;
    if (ch == '#') return true;
    return (ch == '\'' || ch == '"');
  }

  BOOL IsOperatorChar(char ch) {
    return ch == '!' || ch == '<' || ch == '=' || ch == '>' || ch == '*' || ch == ',' || ch == '+' || ch == '-' || ch == '%' || ch == '/';
  }

  UNI_CHAR PeekNextChar() {
    NextChar();
    UNI_CHAR ch = PeekChar();
    Backtrack();
    return ch;
  }

  TOKEN_KIND GetTokenType(LPSTR lpszToken) {
    switch (*lpszToken) {
    case '@':
      return TK_PARAMETER;
    case 'C':
      if (mystrcmpi("CROSS", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("COLLATE", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'D':
      if (mystrcmpi("DISTINCT", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("DESC", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'E':
      if (mystrcmpi("EXCEPT", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("EXISTS", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'F':
      if (mystrcmpi("FROM", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("FALSE", lpszToken) == 0) {
        return TK_BOOL;
      }
      return TK_IDENTIFIER;
    case 'G':
      if (mystrcmpi("GROUP", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'H':
      if (mystrcmpi("HAVING", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'I':
      if (mystrcmpi("INNER", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("INTERSECT", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("IS", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("INTO", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'J':
      if (mystrcmpi("JOIN", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'L':
      if (mystrcmpi("LIMIT", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("LIKE", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'M':
      if (mystrcmpi("MINUS", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'N':
      if (mystrcmpi("NOT", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("NATURAL", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("NULL", lpszToken) == 0) {
        return TK_NULL;
      }
      return TK_IDENTIFIER;
    case 'O':
      if (mystrcmpi("ON", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("OFFSET", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      else if (mystrcmpi("ORDER", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'S':
      if (mystrcmpi("SELECT", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'T':
      if (mystrcmpi("TRUE", lpszToken) == 0) {
        return TK_BOOL;
      }
      return TK_IDENTIFIER;
    case 'U':
      if (mystrcmpi("UNION", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    case 'W':
      if (mystrcmpi("WHERE", lpszToken) == 0) {
        return TK_KEYWORD;
      }
      return TK_IDENTIFIER;
    default:
      return TK_IDENTIFIER;
    }
  }
};

#endif //_SQLTOKENIZER_H_


