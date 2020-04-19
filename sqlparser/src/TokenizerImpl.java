package core;
/*#
using RSSBus.core;
using RSSBus;
using CData.Sql;
using System.Globalization;
#*/

import cdata.sql.RebuildOptions;
import cdata.sql.SqlToken;
import cdata.sql.SqlValue;
import cdata.sql.SqlValueType;
import cdata.sql.TokenKind;
import rssbus.oputils.common.Utilities;
import java.text.NumberFormat;
import java.util.Locale;

/*#public #*/ /*@*/public /*@*/ class SqlTokenizerImpl {
    private static final String[] EXPR_OPERATORS = new String[]{"+", "-", "*", "/", ">", "<", "==", ">=", "<=", "!="};
    private static final /*#CultureInfo#*//*@*/ Locale/*@*/ InvariantCulture = /*#CultureInfo.InvariantCulture#*//*@*/Locale.US/*@*/;
    private String inputText;
    private int currentPos;
    protected SqlToken lastToken;
    protected boolean lastTokenWasQuoted;
    private int lastStatementStart;
    private final char escapeChar;

    public SqlTokenizerImpl(String text) {
        this(text, '\\');
    }

    public SqlTokenizerImpl(String text, Character escapeChar) {
        inputText = text;
        currentPos = 0;
        lastStatementStart = 0;
        lastToken = SqlToken.None;
        this.escapeChar = escapeChar;
    }

    // these are only used for the unit tests
    public String NextTokenStr() throws Exception {
        return NextToken().Value;
    }

    public String LastTokenStr() {
        return LastToken().Value;
    }

    public SqlToken LastToken() {
        return lastToken;
    }

    public boolean LastTokenWasQuoted() {
        return lastTokenWasQuoted;
    }

    public String getInputText() {
        return inputText;
    }

    public String GetStatementText() {
        int end = currentPos;
        if (end > inputText.length()) end--;
        String stmt = inputText.substring(lastStatementStart, end);

        //@
        //Implement return stmt.Trim(' ', '\t', '\r', '\n', ';') in CSharp version
        String tmp;
        do {
            tmp = stmt;
            stmt = stmt.replaceAll("^ |^;|^\t|^\r|^\n| $|;$|\t$|\r$|\n$", "");
        } while (!stmt.equals(tmp));
        //@
    /*#
    stmt = stmt.Trim(' ', '\t', '\r', '\n', ';');
    #*/
        return stmt;
    }

    public void MarkStart() throws Exception {
        SkipWhitespace();
        lastStatementStart = currentPos;
    }

    public int currentPosition() throws Exception {
        return currentPos;
    }

    public SqlToken NextIdentifier() throws Exception {
        SqlToken tk = NextToken();
        if (tk.IsEmpty() || tk.Kind == TokenKind.Identifier) {
            return tk;
        }
        throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_IDENTIFIER, tk.Kind, tk.Value, currentPosition()));
    }

    public void EnsureNextIdentifier(String identifier) throws Exception {
        SqlToken tk = NextToken();
        if (tk.IsEmpty()) {
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_PARAM1, identifier));
        }
        if (!tk.IsKeyword(identifier)) {
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_IDENTIFIER, tk.Kind, tk.Value, currentPosition()));
        }
    }

    public void EnsureNextToken(String token) throws Exception {
        SqlToken tk = NextToken();
        if (tk.IsEmpty()) {
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.ENDED_BEFORE_PARAM1, token));
        }
        if (!tk.equals(token)) {
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_TOKEN, token, tk.Kind, tk.Value, currentPosition()));
        }
    }

    public /*#virtual#*/ SqlToken NextToken() throws Exception {
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
        } else if (Character.isDigit(ch) || (ch == '-' && Character.isDigit(PeekNextChar())) || (ch == '.' && Character.isDigit(PeekNextChar()))) {
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
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.UNKNOWN_TOKEN, ch, currentPos));
        }
        return lastToken;
    }

    public SqlToken LookaheadToken2() throws Exception {
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
            currentPos += nextToken.Value.length();
            return lastToken;
        }
        return SqlToken.None;
    }

    private SqlToken MatchExprOperator(int fromIndex) {
        for (String op : EXPR_OPERATORS) {
            if (fromIndex < inputText.length() && inputText.substring(fromIndex).startsWith(op)) {
                return new SqlToken(op, TokenKind.Operator);
            }
        }
        return SqlToken.None;
    }

    // very hackish, but might be enough for now
    static boolean IsText(String name) {
        for (int i = 0; i < name.length(); i++) {
            if (Character.isWhitespace(name.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    public static String Quote(SqlValue expr) {
        return Quote(expr, RebuildOptions.SQLite);
    }

    public static String Quote(SqlValue expr, RebuildOptions rebuildOptions) {
        if (expr != null) {
            if (expr.getValueType() == SqlValueType.DATETIME) {
                return "'" + expr.getValueAsString(null) + "'";
            } else if (expr.getValueType() == SqlValueType.STRING) {
                String str = expr.getValueAsString("");
                StringBuilder buf = new StringBuilder("'");
                for (int i = 0; i < str.length(); i++) {
                    char ch = str.charAt(i);
                    if (ch == '\\') {
                        buf.append('\\');
                    } else if (ch == '\'') {
                        buf.append('\'');
                    }
                    buf.append(ch);
                }
                buf.append("'");
                return buf.toString();
            } else {
                return expr.getValueAsString("");
            }
        }
        return null;
    }

    public static String Quote(SqlToken token) {
        return Quote(token, RebuildOptions.SQLite);
    }

    public static String Quote(SqlToken token, RebuildOptions rebuildOptions) {
        switch (token.Kind) {
            case /*#TokenKind.#*/ Identifier:
                return Bracket(token.Value, rebuildOptions);
            case /*#TokenKind.#*/ Number:
            case /*#TokenKind.#*/ Null:
            case /*#TokenKind.#*/ Bool:
            case /*#TokenKind.#*/ UnquotedStr:
                return token.Value;
        }
        String str = token.Value;
        StringBuilder buf = new StringBuilder("'");
        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            if (ch == '\\') {
                buf.append('\\');
            } else if (ch == '\'') {
                buf.append('\'');
            }
            buf.append(ch);
        }
        buf.append("'");
        return buf.toString();
    }

    static String Bracket(String str) {
        return Bracket(str, RebuildOptions.SQLite);
    }

    static String Bracket(String str, RebuildOptions rebuildOptions) {
        String name = str.trim();
        if (name != "*") {
            return rebuildOptions.quoteIdentifier(name);
        }
        return name;
    }

    private void SkipWhitespace() throws Exception {
        while (!EOF()) {
            char ch = NextChar();
            if (!Character.isWhitespace(ch) && (ch != '\0')) {
                Backtrack();
                break;
            }
        }
    }

    private SqlToken ReadIdentifier() throws Exception {
        char ch = PeekChar();
        boolean readQuoted = false;
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
            StringBuilder token = new StringBuilder();
            String subName = "";
            while (!EOF()) {
                ch = NextChar();
                if (Character.isWhitespace(ch)) {
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
                token.append(ch);
            }
            if (token.length() <= 0) first = SqlToken.None;
            String token_str = token.toString();
            first = new SqlToken(token_str, getTokenType(token_str.toUpperCase()));
        }
        return first;
    }

    private SqlToken ReadString() throws Exception {
        char ch = PeekChar();
        if (ch == '\'') {
            return new SqlToken(ReadQuoted(), true, "'", "'", TokenKind.Str);
        } else if (ch == '"') {
            return new SqlToken(ReadQuoted(), true, "\"", "\"", TokenKind.Str);
        }
        StringBuilder token = new StringBuilder();
        while (!EOF()) {
            ch = NextChar();
            if (Character.isWhitespace(ch)) {
                break;
            }
            if (IsStringDelimiter(ch)) {
                Backtrack();
                break;
            }
            token.append(ch);
        }
        return token.length() > 0
                ? new SqlToken(token.toString(), TokenKind.Str)
                : SqlToken.None;
    }

    public /*#virtual#*/ SqlToken ReadNumberOrDate() throws Exception {
        SqlToken retVal = SqlToken.None;
        StringBuilder token = new StringBuilder();
        boolean digital = true;
        boolean exponential = false;
        while (!EOF()) {
            char ch = NextChar();
            if (token.length() > 0 && digital && (ch == 'E' || ch == 'e')) {
                exponential = true;
            }
            if (!Character.isDigit(ch) && ch != '.') {
                digital = false;
            }

            boolean signOfExponent = exponential && (ch == '+' || ch == '-');
            if (ch == ',' || ch == ')' || ch == '=' || ch == ';' || (token.length() > 0 && ch != '.' && !Character.isLetterOrDigit(ch) && !signOfExponent)) {
                Backtrack();
                break;
            }

            if (Character.isWhitespace(ch)) {
                break;
            }
            token.append(ch);
        }

        if (token.length() > 0) {
            String tokenStr = token.toString();
            double floatVal = 0;
            long longVal;
            try {
                if (tokenStr.startsWith("0x")) {
                    longVal = Utilities.toIntFromHex(tokenStr.substring(2));
                } else {
                    floatVal = Utilities.getValueAsDouble2(tokenStr, 0, InvariantCulture); // CultureInfo is used here to maintain previous functionality that was within SQLParserUtil which just called double.TryParse(value, out)
                }
                if (tokenStr.startsWith(".") && floatVal != 0) {
                    String strFloat;
//@
                    NumberFormat numberFormat = NumberFormat.getInstance(InvariantCulture);
                    numberFormat.setMaximumFractionDigits(Integer.MAX_VALUE);
                    numberFormat.setGroupingUsed(false);
                    strFloat = numberFormat.format(floatVal);
//@
/*#
          IFormatProvider nf = InvariantCulture.NumberFormat;
          strFloat = ((IFormattable)floatVal).ToString(null, nf);
#*/
                    retVal = new SqlToken(strFloat, TokenKind.Number);
                } else {
                    retVal = new SqlToken(tokenStr, TokenKind.Number);
                }
            } catch (Exception e) {
                //Parse double fails, it's not a number
                //String with no quotes around it. It sometimes appears for dates.
                retVal = new SqlToken(token.toString(), TokenKind.Str);
            }
        }
        return retVal;
    }

    private SqlToken ReadMultiLineComment() throws Exception {
        SqlToken retVal = SqlToken.None;
        StringBuilder token = new StringBuilder();
        while (!EOF()) {
            char ch = NextChar();
            if (ch == '*' && PeekChar() == '/') {
                NextChar(); // move past /
                break;
            }
            token.append(ch);
        }
        if (token.length() > 0) {
            retVal = new SqlToken(token.toString(), TokenKind.Comment);
        }
        return retVal;
    }

    private SqlToken ReadLineComment() throws Exception {
        SqlToken retVal = SqlToken.None;
        StringBuilder token = new StringBuilder();
        while (!EOF()) {
            char ch = NextChar();
            if (ch == '\r' && PeekChar() == '\n') {
                NextChar(); // move past /
                break;
            } else if (ch == '\n' || ch == '\r') {
                break;
            }
            token.append(ch);
        }
        if (token.length() > 0) {
            retVal = new SqlToken(token.toString(), TokenKind.Comment);
        }
        return retVal;
    }

    private SqlToken ReadNumber() throws Exception {
        StringBuilder token = new StringBuilder();
        while (!EOF()) {
            char ch = NextChar();
            if (Character.isWhitespace(ch)) {
                break;
            }
            if (Character.isDigit(ch) || ch == '.' || ch == '-') {
                token.append(ch);
            } else {
                Backtrack();
                break;
            }
        }
        return token.length() > 0
                ? new SqlToken(token.toString(), TokenKind.Number)
                : SqlToken.None;
    }

    private SqlToken ReadOperator() throws Exception {
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
                int p = this.currentPosition();
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

    private String ReadDoubleQuoted() throws Exception {
        char ch = NextChar();
        char closeQuote = '"';
        lastTokenWasQuoted = true;
        return ReadUntil(closeQuote);
    }

    private String ReadTickQuoted() throws Exception {
        char ch = NextChar();
        char closeQuote = '`';
        lastTokenWasQuoted = true;
        return ReadUntil(closeQuote);
    }

    private String ReadQuoted() throws Exception {
        char ch = NextChar();
        char closeQuote = ']';
        if (ch == '"' || ch == '\'') {
            closeQuote = ch;
        }
        lastTokenWasQuoted = true;
        return ReadUntil(closeQuote);
    }

    private String ReadUntil(char delim) throws Exception {
        StringBuilder token = new StringBuilder();
        int pos = currentPos - 1;
        boolean foundDelim = false;
        boolean seenEscape = false;
        boolean escapeNextQuote = false;
        while (!EOF()) {
            char ch = NextChar();
            if (seenEscape) {
                token.append(ch);
                seenEscape = false;
            } else {
                if (ch == delim) {
                    if (!EOF() && (PeekChar() == delim || escapeNextQuote)) {
                        if (!escapeNextQuote) token.append(ch);
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
                    token.append(ch);
                }
            }
        }
        if (!foundDelim) {
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.EXPECTED_ENDQUOTE, delim, pos));
        }
        return token.toString();
    }

    private char PeekChar() {
        return inputText.charAt(currentPos);
    }

    private boolean IsStringDelimiter(char ch) {
        if (Character.isLetterOrDigit(ch)) return false;
        if (IsStringChar(ch)) return false;
        return true;
    }

    private boolean IsIdentifierDelimiter(char ch) {
        // return IsIdentifierChar(ch); TBD - Why are IsIdentifierDelimiter and IsIdentifierChar not exactly opposite? -Amit
        if (Character.isLetterOrDigit(ch)) return false;
        //return "$@:_-#.*".indexOf(ch) < 0;
        return "$@:#_".indexOf(ch) < 0;
    }

    private boolean IsIdentifierStartChar(char ch) {
        if (Character.isLetter(ch)) return true;
        if (ch == '"') return true;
        return "$@:_[*`".indexOf(ch) >= 0;
    }

    private boolean IsStringChar(char ch) {
        if (Character.isLetter(ch)) return true;
        if (ch == '#') return true;
        return (ch == '\'' || ch == '"');
    }

    private boolean IsOperatorChar(char ch) {
        return "!<=>*,+-%/|".indexOf(ch) >= 0;
    }

    public boolean EOF() {
        return currentPos >= inputText.length();
    }

    private char PeekNextChar() throws Exception {
        char ch = NextChar();
        ch = PeekChar();
        Backtrack();
        return ch;
    }

    public char NextChar() throws Exception {
        if (EOF()) {
            throw MalformedSql(SqlExceptions.localizedMessage(SqlExceptions.INPUT_READ_ERROR));
        }
        return inputText.charAt(currentPos++);
    }

    protected void Backtrack() {
        currentPos--;
    }

    public void Backtrack(int pos) {
        currentPos = pos;
    }

    public Exception MalformedSql(String message) throws Exception {
        throw SqlExceptions.Exception("SqlException", SqlExceptions.MALFORMED_SQL, message, this.inputText);
    }

    private static TokenKind getTokenType(String token) {
        switch (token.charAt(0)) {
            case '@':
                return TokenKind.Parameter;
            case 'C':
                if (token.equals("CROSS")) {
                    return TokenKind.Keyword;
                } else if (token.equals("COLLATE")) {
                    return TokenKind.Keyword;
                } else if (token.equals("CURRENT_TIMESTAMP")) {
                    return TokenKind.Keyword;
                } else if (token.equals("CURRENT_TIME")) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'D':
                if (token.equals("DISTINCT")) {
                    return TokenKind.Keyword;
                } else if (token.equals("DESC")) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'E':
                if ("EXCEPT".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("EXISTS".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'F':
                if ("FROM".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("FALSE".equals(token)) {
                    return TokenKind.Bool;
                }
                return TokenKind.Identifier;
            case 'G':
                if ("GROUP".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'H':
                if ("HAVING".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'I':
                if ("INNER".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("INTERSECT".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("IS".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("INTO".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'J':
                if ("JOIN".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'L':
                if ("LIMIT".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("LIKE".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'M':
                if ("MINUS".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'N':
                if ("NOT".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("NATURAL".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("NULL".equals(token)) {
                    return TokenKind.Null;
                }
                return TokenKind.Identifier;
            case 'O':
                if ("ON".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("OFFSET".equals(token)) {
                    return TokenKind.Keyword;
                } else if ("ORDER".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'S':
                if ("SELECT".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'T':
                if ("TRUE".equals(token)) {
                    return TokenKind.Bool;
                }
                return TokenKind.Identifier;
            case 'U':
                if ("UNION".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            case 'W':
                if ("WHERE".equals(token)) {
                    return TokenKind.Keyword;
                }
                return TokenKind.Identifier;
            default:
                return TokenKind.Identifier;
        }
    }
}

