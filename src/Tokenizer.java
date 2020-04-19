import java.util.ArrayList;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class Tokenizer implements Iterable<Token> {
    private char openChar;
    private char closeChar;
    private int currentPos;
    private String inputSql;

    private static final int STATE_NONE = 0;
    private static final int STATE_IN_DOUBLE_QUOTES = 1;
    private static final int STATE_IN_SINGLE_QUOTES = 2;
    private static final int STATE_ENCLOSED = 3;
    ArrayList<String> operators = new ArrayList<String>() {{
        add("+");
        add("-");
        add("%");
        add("*");
        add("/");
        add("&");
        add("|");
        add("^");
    }};

    ArrayList<String> comparators = new ArrayList<String>() {{
        add("!");
        add("=");
        add(">");
        add("<");
    }};

    private int state = STATE_NONE;
    protected Token lastToken = null;
    protected Token currentToken = null;

    public Tokenizer(char open, char close, String text) {
        openChar = open;
        closeChar = close;
        inputSql = text;
    }

    public Tokenizer(String text) {
        inputSql = text;
    }

    public Token nextToken() {
        lastToken = currentToken;
        currentToken = nextTokenInner();
        return currentToken;
    }

    private Token nextTokenInner() {
        if (EOF()) {
            return null;
        }

        StringBuilder token = new StringBuilder();
        char c = getNextChar();

        if (state == STATE_NONE) {
            if (c == openChar) {
                state = STATE_IN_DOUBLE_QUOTES;
                c = getNextChar();
            }
            if (c == '\'') {
                state = STATE_IN_SINGLE_QUOTES;
                c = getNextChar();
            }
        }

        while ((state == STATE_IN_DOUBLE_QUOTES && c != closeChar) || (state == STATE_IN_SINGLE_QUOTES && c != '\'')) {
            token.append(c);
            c = getNextChar();
        }

        while (state == STATE_ENCLOSED && c != ']') {
            token.append(c);
            c = getNextChar();
        }


        if (c == closeChar || c == '\'') {
            state = STATE_NONE;
            return new Token(token.toString(), getTokenType(token.toString()), true);
        }

        while (!isWhitespace(c) && !isOperator(c) && !isComparator(c)) {
            if ((c == '!') && (inputSql.charAt(currentPos) == '=' || inputSql.charAt(currentPos) == '<' || inputSql.charAt(currentPos) == '>')) {
                currentPos++;
                return new Token(Character.toString(c) + inputSql.charAt(currentPos), TokenType.Operator, false);
            }

            if (c == '(') {
                if (token.length() != 0) {
                    currentPos--;
                    break;
                } else
                    return new Token(Character.toString(c), getTokenType(Character.toString(c)), false);
            }

            if (c == '[') {
                state = STATE_ENCLOSED;
                return new Token(Character.toString(c), getTokenType(Character.toString(c)), false);
            }

            if (c == ')' || c == ']' || c == ',') {
                if (token.length() != 0) {
                    currentPos--;
                    break;
                } else
                    return new Token(Character.toString(c), getTokenType(Character.toString(c)), false);
            }

            token.append(c);
            if (!EOF())
                c = getNextChar();
            else break;
        }

        if (isOperator(c)) {
            if (token.length() == 0) {
                return new Token(token.append(c).toString(), TokenType.Operator, false);
            } else
                currentPos--;
        }
        if (isComparator(c)) {
            if (token.length() == 0) {
                token.append(c);
                char next = getNextChar();
                if (next == '=') {
                    token.append(next);
                } else {
                    currentPos--;
                }
                return new Token(token.toString(), TokenType.Comparator, false);
            } else
                currentPos--;
        }

        state = STATE_NONE;

        if (token.length() == 0) {
            return nextToken();
        }

        return new Token(token.toString(), getTokenType(token.toString()), false);
    }

    private boolean isOperator(char c) {
        return operators.contains(Character.toString(c));
    }

    private boolean isComparator(char c) {
        return comparators.contains(Character.toString(c));
    }

    private boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n';
    }

    private char getNextChar() {
        return inputSql.charAt(currentPos++);
    }

    public int getCurrentPosition() {
        return currentPos;
    }

    public void backtrack(int pos) throws TokenizerException {
        currentPos = pos;
    }

    public char lookahead(int pos) {
        return inputSql.charAt(currentPos + pos);
    }

    public Token lookaheadToken() throws TokenizerException {
        int position = getCurrentPosition();
        try {
            return nextToken();
        } finally {
            backtrack(position);
        }
    }

    public boolean EOF() {
        return currentPos > inputSql.length() - 1;
    }

    private TokenType getTokenType(String tokenstr) {
        tokenstr=tokenstr.toUpperCase();
        switch (tokenstr.charAt(0)) {
            case '*':
                return TokenType.Asterisk;
            case ',':
                return TokenType.Comma;
            case '.':
                return TokenType.Dot;
            case '(':
                return TokenType.OpenParenthese;
            case ')':
                return TokenType.CloseParenthese;
            case '[':
                return TokenType.OpenParenthese;
            case ']':
                return TokenType.CloseParenthese;
            case '@':
                return TokenType.Parameter;
            case 'A':
                if (tokenstr.equals("AS"))
                    return TokenType.Keyword;
                if (tokenstr.equals("ASC"))
                    return TokenType.Keyword;
                return TokenType.Identifier;
            case 'B':
                if (tokenstr.equals("BY"))
                    return TokenType.Keyword;
                return TokenType.Identifier;
            case 'C':
                if (tokenstr.equals("CREATE")) {
                    return TokenType.Keyword;
                }
                if (tokenstr.equals("CROSS")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("COLLATE")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("CURRENT_TIMESTAMP")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("CURRENT_TIME")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'D':
                if (tokenstr.equals("DISTINCT")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("DESC")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'E':
                if (tokenstr.equals("EXCEPT")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("EXISTS")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'F':
                if (tokenstr.equals("FROM")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("FALSE")) {
                    return TokenType.Bool;
                }
                return TokenType.Identifier;
            case 'G':
                if (tokenstr.equals("GROUP")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'H':
                if (tokenstr.equals("HAVING")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'I':
                if (tokenstr.equals("INNER")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("INTERSECT")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("IS")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("INTO")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'J':
                if (tokenstr.equals("JOIN")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'L':
                if (tokenstr.equals("LIMIT")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("LIKE")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("LEFT"))
                    return TokenType.Keyword;
                return TokenType.Identifier;
            case 'M':
                if (tokenstr.equals("MINUS")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'N':
                if (tokenstr.equals("NOT")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("NATURAL")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("NULL")) {
                    return TokenType.Null;
                }
                return TokenType.Identifier;
            case 'O':
                if (tokenstr.equals("ON")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("OFFSET")) {
                    return TokenType.Keyword;
                } else if (tokenstr.equals("ORDER")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'R':
                if (tokenstr.equals("RIGHT")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'S':
                if (tokenstr.equals("SELECT")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'T':
                if (tokenstr.equals("TABLE")) {
                    return TokenType.Keyword;
                }
                if (tokenstr.equals("TRUE")) {
                    return TokenType.Bool;
                }
                return TokenType.Identifier;
            case 'U':
                if (tokenstr.equals("UNION")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            case 'W':
                if (tokenstr.equals("WHERE")) {
                    return TokenType.Keyword;
                }
                return TokenType.Identifier;
            default:
                return TokenType.Identifier;
        }
    }

    public void ensureNext(String s) throws TokenizerException {
        Token token = nextToken();

        if (!token.equals(s)) {
            throw new TokenizerException("Expected: " + s + ", found: " + token.getValue() + ".");
        }
    }

    public void ensureNextList(String... s) throws TokenizerException {
        Token token = nextToken();

        boolean found = false;
        for (String sEle : s) {
            found = found || token.equals(sEle);
        }

        if (!found) {
            throw new TokenizerException("Unexpected element found.");
        }
    }

    public Token lastToken() {
        return lastToken;
    }

    @Override
    public Iterator<Token> iterator() {
        return new TokenizerIter(new Tokenizer(openChar, closeChar, inputSql));
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Token t : this) {
            builder.append(t.getValue());
            builder.append(" ");
        }
        return builder.toString();
    }

    class TokenizerIter implements Iterator<Token> {
        private int back = 0;
        private Tokenizer clonedTk;

        public TokenizerIter(Tokenizer clonedTk) {
            this.clonedTk = clonedTk;
        }

        @Override
        public boolean hasNext() {
            int backtrack = clonedTk.getCurrentPosition();
            try {
                return clonedTk.nextToken() != null;
            } finally {
                try {
                    clonedTk.backtrack(backtrack);
                } catch (TokenizerException e) {
                }
            }
        }

        @Override
        public Token next() {
            back = clonedTk.getCurrentPosition();
            return clonedTk.nextToken();
        }

        @Override
        public void remove() {
            try {
                clonedTk.backtrack(back);
            } catch (TokenizerException e) {
            }
        }
    }
}