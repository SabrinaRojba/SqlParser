public class Token {
    private String value;
    private TokenType type;
    private boolean isQuoted;

    public Token(String value, TokenType type, boolean isQuoted) {
        this.value = value;
        this.type = type;
        this.isQuoted = isQuoted;
    }

    public String getValue() {
        return value;
    }

    public TokenType getType() {
        return type;
    }

    public boolean isQuoted() {
        return isQuoted;
    }
}
