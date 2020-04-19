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

    @Override
    public boolean equals(Object obj) {
        if (obj == null && this.value == null) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj instanceof String) {
            return this.value.toLowerCase().equals(((String) obj).toLowerCase());
        }

        if (obj instanceof Token) {
            return ((Token) obj).value.toLowerCase().equals(this.value.toLowerCase()) && ((Token) obj).getType() == this.type;
        }

        return false;
    }
}
