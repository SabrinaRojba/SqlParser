public class Operator extends Expression {
    private Kind kind;

    public Operator(Object value) {
        super(value);

        if (value.equals("+")) {
            kind = Kind.PLUS;
        } else if (value.equals("-")) {
            kind = Kind.MINUS;
        } else if (value.equals("/")) {
            kind = Kind.DIVISION;
        } else if (value.equals("*")) {
            kind = Kind.MULTIPLY;
        } else if (value.equals("%")) {
            kind = Kind.MODULUS;
        } else if (value.equals("=")) {
            kind = Kind.EQUALS;
        } else if (value.equals("!=")) {
            kind = Kind.NOTEQUALS;
        }
    }

    public Kind getKind() {
        return kind;
    }

    public enum Kind {
        PLUS,
        MINUS,
        DIVISION,
        MULTIPLY,
        MODULUS,
        EQUALS,
        NOTEQUALS
    }
}
