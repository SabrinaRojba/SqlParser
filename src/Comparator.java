public class Comparator extends Expression {
    private Kind kind;

    public Comparator(Object value) {
        super(value);

        if (value.equals(">")) {
            kind = Kind.LARGER_THAN;
        } else if (value.equals("<")) {
            kind = Kind.SMALLER_THAN;
        } else if (value.equals(">=")) {
            kind = Kind.LARGER_THAN_OR_EQUAL_TO;
        } else if (value.equals("<=")) {
            kind = Kind.SMALLER_THAN_OR_EQUAL_TO;
        } else if (value.equals("=")) {
            kind = Kind.EQUAL_TO;
        } else if (value.equals("!=")) {
            kind = Kind.NOT_EQUAL_TO;
        } else if (value.equals("IN")) {
            kind = Kind.IN;
        } else if (value.equals("NOT IN")) {
            kind = Kind.NOT_IN;
        } else if (value.equals("LIKE")) {
            kind = Kind.LIKE;
        } else if (value.equals("NOT LIKE")) {
            kind = Kind.NOT_LIKE;
        }
    }

    public Kind getKind() {
        return kind;
    }

    public enum Kind {
        LARGER_THAN,
        SMALLER_THAN,
        LARGER_THAN_OR_EQUAL_TO,
        SMALLER_THAN_OR_EQUAL_TO,
        EQUAL_TO,
        NOT_EQUAL_TO,
        IN,
        NOT_IN,
        LIKE,
        NOT_LIKE,
    }

    @Override
    public String toString() {
        switch (kind) {

            case LARGER_THAN:
                return ">";
            case SMALLER_THAN:
                return "<";
            case LARGER_THAN_OR_EQUAL_TO:
                return ">=";
            case SMALLER_THAN_OR_EQUAL_TO:
                return "<=";
            case EQUAL_TO:
                return "=";
            case NOT_EQUAL_TO:
                return "!=";
            case IN:
                return "IN";
            case NOT_IN:
                return "NOT IN";
            case LIKE:
                return "LIKE";
            case NOT_LIKE:
                return "NOT LIKE";
        }

        return "";
    }
}
