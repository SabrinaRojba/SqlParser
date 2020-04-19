public class Condition extends TreeExpression {
    private Kind kind;

    public Condition(Expression left, Kind kind, Expression right) {
        super(left, right);
        this.kind = kind;
    }

    public Kind getKind() {
        return kind;
    }

    public enum Kind {
        AND,
        OR
    }

    @Override
    public String toString() {
        switch (kind) {
            case AND:
                return "AND";
            case OR:
                return "OR";
        }

        return "";
    }
}
