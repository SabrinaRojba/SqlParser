public class Operation extends TreeExpression {
    private Expression left;
    private Expression right;
    private Operator operator;

    public Operation(Expression left, Operator operator, Expression right) {
        super(left, right);
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Operator getOperator() {
        return operator;
    }

    public Expression getRight() {
        return right;
    }
}
