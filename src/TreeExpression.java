public class TreeExpression extends Expression {
    private Expression left;
    private Expression right;

    public TreeExpression (Expression left, Expression right) {
        super(null);
        this.left = left;
        this.right = right;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }
}
