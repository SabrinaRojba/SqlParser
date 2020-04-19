public class Criteria extends TreeExpression {
    private Comparator comparator;

    public Criteria(Expression left, Comparator comparator, Expression right) {
        super(left, right);
        this.comparator = comparator;
    }

    public Comparator getComparator() {
        return comparator;
    }
}
