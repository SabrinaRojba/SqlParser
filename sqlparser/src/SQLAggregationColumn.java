public class SQLAggregationColumn extends SQLColumn {
    private String aggregateFunction;

    public SQLAggregationColumn(String function) {
        super();
        aggregateFunction = function;
    }

    public SQLAggregationColumn(String column, String function) {
        super(column);
        aggregateFunction = function;
    }
}
