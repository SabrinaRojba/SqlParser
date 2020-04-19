public class SQLColumn {
    private String column;
    private boolean isAsterisk;

    public SQLColumn() {
    }

    public SQLColumn(String columnname) {
        column = columnname;
    }

    public void setIsAsterisk(boolean isAsterisk) {
        this.isAsterisk = isAsterisk;
    }
}
