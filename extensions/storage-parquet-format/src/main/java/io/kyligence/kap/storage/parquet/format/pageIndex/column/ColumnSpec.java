package io.kyligence.kap.storage.parquet.format.pageIndex.column;

public class ColumnSpec {
    private int columnLength;
    private int cardinality;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    private String columnName;

    public ColumnSpec(String columnName, int columnLength, int cardinality, boolean onlyEQIndex, int columnSequence) {
        this.columnLength = columnLength;
        this.cardinality = cardinality;
        this.columnName = columnName;
        this.onlyEQIndex = onlyEQIndex;
        this.columnSequence = columnSequence;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    private boolean onlyEQIndex;

    public boolean isOnlyEQIndex() {
        return onlyEQIndex;
    }

    public void setOnlyEQIndex(boolean onlyEQIndex) {
        this.onlyEQIndex = onlyEQIndex;
    }

    public int getColumnLength() {
        return columnLength;
    }

    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    public int getColumnSequence() {
        return columnSequence;
    }

    public void setColumnSequence(int columnSequence) {
        this.columnSequence = columnSequence;
    }

    private int columnSequence;
}
