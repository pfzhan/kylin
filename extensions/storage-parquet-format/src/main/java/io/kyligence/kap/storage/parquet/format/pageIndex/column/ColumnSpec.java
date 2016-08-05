package io.kyligence.kap.storage.parquet.format.pageIndex.column;

public class ColumnSpec {
    private int columnLength;
    private int cardinality;
    private char keyEncodingIdentifier = 'a';
    private char valueEncodingIdentifier = 'a';
    private String columnName;
    private boolean onlyEQIndex;
    private int columnSequence;

    public ColumnSpec(String columnName, int columnLength, int cardinality, boolean onlyEQIndex, int columnSequence) {
        this.columnLength = columnLength;
        this.cardinality = cardinality;
        this.columnName = columnName;
        this.onlyEQIndex = onlyEQIndex;
        this.columnSequence = columnSequence;
    }

    public char getKeyEncodingIdentifier() {
        return keyEncodingIdentifier;
    }

    public void setKeyEncodingIdentifier(char keyEncodingIdentifier) {
        this.keyEncodingIdentifier = keyEncodingIdentifier;
    }

    public char getValueEncodingIdentifier() {
        return valueEncodingIdentifier;
    }

    public void setValueEncodingIdentifier(char valueEncodingIdentifier) {
        this.valueEncodingIdentifier = valueEncodingIdentifier;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

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
}
