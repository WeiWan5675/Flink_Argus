package org.weiwan.argus.core.pub.api;

/**
 * @Author: xiaozhennan
 * @Date: 2020/7/23 14:19
 * @Package: org.weiwan.argus.core.pub.api
 * @ClassName: SqlInfo
 * @Description:
 **/
public class SqlInfo {
    private String tableName;
    private Long lastOffset;
    private Long maxOffset;
    private Long minOffset;
    private String incrField;
    private String splitField;
    private String[] columns;
    private String[] filters;
    private Integer splitNum;
    private Integer thisSplitNum;

    private SqlInfo(Builder builder) {
        setTableName(builder.tableName);
        setLastOffset(builder.lastOffset);
        setMaxOffset(builder.maxOffset);
        setMinOffset(builder.minOffset);
        setIncrField(builder.incrField);
        setSplitField(builder.splitField);
        setColumns(builder.columns);
        setFilters(builder.filters);
        setSplitNum(builder.splitNum);
        setThisSplitNum(builder.thisSplitNum);
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(Long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public String getIncrField() {
        return incrField;
    }

    public void setIncrField(String incrField) {
        this.incrField = incrField;
    }

    public String getSplitField() {
        return splitField;
    }

    public void setSplitField(String splitField) {
        this.splitField = splitField;
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public String[] getFilters() {
        return filters;
    }

    public void setFilters(String[] filters) {
        this.filters = filters;
    }

    public Integer getSplitNum() {
        return splitNum;
    }

    public void setSplitNum(Integer splitNum) {
        this.splitNum = splitNum;
    }

    public Integer getThisSplitNum() {
        return thisSplitNum;
    }

    public void setThisSplitNum(Integer thisSplitNum) {
        this.thisSplitNum = thisSplitNum;
    }

    public static final class Builder {
        private String tableName;
        private Long lastOffset;
        private Long maxOffset;
        private Long minOffset;
        private String incrField;
        private String splitField;
        private String[] columns;
        private String[] filters;
        private Integer splitNum;
        private Integer thisSplitNum;

        private Builder() {
        }

        public Builder tableName(String val) {
            tableName = val;
            return this;
        }

        public Builder lastOffset(Long val) {
            lastOffset = val;
            return this;
        }

        public Builder maxOffset(Long val) {
            maxOffset = val;
            return this;
        }

        public Builder minOffset(Long val) {
            minOffset = val;
            return this;
        }

        public Builder incrField(String val) {
            incrField = val;
            return this;
        }

        public Builder splitField(String val) {
            splitField = val;
            return this;
        }

        public Builder columns(String[] val) {
            columns = val;
            return this;
        }

        public Builder filters(String[] val) {
            filters = val;
            return this;
        }

        public Builder splitNum(Integer val) {
            splitNum = val;
            return this;
        }

        public Builder thisSplitNum(Integer val) {
            thisSplitNum = val;
            return this;
        }

        public SqlInfo build() {
            return new SqlInfo(this);
        }
    }
}

