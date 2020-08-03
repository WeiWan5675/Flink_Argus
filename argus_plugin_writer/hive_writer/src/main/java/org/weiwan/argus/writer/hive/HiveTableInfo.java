package org.weiwan.argus.writer.hive;

import org.weiwan.argus.core.pub.enums.CompressType;
import org.weiwan.argus.core.pub.enums.FileType;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 10:50
 * @Package: org.weiwan.argus.writer.hive
 * @ClassName: HiveTableInfo
 * @Description:
 **/
public class HiveTableInfo {
    private String jdbcUrl;
    private String driveClassName;
    private String userName;
    private String passWord;
    private String dbSchema;
    private String tableName;
    private String inputClass;
    private String outputClass;
    private FileType fileType;
    private CompressType compressType;
    private boolean enableAutoCreateTable;
    private MatchMode matchMode;
    private String fieldDelimiter;
    private String lineDelimiter;

    private HiveTableInfo(Builder builder) {
        setJdbcUrl(builder.jdbcUrl);
        setDriveClassName(builder.driveClassName);
        setUserName(builder.userName);
        setPassWord(builder.passWord);
        setDbSchema(builder.dbSchema);
        setTableName(builder.tableName);
        setInputClass(builder.inputClass);
        setOutputClass(builder.outputClass);
        setFileType(builder.fileType);
        setCompressType(builder.compressType);
        setEnableAutoCreateTable(builder.enableAutoCreateTable);
        setMatchMode(builder.matchMode);
        setFieldDelimiter(builder.fieldDelimiter);
        setLineDelimiter(builder.lineDelimiter);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getDriveClassName() {
        return driveClassName;
    }

    public void setDriveClassName(String driveClassName) {
        this.driveClassName = driveClassName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassWord() {
        return passWord;
    }

    public void setPassWord(String passWord) {
        this.passWord = passWord;
    }

    public String getDbSchema() {
        return dbSchema;
    }

    public void setDbSchema(String dbSchema) {
        this.dbSchema = dbSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getInputClass() {
        return inputClass;
    }

    public void setInputClass(String inputClass) {
        this.inputClass = inputClass;
    }

    public String getOutputClass() {
        return outputClass;
    }

    public void setOutputClass(String outputClass) {
        this.outputClass = outputClass;
    }

    public FileType getFileType() {
        return fileType;
    }

    public void setFileType(FileType fileType) {
        this.fileType = fileType;
    }

    public CompressType getCompressType() {
        return compressType;
    }

    public void setCompressType(CompressType compressType) {
        this.compressType = compressType;
    }

    public boolean isEnableAutoCreateTable() {
        return enableAutoCreateTable;
    }

    public void setEnableAutoCreateTable(boolean enableAutoCreateTable) {
        this.enableAutoCreateTable = enableAutoCreateTable;
    }

    public MatchMode getMatchMode() {
        return matchMode;
    }

    public void setMatchMode(MatchMode matchMode) {
        this.matchMode = matchMode;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public void setLineDelimiter(String lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
    }

    public static final class Builder {
        private String jdbcUrl;
        private String driveClassName;
        private String userName;
        private String passWord;
        private String dbSchema;
        private String tableName;
        private String inputClass;
        private String outputClass;
        private FileType fileType;
        private CompressType compressType;
        private boolean enableAutoCreateTable;
        private MatchMode matchMode;
        private String fieldDelimiter;
        private String lineDelimiter;

        private Builder() {
        }

        public Builder jdbcUrl(String val) {
            jdbcUrl = val;
            return this;
        }

        public Builder driveClassName(String val) {
            driveClassName = val;
            return this;
        }

        public Builder userName(String val) {
            userName = val;
            return this;
        }

        public Builder passWord(String val) {
            passWord = val;
            return this;
        }

        public Builder dbSchema(String val) {
            dbSchema = val;
            return this;
        }

        public Builder tableName(String val) {
            tableName = val;
            return this;
        }

        public Builder inputClass(String val) {
            inputClass = val;
            return this;
        }

        public Builder outputClass(String val) {
            outputClass = val;
            return this;
        }

        public Builder fileType(FileType val) {
            fileType = val;
            return this;
        }

        public Builder compressType(CompressType val) {
            compressType = val;
            return this;
        }

        public Builder enableAutoCreateTable(boolean val) {
            enableAutoCreateTable = val;
            return this;
        }

        public Builder matchMode(MatchMode val) {
            matchMode = val;
            return this;
        }

        public Builder fieldDelimiter(String val) {
            fieldDelimiter = val;
            return this;
        }

        public Builder lineDelimiter(String val) {
            lineDelimiter = val;
            return this;
        }

        public HiveTableInfo build() {
            return new HiveTableInfo(this);
        }
    }
}
