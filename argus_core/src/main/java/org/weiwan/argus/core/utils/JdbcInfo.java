package org.weiwan.argus.core.utils;

/**
 * @Author: xiaozhennan
 * @Date: 2020/8/3 14:25
 * @Package: org.weiwan.argus.core.utils
 * @ClassName: JdbcInfo
 * @Description:
 **/
public class JdbcInfo {
    private String jdbcUrl;
    private String driveClassName;
    private String username;
    private String password;
    private String database;

    private JdbcInfo(Builder builder) {
        setJdbcUrl(builder.jdbcUrl);
        setDriveClassName(builder.driveClassName);
        setUsername(builder.username);
        setPassword(builder.password);
        setDatabase(builder.database);
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public static final class Builder {
        private String jdbcUrl;
        private String driveClassName;
        private String username;
        private String password;
        private String database;

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

        public Builder username(String val) {
            username = val;
            return this;
        }

        public Builder password(String val) {
            password = val;
            return this;
        }

        public Builder database(String val) {
            database = val;
            return this;
        }

        public JdbcInfo build() {
            return new JdbcInfo(this);
        }
    }
}
