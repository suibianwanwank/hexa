package com.ccsu.datasource.api.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@Data
public class DatasourceConfig implements Serializable {
    private DatasourceType sourceType;
    private String host;
    private String port;
    private String userName;
    private String password;
    private String database;
    private Map<String, String> optionParameters;

    public DatasourceConfig(DatasourceType sourceType,
                           String host,
                           String port,
                           String userName,
                           String password,
                           String database,
                           Map<String, String> optionParameters) {
        this.sourceType = sourceType;
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.password = password;
        this.database = database;
        this.optionParameters = optionParameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatasourceConfig that = (DatasourceConfig) o;
        return sourceType == that.sourceType
                && Objects.equals(host, that.host)
                && Objects.equals(port, that.port)
                && Objects.equals(userName, that.userName)
                && Objects.equals(password, that.password)
                && Objects.equals(database, that.database)
                && Objects.equals(optionParameters, that.optionParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceType, host, port, userName, password, database, optionParameters);
    }

    @Override
    public String toString() {
        return "DatasourceConfig{"
                + "sourceType=" + sourceType
                + ", host='" + host + '\''
                + ", port='" + port + '\''
                + ", userName='" + userName + '\''
                + ", password='" + password + '\''
                + ", database='" + database + '\''
                + '}';
    }
}
