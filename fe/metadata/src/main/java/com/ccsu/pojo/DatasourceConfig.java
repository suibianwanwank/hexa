package com.ccsu.pojo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

@Data
public class DatasourceConfig implements Serializable {
    @JsonIgnore
    private String configUniqueKey;
    private DatasourceType sourceType;
    private String host;
    private String port;
    private String username;
    private String password;
    private String database;
    // TODO Special configurations are not currently supported
    @JsonIgnore
    private Map<String, String> optionParameters;

    public DatasourceConfig() {
    }

    public DatasourceConfig(
            DatasourceType sourceType,
            String host,
            String port,
            String userName,
            String password,
            String database,
            Map<String, String> optionParameters) {
        this.sourceType = sourceType;
        this.host = host;
        this.port = port;
        this.username = userName;
        this.password = password;
        this.database = database;
        this.optionParameters = optionParameters;
        this.configUniqueKey = generateKey(sourceType, host, port, userName, password, database);
    }

    public String getConfigUniqueKey() {
        if (configUniqueKey == null) {
            this.configUniqueKey = generateKey(sourceType, host, port, username, password, database);
        }
        return configUniqueKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatasourceConfig that = (DatasourceConfig) o;
        return Objects.equals(configUniqueKey, that.configUniqueKey) && sourceType == that.sourceType && Objects.equals(host, that.host) && Objects.equals(port, that.port) && Objects.equals(username, that.username) && Objects.equals(password, that.password) && Objects.equals(database, that.database) && Objects.equals(optionParameters, that.optionParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configUniqueKey, sourceType, host, port, username, password, database, optionParameters);
    }

    @Override
    public String toString() {
        return "DatasourceConfig{" +
                "sourceType=" + sourceType +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", database='" + database + '\'' +
                ", optionParameters=" + optionParameters +
                '}';
    }

    private static String generateKey(DatasourceType sourceType, String host, String port, String username,
                                      String password, String database) {
        return sourceType + "/" + host + ":" + port + "/" + username + "@" + password + "/database =" + database;
    }
}
