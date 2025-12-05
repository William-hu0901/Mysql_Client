package org.daodao.jdbc.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.exceptions.PropertyException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MySqlConfig {
    
    private static final Logger log = LoggerFactory.getLogger(MySqlConfig.class);
    private static final String PROPERTIES_FILE = "application.properties";
    
    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUsername;
    private final String mysqlPassword;
    
    public MySqlConfig() {
        Properties properties = loadProperties();
        this.mysqlHost = getProperty(properties, "mysql.host");
        this.mysqlPort = Integer.parseInt(getProperty(properties, "mysql.port"));
        this.mysqlDatabase = getProperty(properties, "mysql.database");
        this.mysqlUsername = getProperty(properties, "mysql.username");
        this.mysqlPassword = getProperty(properties, "mysql.password");
        
        log.info("MySQL configuration loaded successfully");
    }
    
    public MySqlConfig(String host, int port, String database, String username, String password) {
        this.mysqlHost = host;
        this.mysqlPort = port;
        this.mysqlDatabase = database;
        this.mysqlUsername = username;
        this.mysqlPassword = password;
    }
    
    private Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE)) {
            if (input == null) {
                throw new PropertyException("Unable to find " + PROPERTIES_FILE);
            }
            properties.load(input);
        } catch (IOException e) {
            throw new PropertyException("Error loading properties file: " + PROPERTIES_FILE, e);
        }
        return properties;
    }
    
    private String getProperty(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new PropertyException("Required property '" + key + "' is missing or empty in " + PROPERTIES_FILE);
        }
        return value.trim();
    }
    
    public String getMysqlHost() {
        return mysqlHost;
    }
    
    public int getMysqlPort() {
        return mysqlPort;
    }
    
    public String getMysqlDatabase() {
        return mysqlDatabase;
    }
    
    public String getMysqlUsername() {
        return mysqlUsername;
    }
    
    public String getMysqlPassword() {
        return mysqlPassword;
    }
    
    // Convenience methods for MySqlConnector compatibility
    public String getHost() {
        return mysqlHost;
    }
    
    public int getPort() {
        return mysqlPort;
    }
    
    public String getDatabase() {
        return mysqlDatabase;
    }
    
    public String getUsername() {
        return mysqlUsername;
    }
    
    public String getPassword() {
        return mysqlPassword;
    }
    
    public String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", mysqlHost, mysqlPort, mysqlDatabase);
    }
}