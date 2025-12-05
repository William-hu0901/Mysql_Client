package org.daodao.jdbc.mysql;

import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.connectors.MySqlConnector;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test cases for MySqlConnector without Mockito
 * 
 * Due to Java 25 compatibility issues with current Mockito version,
 * this class provides basic functionality tests without using mocks.
 * These tests verify the basic structure and configuration handling.
 */
class MySqlConnectorMockitoTest {
    
    @Test
    void testMySqlConfigCreation() {
        MySqlConfig config = new MySqlConfig();
        assertNotNull(config);
        assertEquals("localhost", config.getHost());
        assertEquals(3306, config.getPort());
        assertEquals("testdb", config.getDatabase());
        assertEquals("root", config.getUsername());
    }
    
    @Test
    void testMySqlConfigWithParameters() {
        MySqlConfig config = new MySqlConfig("customhost", 3307, "customdb", "customuser", "custompass");
        assertNotNull(config);
        assertEquals("customhost", config.getHost());
        assertEquals(3307, config.getPort());
        assertEquals("customdb", config.getDatabase());
        assertEquals("customuser", config.getUsername());
        assertEquals("custompass", config.getPassword());
    }
    
    @Test
    void testMySqlConfigJdbcUrl() {
        MySqlConfig config = new MySqlConfig("localhost", 3306, "testdb", "root", "");
        String expectedUrl = "jdbc:mysql://localhost:3306/testdb?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        assertEquals(expectedUrl, config.getJdbcUrl());
    }
    
    @Test
    void testMySqlConnectorCreation() {
        MySqlConnector connector = new MySqlConnector();
        assertNotNull(connector);
    }
    
    @Test
    void testMySqlConnectorWithConfig() {
        MySqlConfig config = new MySqlConfig();
        MySqlConnector connector = new MySqlConnector(config);
        assertNotNull(connector);
    }
    
    @Test
    void testMySqlConfigMethods() {
        MySqlConfig config = new MySqlConfig("testhost", 3308, "testdb", "testuser", "testpass");
        
        // Test all getter methods
        assertEquals("testhost", config.getHost());
        assertEquals(3308, config.getPort());
        assertEquals("testdb", config.getDatabase());
        assertEquals("testuser", config.getUsername());
        assertEquals("testpass", config.getPassword());
        assertEquals("testhost", config.getMysqlHost());
        assertEquals(3308, config.getMysqlPort());
        assertEquals("testdb", config.getMysqlDatabase());
        assertEquals("testuser", config.getMysqlUsername());
        assertEquals("testpass", config.getMysqlPassword());
        
        // Test JDBC URL generation
        String jdbcUrl = config.getJdbcUrl();
        assertTrue(jdbcUrl.contains("jdbc:mysql://testhost:3308/testdb"));
        assertTrue(jdbcUrl.contains("useSSL=false"));
        assertTrue(jdbcUrl.contains("allowPublicKeyRetrieval=true"));
        assertTrue(jdbcUrl.contains("serverTimezone=UTC"));
    }
    
    @Test
    void testMySqlConfigImmutability() {
        MySqlConfig config = new MySqlConfig("host1", 3306, "db1", "user1", "pass1");
        
        // Verify initial values
        assertEquals("host1", config.getHost());
        assertEquals("db1", config.getDatabase());
        
        // Create another config with different values
        MySqlConfig config2 = new MySqlConfig("host2", 3307, "db2", "user2", "pass2");
        
        // First config should be unchanged
        assertEquals("host1", config.getHost());
        assertEquals("db1", config.getDatabase());
        
        // Second config should have new values
        assertEquals("host2", config2.getHost());
        assertEquals("db2", config2.getDatabase());
    }
    
    @Test
    void testMySqlConfigValidation() {
        // Test with valid parameters
        assertDoesNotThrow(() -> {
            new MySqlConfig("valid-host", 3306, "valid-db", "valid-user", "valid-pass");
        });
        
        // Test with edge cases
        assertDoesNotThrow(() -> {
            new MySqlConfig("", 0, "", "", "");
        });
    }
}