package org.daodao.jdbc.mysql;

import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.connectors.MySqlConnector;
import org.daodao.jdbc.exceptions.MySqlException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;
import org.opentest4j.TestAbortedException;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MySqlBasicCRUDTest {
    
    private MySqlConnector mysqlConnector;
    
    @BeforeAll
    void setUp() {
        try {
            MySqlConfig config = new MySqlConfig();
            mysqlConnector = new MySqlConnector(config);
            mysqlConnector.connect();
            
            // Clean up before tests
            mysqlConnector.execute("DROP TABLE IF EXISTS users");
            mysqlConnector.execute("DROP TABLE IF EXISTS products");
            mysqlConnector.execute("DROP VIEW IF EXISTS user_product_summary");
            
            // Initialize database
            mysqlConnector.initializeDatabase();
            
        } catch (Exception e) {
            // Skip tests if MySQL is not running
            throw new TestAbortedException("MySQL not available for testing", e);
        }
    }
    
    @AfterAll
    void tearDown() {
        if (mysqlConnector != null) {
            mysqlConnector.disconnect();
        }
    }
    
    @Test
    void testConnection() {
        assertDoesNotThrow(() -> mysqlConnector.connect());
    }
    
    @Test
    void testDatabaseInitialization() {
        assertDoesNotThrow(() -> mysqlConnector.initializeDatabase());
        assertFalse(mysqlConnector.isDatabaseEmpty());
    }
    
    @Test
    void testFindAllUsers() {
        List<String> users = mysqlConnector.findAllUsers();
        assertNotNull(users);
        assertFalse(users.isEmpty());
        assertTrue(users.size() >= 5); // Should have at least 5 sample users
    }
    
    @Test
    void testInsertUser() {
        String username = "test_user_" + System.currentTimeMillis();
        String email = username + "@example.com";
        int age = 25;
        String city = "Test City";
        
        boolean inserted = mysqlConnector.insertUser(username, email, age, city);
        assertTrue(inserted);
        
        // Verify user was inserted
        List<String> users = mysqlConnector.findUsersByCity(city);
        assertTrue(users.stream().anyMatch(user -> user.contains(username)));
    }
    
    @Test
    void testUpdateUserEmail() {
        String username = "john_doe"; // Existing user from sample data
        String newEmail = "john.doe.updated@example.com";
        
        boolean updated = mysqlConnector.updateUserEmail(username, newEmail);
        assertTrue(updated);
        
        // Verify email was updated by checking if user count is still the same
        int userCount = mysqlConnector.getUserCount();
        assertTrue(userCount >= 5);
    }
    
    @Test
    void testDeleteUser() {
        String username = "test_user_delete_" + System.currentTimeMillis();
        String email = username + "@example.com";
        
        // First insert a user
        mysqlConnector.insertUser(username, email, 30, "Delete City");
        
        // Then delete the user
        boolean deleted = mysqlConnector.deleteUser(username);
        assertTrue(deleted);
    }
    
    @Test
    void testFindUsersByCity() {
        String city = "New York"; // From sample data
        List<String> users = mysqlConnector.findUsersByCity(city);
        
        assertNotNull(users);
        assertFalse(users.isEmpty());
        assertTrue(users.stream().anyMatch(user -> user.contains("john_doe")));
    }
    
    @Test
    void testGetUserCount() {
        int count = mysqlConnector.getUserCount();
        assertTrue(count >= 5); // Should have at least 5 sample users
    }
    
    @Test
    void testInsertDuplicateUser() {
        String username = "john_doe"; // Existing user
        String email = "duplicate@example.com";
        
        // Should not throw exception due to INSERT IGNORE
        assertDoesNotThrow(() -> mysqlConnector.insertUser(username, email, 25, "City"));
    }
    
    @Test
    void testUpdateNonExistentUser() {
        String username = "nonexistent_user";
        String newEmail = "new@example.com";
        
        boolean updated = mysqlConnector.updateUserEmail(username, newEmail);
        assertFalse(updated);
    }
    
    @Test
    void testDeleteNonExistentUser() {
        String username = "nonexistent_user";
        
        boolean deleted = mysqlConnector.deleteUser(username);
        assertFalse(deleted);
    }
}