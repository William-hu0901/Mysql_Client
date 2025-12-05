package org.daodao.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.connectors.MySqlConnector;
import org.daodao.jdbc.exceptions.MySqlException;

import java.util.List;

public class JdbcClientMain {
    
    private static final Logger log = LoggerFactory.getLogger(JdbcClientMain.class);

    public static void main(String[] args) {
        new JdbcClientMain().run();
    }

    private void run() {
        try {
            actionOnMySQL();
        } catch (Exception e) {
            log.error("Application error: ", e);
        }
    }
    
    
    private void actionOnMySQL() {
        MySqlConnector mysqlConnector = null;
        try {
            // Load configuration from application.properties
            MySqlConfig config = new MySqlConfig();
            
            // Create MySQL connector using configuration
            mysqlConnector = new MySqlConnector(config);
            
            mysqlConnector.connect();
            log.info("Successfully connected to MySQL database.");
            
            // Initialize database if empty
            if (mysqlConnector.isDatabaseEmpty()) {
                log.info("Database is empty, initializing with sample data...");
                mysqlConnector.initializeDatabase();
                log.info("Database initialized successfully.");
            }
            
            // Find all users
            List<String> allUsers = mysqlConnector.findAllUsers();
            log.info("Total users in database: {}", allUsers.size());
            
            // Display first few users
            int displayCount = Math.min(3, allUsers.size());
            for (int i = 0; i < displayCount; i++) {
                log.info("User {}: {}", i + 1, allUsers.get(i));
            }
            
            // Find users by city
            List<String> newYorkUsers = mysqlConnector.findUsersByCity("New York");
            log.info("Found {} users in New York", newYorkUsers.size());
            
            // Get total user count
            int userCount = mysqlConnector.getUserCount();
            log.info("Total user count: {}", userCount);
            
            // Test CRUD operations
            String testUsername = "test_user_" + System.currentTimeMillis();
            String testEmail = testUsername + "@example.com";
            
            // Create
            boolean inserted = mysqlConnector.insertUser(testUsername, testEmail, 25, "Test City");
            if (inserted) {
                log.info("Successfully inserted test user: {}", testUsername);
            }
            
            // Update
            String newEmail = "updated_" + testEmail;
            boolean updated = mysqlConnector.updateUserEmail(testUsername, newEmail);
            if (updated) {
                log.info("Successfully updated email for user: {}", testUsername);
            }
            
            // Delete
            boolean deleted = mysqlConnector.deleteUser(testUsername);
            if (deleted) {
                log.info("Successfully deleted test user: {}", testUsername);
            }
            
        } catch (MySqlException e) {
            log.error("MySQL error occurred: ", e);
        } catch (Exception e) {
            log.error("Unexpected error occurred with MySQL: ", e);
        } finally {
            if (mysqlConnector != null) mysqlConnector.disconnect();
        }
    }
}