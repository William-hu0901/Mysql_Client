package org.daodao.jdbc.connectors;

import org.daodao.jdbc.config.MySqlConfig;
import org.daodao.jdbc.exceptions.MySqlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class MySqlConnector {
    
    private static final Logger log = LoggerFactory.getLogger(MySqlConnector.class);
    
    private MySqlConfig config;
    private Connection connection;
    
    public MySqlConnector() {
        this.config = new MySqlConfig();
    }
    
    public MySqlConnector(MySqlConfig config) {
        this.config = config;
    }
    
    public void connect() {
        try {
            if (connection != null && !connection.isClosed()) {
                log.info("MySQL connection already established");
                return;
            }
            
            Properties connectionProps = new Properties();
            connectionProps.put("user", config.getUsername());
            connectionProps.put("password", config.getPassword());
            connectionProps.put("useSSL", "false");
            connectionProps.put("allowPublicKeyRetrieval", "true");
            connectionProps.put("serverTimezone", "UTC");
            
            // First try to connect to the specific database
            try {
                connection = DriverManager.getConnection(config.getJdbcUrl(), connectionProps);
                log.info("Connected to MySQL database at: {}", config.getJdbcUrl());
            } catch (SQLException e) {
                // If database doesn't exist, try to create it
                if (e.getMessage().contains("Unknown database")) {
                    log.info("Database '{}' does not exist, attempting to create it", config.getDatabase());
                    
                    // Connect to MySQL server without specifying database
                    String serverUrl = String.format("jdbc:mysql://%s:%d?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC", 
                                                   config.getHost(), config.getPort());
                    try (Connection serverConnection = DriverManager.getConnection(serverUrl, connectionProps)) {
                        try (Statement stmt = serverConnection.createStatement()) {
                            stmt.execute("CREATE DATABASE " + config.getDatabase());
                            log.info("Database '{}' created successfully", config.getDatabase());
                        }
                    }
                    
                    // Now connect to the newly created database
                    connection = DriverManager.getConnection(config.getJdbcUrl(), connectionProps);
                    log.info("Connected to MySQL database at: {}", config.getJdbcUrl());
                } else {
                    throw e;
                }
            }
            
        } catch (SQLException e) {
            log.error("Failed to connect to MySQL database: {}", e.getMessage());
            throw new MySqlException("Failed to connect to MySQL database", e);
        }
    }
    
    public void disconnect() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                log.info("Disconnected from MySQL database");
            }
        } catch (SQLException e) {
            log.error("Error closing MySQL connection: {}", e.getMessage());
            throw new MySqlException("Error closing MySQL connection", e);
        }
    }
    
    public boolean isDatabaseEmpty() {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables(null, null, "users", new String[]{"TABLE"});
            boolean hasUsersTable = tables.next();
            tables.close();
            
            if (!hasUsersTable) {
                return true;
            }
            
            ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM users");
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            
            return count == 0;
            
        } catch (SQLException e) {
            log.error("Error checking if database is empty: {}", e.getMessage());
            throw new MySqlException("Error checking if database is empty", e);
        }
    }
    
    public void initializeDatabase() {
        log.info("Initializing MySQL database with schema, tables, and sample data");
        
        // Create users table
        String createUsersTable = """
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL UNIQUE,
                email VARCHAR(100) NOT NULL UNIQUE,
                age INT,
                city VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """;
        execute(createUsersTable);
        log.info("Users table created successfully");
        
        // Create products table
        String createProductsTable = """
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                category VARCHAR(50),
                price DECIMAL(10,2),
                stock_quantity INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """;
        execute(createProductsTable);
        log.info("Products table created successfully");
        
        // Create indexes (MySQL doesn't support IF NOT EXISTS for CREATE INDEX)
        try {
            String createEmailIndex = "CREATE INDEX idx_users_email ON users(email)";
            execute(createEmailIndex);
        } catch (Exception e) {
            log.debug("Email index may already exist: {}", e.getMessage());
        }
        
        try {
            String createUsernameIndex = "CREATE INDEX idx_users_username ON users(username)";
            execute(createUsernameIndex);
        } catch (Exception e) {
            log.debug("Username index may already exist: {}", e.getMessage());
        }
        
        try {
            String createProductsCategoryIndex = "CREATE INDEX idx_products_category ON products(category)";
            execute(createProductsCategoryIndex);
        } catch (Exception e) {
            log.debug("Products category index may already exist: {}", e.getMessage());
        }
        
        try {
            String createProductsPriceIndex = "CREATE INDEX idx_products_price ON products(price)";
            execute(createProductsPriceIndex);
        } catch (Exception e) {
            log.debug("Products price index may already exist: {}", e.getMessage());
        }
        
        log.info("Indexes created successfully");
        
        // Create view
        String createView = """
            CREATE OR REPLACE VIEW user_product_summary AS
            SELECT 
                u.id as user_id,
                u.username,
                u.email,
                u.city,
                COUNT(p.id) as product_count,
                COALESCE(SUM(p.price), 0) as total_product_value
            FROM users u
            LEFT JOIN products p ON u.id = p.id
            GROUP BY u.id, u.username, u.email, u.city
            """;
        execute(createView);
        log.info("View created successfully");
        
        // Insert sample data
        insertSampleData();
        
        log.info("Database initialization completed successfully");
    }
    
    private void insertSampleData() {
        // Insert sample users
        String insertUsers = """
            INSERT IGNORE INTO users (username, email, age, city) VALUES
            ('john_doe', 'john.doe@example.com', 30, 'New York'),
            ('jane_smith', 'jane.smith@example.com', 25, 'Los Angeles'),
            ('bob_wilson', 'bob.wilson@example.com', 35, 'Chicago'),
            ('alice_brown', 'alice.brown@example.com', 28, 'Houston'),
            ('charlie_davis', 'charlie.davis@example.com', 32, 'Phoenix')
            """;
        execute(insertUsers);
        
        // Insert sample products
        String insertProducts = """
            INSERT IGNORE INTO products (name, category, price, stock_quantity) VALUES
            ('Laptop', 'Electronics', 999.99, 50),
            ('Smartphone', 'Electronics', 699.99, 100),
            ('Desk Chair', 'Furniture', 199.99, 25),
            ('Coffee Maker', 'Appliances', 89.99, 75),
            ('Headphones', 'Electronics', 149.99, 60)
            """;
        execute(insertProducts);
        
        log.info("Sample data inserted successfully");
    }
    
    public void execute(String sql) throws MySqlException {
        try {
            Statement stmt = connection.createStatement();
            log.debug("Executing SQL: {}", sql);
            stmt.execute(sql);
            stmt.close();
        } catch (SQLException e) {
            log.error("Error executing SQL: {}", sql);
            throw new MySqlException("Error executing SQL: " + sql, e);
        }
    }
    
    public ResultSet executeQuery(String sql) throws SQLException {
        log.debug("Executing query: {}", sql);
        return connection.createStatement().executeQuery(sql);
    }
    
    // CRUD Operations
    
    public List<String> findAllUsers() {
        List<String> users = new ArrayList<>();
        String sql = "SELECT username, email, age, city FROM users ORDER BY username";
        
        try {
            ResultSet rs = executeQuery(sql);
            while (rs.next()) {
                String user = String.format("User: %s, Email: %s, Age: %d, City: %s",
                    rs.getString("username"),
                    rs.getString("email"),
                    rs.getInt("age"),
                    rs.getString("city"));
                users.add(user);
            }
            rs.close();
        } catch (SQLException e) {
            log.error("Error retrieving users: {}", e.getMessage());
            throw new MySqlException("Error retrieving users", e);
        }
        
        return users;
    }
    
    public boolean insertUser(String username, String email, int age, String city) {
        String sql = "INSERT IGNORE INTO users (username, email, age, city) VALUES (?, ?, ?, ?)";
        
        try {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, username);
            pstmt.setString(2, email);
            pstmt.setInt(3, age);
            pstmt.setString(4, city);
            
            int rowsAffected = pstmt.executeUpdate();
            pstmt.close();
            log.info("User inserted successfully: {}", username);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            log.error("Error inserting user: {}", e.getMessage());
            throw new MySqlException("Error inserting user", e);
        }
    }
    
    public boolean updateUserEmail(String username, String newEmail) {
        String sql = "UPDATE users SET email = ?, updated_at = CURRENT_TIMESTAMP WHERE username = ?";
        
        try {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, newEmail);
            pstmt.setString(2, username);
            
            int rowsAffected = pstmt.executeUpdate();
            pstmt.close();
            log.info("User email updated successfully: {} -> {}", username, newEmail);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            log.error("Error updating user email: {}", e.getMessage());
            throw new MySqlException("Error updating user email", e);
        }
    }
    
    public boolean deleteUser(String username) {
        String sql = "DELETE FROM users WHERE username = ?";
        
        try {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, username);
            
            int rowsAffected = pstmt.executeUpdate();
            pstmt.close();
            log.info("User deleted successfully: {}", username);
            return rowsAffected > 0;
            
        } catch (SQLException e) {
            log.error("Error deleting user: {}", e.getMessage());
            throw new MySqlException("Error deleting user", e);
        }
    }
    
    public List<String> findUsersByCity(String city) {
        List<String> users = new ArrayList<>();
        String sql = "SELECT username, email, age FROM users WHERE city = ? ORDER BY username";
        
        try {
            PreparedStatement pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, city);
            
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String user = String.format("User: %s, Email: %s, Age: %d",
                    rs.getString("username"),
                    rs.getString("email"),
                    rs.getInt("age"));
                users.add(user);
            }
            rs.close();
            pstmt.close();
        } catch (SQLException e) {
            log.error("Error finding users by city: {}", e.getMessage());
            throw new MySqlException("Error finding users by city", e);
        }
        
        return users;
    }
    
    public int getUserCount() {
        String sql = "SELECT COUNT(*) FROM users";
        
        try {
            ResultSet rs = executeQuery(sql);
            rs.next();
            int count = rs.getInt(1);
            rs.close();
            return count;
        } catch (SQLException e) {
            log.error("Error getting user count: {}", e.getMessage());
            throw new MySqlException("Error getting user count", e);
        }
    }
}