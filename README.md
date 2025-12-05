# MySQL JDBC Client

This is a Java client project for testing MySQL functionality, providing complete MySQL database connection and CRUD operation features.

## Project Features

- **MySQL Database Support**: Complete MySQL connection and operation functionality
- **CRUD Operations**: Full Create, Read, Update, Delete functionality
- **Modern Java**: Built with Java21 and Maven
- **Comprehensive Testing**: Includes integration and unit tests
- **Logging**: SLF4J with Logback for proper logging
- **Configuration Management**: External configuration via application.properties

## MySQL Connection Example

```java
MySqlConnector mysqlConnector = new MySqlConnector();
mysqlConnector.connect();

// Initialize database if empty
if (mysqlConnector.isDatabaseEmpty()) {
    mysqlConnector.initializeDatabase();
}

// Find all users
List<String> users = mysqlConnector.findAllUsers();

// Insert new user
boolean inserted = mysqlConnector.insertUser("john_doe", "john@example.com", 30, "New York");

// Update user email
boolean updated = mysqlConnector.updateUserEmail("john_doe", "john.new@example.com");

// Delete user
boolean deleted = mysqlConnector.deleteUser("john_doe");

// Find users by city
List<String> cityUsers = mysqlConnector.findUsersByCity("New York");

// Get user count
int userCount = mysqlConnector.getUserCount();
```

## Configuration

The application uses `application.properties` for MySQL configuration:

```properties
mysql.host=localhost
mysql.port=3306
mysql.database=testdb
mysql.username=root
mysql.password=your_password
```

## Requirements

- Java 21
- MySQL database running on localhost:3306
- Maven for dependency management

## Project Structure

```
src/main/java/org/daodao/jdbc/
├── JdbcClientMain.java          # Main application entry point
├── config/
│   └── MySqlConfig.java         # MySQL configuration class
├── connectors/
│   └── MySqlConnector.java      # MySQL connection handler
├── exceptions/
│   ├── MySqlException.java       # MySQL exception class
│   └── PropertyException.java   # Property loading exception class
└── util/
    └── Constants.java            # Application constants

src/test/java/org/daodao/jdbc/mysql/
├── MySqlBasicCRUDTest.java       # Basic CRUD operations tests
├── MySqlNewFeaturesTest.java     # MySQL 8.0+ new features tests
├── MySqlSecurityPerformanceTest.java # Security and Performance features tests
├── MySqlConnectorMockitoTest.java  # Mockito unit tests
└── TestSuite.java               # MySQL test suite
```

## Running the Application

### Using Maven
```bash
mvn compile exec:java -Dexec.mainClass="org.daodao.jdbc.JdbcClientMain"
```

## Running Tests

### All Tests
```bash
mvn test
```

### Specific Test Classes
```bash
# Basic CRUD tests
mvn test -Dtest=MySqlBasicCRUDTest

# MySQL new features tests
mvn test -Dtest=MySqlNewFeaturesTest

# Security and Performance tests
mvn test -Dtest=MySqlSecurityPerformanceTest

# Mockito unit tests
mvn test -Dtest=MySqlConnectorMockitoTest

# Complete test suite
mvn test -Dtest=org.daodao.jdbc.mysql.TestSuite
```

## Test Coverage

### MySQL Test Coverage

**Total Tests**: 16+ test cases across multiple test classes
- **100% Pass Rate**: 16 tests in basic suite all pass
- **New Features Tests**: Covering MySQL 8.0+ functionality
- **0 Test Failures**
- **Graceful test skipping when features unavailable**

**Test Categories**:
1. **MySqlBasicCRUDTest**: Core MySQL functionality and CRUD operations
2. **MySqlNewFeaturesTest**: MySQL 8.0+ new features
3. **MySqlSecurityPerformanceTest**: Security and Performance features
4. **MySqlConnectorMockitoTest**: Unit tests with mocked dependencies

## Features

### MySQL Features
- Automatic database initialization
- Schema, table, and index creation
- Data summarization view creation
- Prepared statements for security
- Connection pooling and timeout handling
- MySQL 8.0+ new features testing

### Application Features
- Simple CRUD operations
- Custom exception handling
- Logging with SLF4J and Logback
- MySQL automatic database initialization
- Schema, table, and index creation
- MySQL view creation
- Java21 compatibility
- Comprehensive test coverage

## Troubleshooting

### Common Issues

1. **MySQL Connection Failure**
   - Ensure MySQL is running on localhost:3306
   - Verify connection parameters in application.properties
   - Check database name configuration

2. **Test Failures**
   - Verify MySQL is running
   - Check connection parameters
   - Ensure correct configuration in application.properties

3. **Maven Build Issues**
   - Ensure Java21 is properly installed
   - Check Maven version compatibility (3.6+)

### Verification Commands

```bash
# Check Java version
java -version

# Check Maven version
mvn -version

# Test compilation
mvn compile

# Run specific test
mvn test -Dtest=MySqlBasicCRUDTest
```

## License

This project is for educational and demonstration purposes.