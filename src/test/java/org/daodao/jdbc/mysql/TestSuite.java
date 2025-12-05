package org.daodao.jdbc.mysql;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;
import org.junit.platform.suite.api.SuiteDisplayName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Suite for MySQL JDBC Client Tests
 * 
 * This test suite includes all MySQL-related test cases:
 * - Basic CRUD operations
 * - MySQL 8.0+ new features (Window Functions, CTE, JSON, Generated Columns)
 * - Security and Performance features (Authentication, Performance Schema, Histograms)
 * - Mockito-based unit tests for edge cases and error handling
 * 
 * The suite ensures comprehensive testing of MySQL connectivity and functionality
 * while maintaining high test coverage and reliability.
 */
@Suite
@SuiteDisplayName("MySQL JDBC Client Test Suite")
@SelectClasses({
    MySqlBasicCRUDTest.class,
    MySqlNewFeaturesTest.class,
    MySqlSecurityPerformanceTest.class,
    MySqlConnectorMockitoTest.class
})
public class TestSuite {
    
    private static final Logger logger = LoggerFactory.getLogger(TestSuite.class);
    
    static {
        logger.info("Starting MySQL JDBC Client Test Suite");
        logger.info("Test Classes Included:");
        logger.info("  - MySqlBasicCRUDTest: Basic CRUD operations testing");
        logger.info("  - MySqlNewFeaturesTest: MySQL 8.0+ new features testing");
        logger.info("  - MySqlSecurityPerformanceTest: Security and Performance features testing");
        logger.info("  - MySqlConnectorMockitoTest: Mockito-based unit testing");
        logger.info("Suite initialization completed");
    }
}