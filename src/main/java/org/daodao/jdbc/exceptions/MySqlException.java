package org.daodao.jdbc.exceptions;

public class MySqlException extends RuntimeException {
    
    public MySqlException(String message) {
        super(message);
    }
    
    public MySqlException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public MySqlException(Throwable cause) {
        super(cause);
    }
}