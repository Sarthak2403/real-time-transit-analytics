package com.example.transit;

public class InvalidEventException extends RuntimeException {
    public InvalidEventException(String message) { super(message); }
    public InvalidEventException(String message, Throwable cause) { super(message, cause); }
}
