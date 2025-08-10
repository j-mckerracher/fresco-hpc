/**
 * Centralized error handling utilities for FRESCO
 * 
 * This module provides consistent error handling, logging, and user-friendly
 * error messages throughout the application.
 */

import { AppError, ErrorType } from '../types';

// Re-export ErrorType for convenience
export { ErrorType };

/**
 * Error handler class for managing application errors
 */
export class ErrorHandler {
  private static instance: ErrorHandler;
  private errorListeners: Array<(error: AppError) => void> = [];

  /**
   * Get the singleton instance of ErrorHandler
   */
  static getInstance(): ErrorHandler {
    if (!ErrorHandler.instance) {
      ErrorHandler.instance = new ErrorHandler();
    }
    return ErrorHandler.instance;
  }

  /**
   * Handle an error and create a structured AppError
   * 
   * @param error - The error to handle
   * @param context - Context where the error occurred
   * @param type - Type of error
   * @returns Structured AppError object
   */
  static handle(error: unknown, context: string, type: ErrorType = ErrorType.Unknown): AppError {
    const appError: AppError = {
      type,
      message: ErrorHandler.getUserFriendlyMessage(error, type),
      details: error instanceof Error ? error.message : String(error),
      timestamp: new Date(),
      stack: error instanceof Error ? error.stack : undefined,
      context: { context }
    };

    // Log the error (in production, this would go to a logging service)
    console.error(`[${context}] ${type.toUpperCase()}:`, appError);

    // Notify listeners
    ErrorHandler.getInstance().notifyListeners(appError);

    return appError;
  }

  /**
   * Convert an error to a user-friendly message
   * 
   * @param error - The error to convert
   * @param type - Type of error
   * @returns User-friendly error message
   */
  static getUserFriendlyMessage(error: unknown, type: ErrorType): string {
    if (error instanceof Error) {
      // Handle specific error types
      switch (type) {
        case ErrorType.Database:
          return 'Database connection failed. Please check your internet connection and try again.';
        case ErrorType.API:
          return 'Unable to connect to the data service. Please try again later.';
        case ErrorType.Network:
          return 'Network connection failed. Please check your internet connection.';
        case ErrorType.Validation:
          return 'Invalid data provided. Please check your inputs and try again.';
        case ErrorType.Visualization:
          return 'Unable to render the chart. Please try refreshing the page.';
        default:
          // For unknown errors, check if the error message is user-friendly
          if (error.message.includes('timeout')) {
            return 'The operation took too long. Please try again.';
          }
          if (error.message.includes('network')) {
            return 'Network connection failed. Please check your internet connection.';
          }
          if (error.message.includes('permission')) {
            return 'Permission denied. Please check your access rights.';
          }
          return 'An unexpected error occurred. Please try again.';
      }
    }

    return 'An unexpected error occurred. Please try again.';
  }

  /**
   * Add an error listener
   * 
   * @param listener - Function to call when an error occurs
   */
  addErrorListener(listener: (error: AppError) => void): void {
    this.errorListeners.push(listener);
  }

  /**
   * Remove an error listener
   * 
   * @param listener - Function to remove
   */
  removeErrorListener(listener: (error: AppError) => void): void {
    const index = this.errorListeners.indexOf(listener);
    if (index > -1) {
      this.errorListeners.splice(index, 1);
    }
  }

  /**
   * Notify all error listeners
   * 
   * @param error - Error to notify about
   */
  private notifyListeners(error: AppError): void {
    this.errorListeners.forEach(listener => {
      try {
        listener(error);
      } catch (err) {
        console.error('Error in error listener:', err);
      }
    });
  }
}

/**
 * Create a standardized error for async operations
 * 
 * @param operation - The operation that failed
 * @param error - The error that occurred
 * @param context - Context where the error occurred
 * @returns Promise that rejects with a structured error
 */
export function createAsyncError(
  operation: string,
  error: unknown,
  context: string
): Promise<never> {
  const appError = ErrorHandler.handle(error, `${context}.${operation}`, ErrorType.Unknown);
  return Promise.reject(appError);
}

/**
 * Wrap an async function with error handling
 * 
 * @param fn - Async function to wrap
 * @param context - Context for error reporting
 * @returns Wrapped function that handles errors
 */
export function withErrorHandling<T extends unknown[], R>(
  fn: (...args: T) => Promise<R>,
  context: string
): (...args: T) => Promise<R> {
  return async (...args: T): Promise<R> => {
    try {
      return await fn(...args);
    } catch (error) {
      throw ErrorHandler.handle(error, context);
    }
  };
}

/**
 * Create a safe async function that doesn't throw
 * 
 * @param fn - Async function to make safe
 * @param context - Context for error reporting
 * @returns Function that returns a result or error
 */
export function safeAsync<T extends unknown[], R>(
  fn: (...args: T) => Promise<R>,
  context: string
): (...args: T) => Promise<{ success: true; data: R } | { success: false; error: AppError }> {
  return async (...args: T) => {
    try {
      const data = await fn(...args);
      return { success: true, data };
    } catch (error) {
      return { success: false, error: ErrorHandler.handle(error, context) };
    }
  };
}

/**
 * Validate a value and throw a structured error if invalid
 * 
 * @param value - Value to validate
 * @param validator - Validation function
 * @param context - Context for error reporting
 * @param errorMessage - Custom error message
 */
export function validateOrThrow<T>(
  value: T,
  validator: (value: T) => boolean,
  context: string,
  errorMessage: string
): void {
  if (!validator(value)) {
    throw ErrorHandler.handle(
      new Error(errorMessage),
      context,
      ErrorType.Validation
    );
  }
}

/**
 * Retry an async operation with exponential backoff
 * 
 * @param operation - The operation to retry
 * @param maxRetries - Maximum number of retries
 * @param context - Context for error reporting
 * @returns Promise resolving to the operation result
 */
export async function retryAsync<T>(
  operation: () => Promise<T>,
  maxRetries: number = 3,
  context: string = 'retryAsync'
): Promise<T> {
  let lastError: unknown;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      
      if (attempt === maxRetries) {
        break;
      }
      
      // Exponential backoff: 1s, 2s, 4s, etc.
      const delay = Math.pow(2, attempt - 1) * 1000;
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  throw ErrorHandler.handle(lastError, `${context}.retry`, ErrorType.Unknown);
}

/**
 * Default error handler for unhandled promise rejections
 */
export function setupGlobalErrorHandling(): void {
  if (typeof window !== 'undefined') {
    window.addEventListener('unhandledrejection', (event) => {
      const error = ErrorHandler.handle(
        event.reason,
        'window.unhandledrejection',
        ErrorType.Unknown
      );
      console.error('Unhandled promise rejection:', error);
    });

    window.addEventListener('error', (event) => {
      const error = ErrorHandler.handle(
        event.error,
        'window.error',
        ErrorType.Unknown
      );
      console.error('Unhandled error:', error);
    });
  }
}