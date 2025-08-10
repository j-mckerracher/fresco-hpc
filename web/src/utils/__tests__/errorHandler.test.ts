import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  ErrorHandler,
  ErrorType,
  createAsyncError,
  withErrorHandling,
  safeAsync,
  validateOrThrow,
  retryAsync,
  setupGlobalErrorHandling,
} from '../errorHandler';
import { AppError } from '../../types';

// Mock console methods
const consoleMocks = {
  error: vi.fn(),
};

describe('ErrorHandler', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    global.console = {
      ...global.console,
      ...consoleMocks,
    };
    
    // Reset singleton
    (ErrorHandler as any).instance = undefined;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('singleton pattern', () => {
    it('should return the same instance', () => {
      const instance1 = ErrorHandler.getInstance();
      const instance2 = ErrorHandler.getInstance();
      
      expect(instance1).toBe(instance2);
    });
  });

  describe('handle method', () => {
    it('should handle Error objects', () => {
      const originalError = new Error('Test error message');
      originalError.stack = 'Error stack trace';
      
      const appError = ErrorHandler.handle(originalError, 'test-context', ErrorType.Database);
      
      expect(appError).toEqual({
        type: ErrorType.Database,
        message: 'Database connection failed. Please check your internet connection and try again.',
        details: 'Test error message',
        timestamp: expect.any(Date),
        stack: 'Error stack trace',
        context: { context: 'test-context' },
      });
    });

    it('should handle string errors', () => {
      const appError = ErrorHandler.handle('String error', 'test-context', ErrorType.API);
      
      expect(appError).toEqual({
        type: ErrorType.API,
        message: 'Unable to connect to the data service. Please try again later.',
        details: 'String error',
        timestamp: expect.any(Date),
        stack: undefined,
        context: { context: 'test-context' },
      });
    });

    it('should handle unknown error types', () => {
      const appError = ErrorHandler.handle({ custom: 'error' }, 'test-context', ErrorType.Unknown);
      
      expect(appError).toEqual({
        type: ErrorType.Unknown,
        message: 'An unexpected error occurred. Please try again.',
        details: '[object Object]',
        timestamp: expect.any(Date),
        stack: undefined,
        context: { context: 'test-context' },
      });
    });

    it('should log the error', () => {
      ErrorHandler.handle(new Error('Test'), 'test-context', ErrorType.Network);
      
      expect(consoleMocks.error).toHaveBeenCalledWith(
        '[test-context] NETWORK:',
        expect.any(Object)
      );
    });

    it('should notify error listeners', () => {
      const listener = vi.fn();
      const handler = ErrorHandler.getInstance();
      handler.addErrorListener(listener);
      
      const appError = ErrorHandler.handle(new Error('Test'), 'test-context');
      
      expect(listener).toHaveBeenCalledWith(appError);
    });
  });

  describe('getUserFriendlyMessage', () => {
    it('should return database error message', () => {
      const error = new Error('Connection failed');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Database);
      
      expect(message).toBe('Database connection failed. Please check your internet connection and try again.');
    });

    it('should return API error message', () => {
      const error = new Error('API failed');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.API);
      
      expect(message).toBe('Unable to connect to the data service. Please try again later.');
    });

    it('should return network error message', () => {
      const error = new Error('Network failed');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Network);
      
      expect(message).toBe('Network connection failed. Please check your internet connection.');
    });

    it('should return validation error message', () => {
      const error = new Error('Invalid input');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Validation);
      
      expect(message).toBe('Invalid data provided. Please check your inputs and try again.');
    });

    it('should return visualization error message', () => {
      const error = new Error('Chart failed');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Visualization);
      
      expect(message).toBe('Unable to render the chart. Please try refreshing the page.');
    });

    it('should handle timeout errors', () => {
      const error = new Error('Operation timeout exceeded');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Unknown);
      
      expect(message).toBe('The operation took too long. Please try again.');
    });

    it('should handle network errors in message', () => {
      const error = new Error('network error occurred');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Unknown);
      
      expect(message).toBe('Network connection failed. Please check your internet connection.');
    });

    it('should handle permission errors', () => {
      const error = new Error('permission denied');
      const message = ErrorHandler.getUserFriendlyMessage(error, ErrorType.Unknown);
      
      expect(message).toBe('Permission denied. Please check your access rights.');
    });

    it('should handle non-Error objects', () => {
      const message = ErrorHandler.getUserFriendlyMessage('string error', ErrorType.Unknown);
      
      expect(message).toBe('An unexpected error occurred. Please try again.');
    });
  });

  describe('error listeners', () => {
    it('should add and notify error listeners', () => {
      const handler = ErrorHandler.getInstance();
      const listener1 = vi.fn();
      const listener2 = vi.fn();
      
      handler.addErrorListener(listener1);
      handler.addErrorListener(listener2);
      
      const appError = ErrorHandler.handle(new Error('Test'), 'test-context');
      
      expect(listener1).toHaveBeenCalledWith(appError);
      expect(listener2).toHaveBeenCalledWith(appError);
    });

    it('should remove error listeners', () => {
      const handler = ErrorHandler.getInstance();
      const listener = vi.fn();
      
      handler.addErrorListener(listener);
      handler.removeErrorListener(listener);
      
      ErrorHandler.handle(new Error('Test'), 'test-context');
      
      expect(listener).not.toHaveBeenCalled();
    });

    it('should handle errors in listeners gracefully', () => {
      const handler = ErrorHandler.getInstance();
      const faultyListener = vi.fn(() => {
        throw new Error('Listener error');
      });
      const goodListener = vi.fn();
      
      handler.addErrorListener(faultyListener);
      handler.addErrorListener(goodListener);
      
      const appError = ErrorHandler.handle(new Error('Test'), 'test-context');
      
      expect(faultyListener).toHaveBeenCalled();
      expect(goodListener).toHaveBeenCalledWith(appError);
      expect(consoleMocks.error).toHaveBeenCalledWith('Error in error listener:', expect.any(Error));
    });

    it('should handle removing non-existent listener', () => {
      const handler = ErrorHandler.getInstance();
      const listener = vi.fn();
      
      // Should not throw
      expect(() => handler.removeErrorListener(listener)).not.toThrow();
    });
  });
});

describe('createAsyncError', () => {
  it('should create and reject with AppError', async () => {
    const operation = 'testOperation';
    const originalError = new Error('Test error');
    const context = 'test-context';
    
    await expect(createAsyncError(operation, originalError, context))
      .rejects.toEqual(expect.objectContaining({
        type: ErrorType.Unknown,
        context: { context: 'test-context.testOperation' },
      }));
  });
});

describe('withErrorHandling', () => {
  it('should return result when function succeeds', async () => {
    const successFn = vi.fn().mockResolvedValue('success');
    const wrappedFn = withErrorHandling(successFn, 'test-context');
    
    const result = await wrappedFn('arg1', 'arg2');
    
    expect(result).toBe('success');
    expect(successFn).toHaveBeenCalledWith('arg1', 'arg2');
  });

  it('should handle errors and throw AppError', async () => {
    const errorFn = vi.fn().mockRejectedValue(new Error('Test error'));
    const wrappedFn = withErrorHandling(errorFn, 'test-context');
    
    await expect(wrappedFn('arg1')).rejects.toEqual(
      expect.objectContaining({
        type: ErrorType.Unknown,
        context: { context: 'test-context' },
      })
    );
    
    expect(errorFn).toHaveBeenCalledWith('arg1');
  });
});

describe('safeAsync', () => {
  it('should return success result when function succeeds', async () => {
    const successFn = vi.fn().mockResolvedValue('success data');
    const safeFn = safeAsync(successFn, 'test-context');
    
    const result = await safeFn('arg1');
    
    expect(result).toEqual({
      success: true,
      data: 'success data',
    });
    expect(successFn).toHaveBeenCalledWith('arg1');
  });

  it('should return error result when function fails', async () => {
    const errorFn = vi.fn().mockRejectedValue(new Error('Test error'));
    const safeFn = safeAsync(errorFn, 'test-context');
    
    const result = await safeFn('arg1');
    
    expect(result).toEqual({
      success: false,
      error: expect.objectContaining({
        type: ErrorType.Unknown,
        context: { context: 'test-context' },
      }),
    });
    expect(errorFn).toHaveBeenCalledWith('arg1');
  });
});

describe('validateOrThrow', () => {
  it('should not throw when validation passes', () => {
    const validator = (value: number) => value > 0;
    
    expect(() => validateOrThrow(5, validator, 'test-context', 'Must be positive'))
      .not.toThrow();
  });

  it('should throw AppError when validation fails', () => {
    const validator = (value: number) => value > 0;
    
    expect(() => validateOrThrow(-1, validator, 'test-context', 'Must be positive'))
      .toThrow();
  });

  it('should throw with correct error type', () => {
    const validator = () => false;
    
    try {
      validateOrThrow('test', validator, 'test-context', 'Always fails');
    } catch (error) {
      expect(error).toEqual(expect.objectContaining({
        type: ErrorType.Validation,
        context: { context: 'test-context' },
      }));
    }
  });
});

describe('retryAsync', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return result on first success', async () => {
    const operation = vi.fn().mockResolvedValue('success');
    
    const result = await retryAsync(operation, 3, 'test-context');
    
    expect(result).toBe('success');
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it('should retry on failure and eventually succeed', async () => {
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Attempt 1'))
      .mockRejectedValueOnce(new Error('Attempt 2'))
      .mockResolvedValueOnce('success');
    
    const resultPromise = retryAsync(operation, 3, 'test-context');
    
    // Fast-forward through delays
    await vi.runAllTimersAsync();
    
    const result = await resultPromise;
    
    expect(result).toBe('success');
    expect(operation).toHaveBeenCalledTimes(3);
  });

  it('should throw after max retries', async () => {
    const operation = vi.fn().mockRejectedValue(new Error('Always fails'));
    
    const resultPromise = retryAsync(operation, 2, 'test-context');
    
    // Fast-forward through delays
    await vi.runAllTimersAsync();
    
    await expect(resultPromise).rejects.toEqual(
      expect.objectContaining({
        context: { context: 'test-context.retry' },
      })
    );
    
    expect(operation).toHaveBeenCalledTimes(2);
  });

  it('should use exponential backoff delays', async () => {
    const operation = vi.fn().mockRejectedValue(new Error('Always fails'));
    
    const resultPromise = retryAsync(operation, 3, 'test-context');
    
    // Check that delays are applied
    expect(operation).toHaveBeenCalledTimes(1);
    
    await vi.advanceTimersByTimeAsync(1000);
    expect(operation).toHaveBeenCalledTimes(2);
    
    await vi.advanceTimersByTimeAsync(2000);
    expect(operation).toHaveBeenCalledTimes(3);
    
    await expect(resultPromise).rejects.toThrow();
  });

  it('should use default parameters', async () => {
    const operation = vi.fn().mockResolvedValue('success');
    
    const result = await retryAsync(operation);
    
    expect(result).toBe('success');
    expect(operation).toHaveBeenCalledTimes(1);
  });
});

describe('setupGlobalErrorHandling', () => {
  let mockAddEventListener: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockAddEventListener = vi.fn();
    
    global.window = {
      addEventListener: mockAddEventListener,
    } as any;
  });

  it('should set up global error handlers', () => {
    setupGlobalErrorHandling();
    
    expect(mockAddEventListener).toHaveBeenCalledWith('unhandledrejection', expect.any(Function));
    expect(mockAddEventListener).toHaveBeenCalledWith('error', expect.any(Function));
  });

  it('should handle unhandled promise rejections', () => {
    setupGlobalErrorHandling();
    
    const unhandledRejectionHandler = mockAddEventListener.mock.calls.find(
      call => call[0] === 'unhandledrejection'
    )?.[1];
    
    expect(unhandledRejectionHandler).toBeDefined();
    
    const mockEvent = {
      reason: new Error('Unhandled rejection'),
    };
    
    unhandledRejectionHandler(mockEvent);
    
    expect(consoleMocks.error).toHaveBeenCalledWith(
      'Unhandled promise rejection:',
      expect.any(Object)
    );
  });

  it('should handle unhandled errors', () => {
    setupGlobalErrorHandling();
    
    const errorHandler = mockAddEventListener.mock.calls.find(
      call => call[0] === 'error'
    )?.[1];
    
    expect(errorHandler).toBeDefined();
    
    const mockEvent = {
      error: new Error('Unhandled error'),
    };
    
    errorHandler(mockEvent);
    
    expect(consoleMocks.error).toHaveBeenCalledWith(
      'Unhandled error:',
      expect.any(Object)
    );
  });

  it('should not set up handlers when window is not available', () => {
    global.window = undefined as any;
    
    expect(() => setupGlobalErrorHandling()).not.toThrow();
    expect(mockAddEventListener).not.toHaveBeenCalled();
  });
});