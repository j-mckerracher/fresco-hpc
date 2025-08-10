import React from 'react';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { ErrorBoundary, withErrorBoundary, SimpleErrorFallback } from '../ErrorBoundary';
import { AppError, ErrorType } from '../../types';

// Mock ErrorHandler
vi.mock('../../utils/errorHandler', () => ({
  ErrorHandler: {
    handle: vi.fn((error, context, type) => ({
      message: error.message || 'Test error',
      type: type || 'unknown',
      timestamp: new Date('2024-01-01T00:00:00Z'),
      context,
      details: error.details,
    })),
  },
}));

// Test component that throws an error
const ThrowingComponent: React.FC<{ shouldThrow?: boolean; errorMessage?: string }> = ({ 
  shouldThrow = false, 
  errorMessage = 'Test error' 
}) => {
  if (shouldThrow) {
    throw new Error(errorMessage);
  }
  return <div data-testid="working-component">Component is working</div>;
};

describe('ErrorBoundary', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Suppress console.error for tests
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  describe('when no error occurs', () => {
    it('should render children normally', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent />
        </ErrorBoundary>
      );

      expect(screen.getByTestId('working-component')).toBeInTheDocument();
    });
  });

  describe('when error occurs', () => {
    it('should catch error and display default fallback', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent shouldThrow={true} errorMessage="Something went wrong" />
        </ErrorBoundary>
      );

      expect(screen.getByText('Something went wrong')).toBeInTheDocument();
      expect(screen.getByText('Test error')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Try again' })).toBeInTheDocument();
    });

    it('should call onError callback when provided', () => {
      const onErrorSpy = vi.fn();

      render(
        <ErrorBoundary onError={onErrorSpy}>
          <ThrowingComponent shouldThrow={true} />
        </ErrorBoundary>
      );

      expect(onErrorSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          message: 'Test error',
          type: 'unknown',
        }),
        expect.any(Object)
      );
    });

    it('should reset error when try again is clicked', () => {
      const TestWrapper = () => {
        const [shouldThrow, setShouldThrow] = React.useState(true);
        
        return (
          <div>
            <button onClick={() => setShouldThrow(false)}>Fix Component</button>
            <ErrorBoundary>
              <ThrowingComponent shouldThrow={shouldThrow} />
            </ErrorBoundary>
          </div>
        );
      };

      render(<TestWrapper />);

      // Error should be displayed
      expect(screen.getByText('Something went wrong')).toBeInTheDocument();

      // Fix the component
      fireEvent.click(screen.getByText('Fix Component'));

      // Reset error
      fireEvent.click(screen.getByRole('button', { name: 'Try again' }));

      // Component should render normally
      expect(screen.getByTestId('working-component')).toBeInTheDocument();
    });

    it('should show/hide error details', () => {
      render(
        <ErrorBoundary>
          <ThrowingComponent shouldThrow={true} />
        </ErrorBoundary>
      );

      // Details should be hidden initially
      expect(screen.queryByText('Error Details:')).not.toBeInTheDocument();

      // Click show details
      fireEvent.click(screen.getByRole('button', { name: 'Show details' }));

      // Details should be visible
      expect(screen.getByText('Error Details:')).toBeInTheDocument();
      expect(screen.getByText('Type:')).toBeInTheDocument();
      expect(screen.getByText('Time:')).toBeInTheDocument();

      // Click hide details
      fireEvent.click(screen.getByRole('button', { name: 'Hide details' }));

      // Details should be hidden again
      expect(screen.queryByText('Error Details:')).not.toBeInTheDocument();
    });

    it('should display error details when available', () => {
      // Create an error with details
      const errorWithDetails = new Error('Detailed error');
      (errorWithDetails as any).details = 'Additional error information';

      const ThrowingWithDetails = () => {
        throw errorWithDetails;
      };

      render(
        <ErrorBoundary>
          <ThrowingWithDetails />
        </ErrorBoundary>
      );

      // Show details
      fireEvent.click(screen.getByRole('button', { name: 'Show details' }));

      expect(screen.getByText('Details:')).toBeInTheDocument();
    });
  });

  describe('custom fallback component', () => {
    const CustomFallback: React.FC<{ error: AppError; resetError: () => void }> = ({ error, resetError }) => (
      <div data-testid="custom-fallback">
        <h1>Custom Error: {error.message}</h1>
        <button onClick={resetError}>Custom Reset</button>
      </div>
    );

    it('should use custom fallback when provided', () => {
      render(
        <ErrorBoundary fallback={CustomFallback}>
          <ThrowingComponent shouldThrow={true} />
        </ErrorBoundary>
      );

      expect(screen.getByTestId('custom-fallback')).toBeInTheDocument();
      expect(screen.getByText('Custom Error: Test error')).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Custom Reset' })).toBeInTheDocument();
    });

    it('should call resetError from custom fallback', () => {
      const TestWrapper = () => {
        const [shouldThrow, setShouldThrow] = React.useState(true);
        
        return (
          <div>
            <button onClick={() => setShouldThrow(false)}>Fix Component</button>
            <ErrorBoundary fallback={CustomFallback}>
              <ThrowingComponent shouldThrow={shouldThrow} />
            </ErrorBoundary>
          </div>
        );
      };

      render(<TestWrapper />);

      // Fix the component
      fireEvent.click(screen.getByText('Fix Component'));

      // Reset error using custom button
      fireEvent.click(screen.getByRole('button', { name: 'Custom Reset' }));

      // Component should render normally
      expect(screen.getByTestId('working-component')).toBeInTheDocument();
    });
  });
});

describe('withErrorBoundary HOC', () => {
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  it('should wrap component with error boundary', () => {
    const WrappedComponent = withErrorBoundary(ThrowingComponent);

    render(<WrappedComponent />);

    expect(screen.getByTestId('working-component')).toBeInTheDocument();
  });

  it('should catch errors in wrapped component', () => {
    const WrappedComponent = withErrorBoundary(ThrowingComponent);

    render(<WrappedComponent shouldThrow={true} />);

    expect(screen.getByText('Something went wrong')).toBeInTheDocument();
  });

  it('should use custom fallback when provided', () => {
    const CustomFallback: React.FC<{ error: AppError; resetError: () => void }> = ({ error }) => (
      <div data-testid="hoc-custom-fallback">HOC Error: {error.message}</div>
    );

    const WrappedComponent = withErrorBoundary(ThrowingComponent, CustomFallback);

    render(<WrappedComponent shouldThrow={true} />);

    expect(screen.getByTestId('hoc-custom-fallback')).toBeInTheDocument();
    expect(screen.getByText('HOC Error: Test error')).toBeInTheDocument();
  });

  it('should set correct displayName', () => {
    const TestComponent = () => <div>Test</div>;
    TestComponent.displayName = 'TestComponent';

    const WrappedComponent = withErrorBoundary(TestComponent);

    expect(WrappedComponent.displayName).toBe('withErrorBoundary(TestComponent)');
  });

  it('should handle components without displayName', () => {
    function AnonymousComponent() {
      return <div>Anonymous</div>;
    }

    const WrappedComponent = withErrorBoundary(AnonymousComponent);

    expect(WrappedComponent.displayName).toBe('withErrorBoundary(AnonymousComponent)');
  });
});

describe('SimpleErrorFallback', () => {
  const mockError: AppError = {
    message: 'Simple error message',
    type: ErrorType.Unknown,
    timestamp: new Date('2024-01-01T00:00:00Z'),
    context: 'test-context',
  };

  it('should render error message', () => {
    const resetError = vi.fn();

    render(<SimpleErrorFallback error={mockError} resetError={resetError} />);

    expect(screen.getByText('Simple error message')).toBeInTheDocument();
  });

  it('should call resetError when try again is clicked', () => {
    const resetError = vi.fn();

    render(<SimpleErrorFallback error={mockError} resetError={resetError} />);

    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));

    expect(resetError).toHaveBeenCalledTimes(1);
  });

  it('should have proper styling classes', () => {
    const resetError = vi.fn();

    render(<SimpleErrorFallback error={mockError} resetError={resetError} />);

    const container = screen.getByText('Simple error message').closest('div');
    expect(container).toHaveClass('p-4', 'border', 'border-red-200', 'rounded-md', 'bg-red-50');
  });
});

describe('ErrorBoundary edge cases', () => {
  beforeEach(() => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  it('should handle null error', () => {
    const ErrorBoundaryWithNullError = () => {
      const [hasError, setHasError] = React.useState(false);
      
      if (hasError) {
        throw null;
      }
      
      return (
        <div>
          <button onClick={() => setHasError(true)}>Throw Null</button>
          <div data-testid="content">Content</div>
        </div>
      );
    };

    render(
      <ErrorBoundary>
        <ErrorBoundaryWithNullError />
      </ErrorBoundary>
    );

    fireEvent.click(screen.getByText('Throw Null'));

    // Should still show error boundary
    expect(screen.getByText('Something went wrong')).toBeInTheDocument();
  });

  it('should handle error without message', () => {
    const ThrowingNoMessage = () => {
      const error = new Error();
      error.message = '';
      throw error;
    };

    render(
      <ErrorBoundary>
        <ThrowingNoMessage />
      </ErrorBoundary>
    );

    expect(screen.getByText('Something went wrong')).toBeInTheDocument();
  });

  it('should handle multiple consecutive errors', () => {
    let errorCount = 0;
    const MultipleErrorComponent = () => {
      errorCount++;
      throw new Error(`Error ${errorCount}`);
    };

    const { rerender } = render(
      <ErrorBoundary>
        <MultipleErrorComponent />
      </ErrorBoundary>
    );

    expect(screen.getByText(`Test error`)).toBeInTheDocument();

    // Reset and throw again
    fireEvent.click(screen.getByRole('button', { name: 'Try again' }));

    rerender(
      <ErrorBoundary>
        <MultipleErrorComponent />
      </ErrorBoundary>
    );

    expect(screen.getByText(`Test error`)).toBeInTheDocument();
  });
});