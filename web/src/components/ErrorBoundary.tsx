/**
 * React Error Boundary component for FRESCO
 * 
 * This component catches JavaScript errors anywhere in the child component tree,
 * logs those errors, and displays a fallback UI instead of the component tree that crashed.
 */

import React, { Component, ReactNode } from 'react';
import { AppError, ErrorType } from '../types';
import { ErrorHandler } from '../utils/errorHandler';

interface ErrorBoundaryState {
  hasError: boolean;
  error: AppError | null;
  errorInfo: React.ErrorInfo | null;
}

interface ErrorBoundaryProps {
  children: ReactNode;
  fallback?: React.ComponentType<{ error: AppError; resetError: () => void }>;
  onError?: (error: AppError, errorInfo: React.ErrorInfo) => void;
}

/**
 * Error boundary component that catches React errors and displays fallback UI
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  constructor(props: ErrorBoundaryProps) {
    super(props);
    this.state = {
      hasError: false,
      error: null,
      errorInfo: null
    };
  }

  /**
   * Update state when an error is caught
   */
  static getDerivedStateFromError(error: Error): Partial<ErrorBoundaryState> {
    const appError = ErrorHandler.handle(error, 'ErrorBoundary', ErrorType.Unknown);
    return {
      hasError: true,
      error: appError
    };
  }

  /**
   * Log error details and notify parent component
   */
  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    const appError = ErrorHandler.handle(error, 'ErrorBoundary.componentDidCatch', ErrorType.Unknown);
    
    this.setState({
      error: appError,
      errorInfo
    });

    // Notify parent component if callback provided
    if (this.props.onError) {
      this.props.onError(appError, errorInfo);
    }
  }

  /**
   * Reset error state to allow retry
   */
  resetError = (): void => {
    this.setState({
      hasError: false,
      error: null,
      errorInfo: null
    });
  };

  render(): ReactNode {
    if (this.state.hasError && this.state.error) {
      // Use custom fallback component if provided
      if (this.props.fallback) {
        const FallbackComponent = this.props.fallback;
        return <FallbackComponent error={this.state.error} resetError={this.resetError} />;
      }

      // Default fallback UI
      return <DefaultErrorFallback error={this.state.error} resetError={this.resetError} />;
    }

    return this.props.children;
  }
}

/**
 * Default error fallback component
 */
interface DefaultErrorFallbackProps {
  error: AppError;
  resetError: () => void;
}

const DefaultErrorFallback: React.FC<DefaultErrorFallbackProps> = ({ error, resetError }) => {
  const [showDetails, setShowDetails] = React.useState(false);

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-md w-full space-y-8">
        <div>
          <div className="mx-auto flex items-center justify-center h-12 w-12 rounded-full bg-red-100">
            <svg 
              className="h-6 w-6 text-red-600" 
              fill="none" 
              viewBox="0 0 24 24" 
              stroke="currentColor"
            >
              <path 
                strokeLinecap="round" 
                strokeLinejoin="round" 
                strokeWidth={2} 
                d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z" 
              />
            </svg>
          </div>
          <h2 className="mt-6 text-center text-3xl font-extrabold text-gray-900">
            Something went wrong
          </h2>
          <p className="mt-2 text-center text-sm text-gray-600">
            {error.message}
          </p>
        </div>

        <div className="space-y-4">
          <button
            onClick={resetError}
            className="group relative w-full flex justify-center py-2 px-4 border border-transparent text-sm font-medium rounded-md text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
          >
            Try again
          </button>

          <button
            onClick={() => setShowDetails(!showDetails)}
            className="group relative w-full flex justify-center py-2 px-4 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500"
          >
            {showDetails ? 'Hide' : 'Show'} details
          </button>

          {showDetails && (
            <div className="mt-4 p-4 bg-gray-100 rounded-md">
              <h3 className="text-sm font-medium text-gray-900 mb-2">Error Details:</h3>
              <div className="text-xs text-gray-600 space-y-1">
                <p><strong>Type:</strong> {error.type}</p>
                <p><strong>Time:</strong> {error.timestamp.toLocaleString()}</p>
                {error.details && (
                  <p><strong>Details:</strong> {error.details}</p>
                )}
                {error.context && (
                  <p><strong>Context:</strong> {JSON.stringify(error.context)}</p>
                )}
              </div>
            </div>
          )}

          <div className="text-center">
            <a
              href="mailto:support@fresco.example.com"
              className="text-sm text-blue-600 hover:text-blue-500"
            >
              Contact support if the problem persists
            </a>
          </div>
        </div>
      </div>
    </div>
  );
};

/**
 * Higher-order component that wraps a component with error boundary
 */
export function withErrorBoundary<P extends object>(
  Component: React.ComponentType<P>,
  fallback?: React.ComponentType<{ error: AppError; resetError: () => void }>
): React.ComponentType<P> {
  const WrappedComponent = (props: P) => (
    <ErrorBoundary fallback={fallback}>
      <Component {...props} />
    </ErrorBoundary>
  );

  WrappedComponent.displayName = `withErrorBoundary(${Component.displayName || Component.name})`;
  return WrappedComponent;
}

/**
 * Simple error fallback for inline errors
 */
export const SimpleErrorFallback: React.FC<{ error: AppError; resetError: () => void }> = ({ 
  error, 
  resetError 
}) => (
  <div className="p-4 border border-red-200 rounded-md bg-red-50">
    <div className="flex">
      <div className="flex-shrink-0">
        <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
          <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
        </svg>
      </div>
      <div className="ml-3">
        <p className="text-sm text-red-800">{error.message}</p>
        <button
          onClick={resetError}
          className="mt-2 text-sm text-red-600 hover:text-red-500 underline"
        >
          Try again
        </button>
      </div>
    </div>
  </div>
);

export default ErrorBoundary;