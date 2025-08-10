import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor, renderHook } from '@testing-library/react';
import { DuckDBProvider, useDuckDB } from '../DuckDBContext';

// Mock duckdb-wasm-kit
const mockDB = {
  connect: vi.fn(),
  terminate: vi.fn(),
};

const mockConnection = {
  query: vi.fn(),
  close: vi.fn(),
};

const mockUseDuckDb = {
  db: mockDB,
  loading: false,
};

vi.mock('duckdb-wasm-kit', () => ({
  useDuckDb: () => mockUseDuckDb,
}));

// Mock console methods
const consoleMocks = {
  log: vi.fn(),
  error: vi.fn(),
  warn: vi.fn(),
};

describe('DuckDBContext', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock console methods
    global.console = {
      ...global.console,
      ...consoleMocks,
    };

    // Mock window event listeners
    global.window = {
      ...global.window,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    } as any;

    // Reset connection mock
    mockConnection.query.mockResolvedValue(undefined);
    mockDB.connect.mockResolvedValue(mockConnection);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('DuckDBProvider', () => {
    it('should render children', () => {
      render(
        <DuckDBProvider>
          <div data-testid="child">Test Child</div>
        </DuckDBProvider>
      );

      expect(screen.getByTestId('child')).toBeInTheDocument();
    });

    it('should initialize DuckDB when duckDBKit is ready', async () => {
      const TestComponent = () => {
        const { db, loading } = useDuckDB();
        return (
          <div>
            <div data-testid="loading">{loading.toString()}</div>
            <div data-testid="db">{db ? 'db-ready' : 'no-db'}</div>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      await waitFor(() => {
        expect(screen.getByTestId('loading')).toHaveTextContent('false');
      });

      expect(screen.getByTestId('db')).toHaveTextContent('db-ready');
      expect(mockDB.connect).toHaveBeenCalled();
      expect(mockConnection.query).toHaveBeenCalledWith('LOAD icu');
      expect(mockConnection.query).toHaveBeenCalledWith("SET TimeZone='America/New_York'");
    });

    it('should handle DuckDB initialization errors', async () => {
      mockDB.connect.mockRejectedValue(new Error('Connection failed'));

      const TestComponent = () => {
        const { error, loading } = useDuckDB();
        return (
          <div>
            <div data-testid="loading">{loading.toString()}</div>
            <div data-testid="error">{error?.message || 'no-error'}</div>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      await waitFor(() => {
        expect(screen.getByTestId('loading')).toHaveTextContent('false');
      });

      await waitFor(() => {
        expect(screen.getByTestId('error')).toHaveTextContent('Connection failed');
      });
    });

    it('should wait for duckDBKit to load', async () => {
      mockUseDuckDb.loading = true;
      mockUseDuckDb.db = null;

      const TestComponent = () => {
        const { loading } = useDuckDB();
        return <div data-testid="loading">{loading.toString()}</div>;
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      expect(screen.getByTestId('loading')).toHaveTextContent('true');
    });

    it('should set up event listeners for cleanup', () => {
      render(
        <DuckDBProvider>
          <div>Test</div>
        </DuckDBProvider>
      );

      expect(window.addEventListener).toHaveBeenCalledWith('beforeunload', expect.any(Function));
    });
  });

  describe('useDuckDB hook', () => {
    it('should provide context values', async () => {
      const TestComponent = () => {
        const context = useDuckDB();
        return (
          <div>
            <div data-testid="db">{context.db ? 'has-db' : 'no-db'}</div>
            <div data-testid="loading">{context.loading.toString()}</div>
            <div data-testid="dataloading">{context.dataloading.toString()}</div>
            <div data-testid="histogram">{context.histogramData.toString()}</div>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      await waitFor(() => {
        expect(screen.getByTestId('loading')).toHaveTextContent('false');
      });

      expect(screen.getByTestId('db')).toHaveTextContent('has-db');
      expect(screen.getByTestId('dataloading')).toHaveTextContent('true');
      expect(screen.getByTestId('histogram')).toHaveTextContent('false');
    });

    it('should allow updating state values', async () => {
      const TestComponent = () => {
        const { dataloading, setDataLoading, histogramData, setHistogramData } = useDuckDB();
        
        return (
          <div>
            <div data-testid="dataloading">{dataloading.toString()}</div>
            <div data-testid="histogram">{histogramData.toString()}</div>
            <button onClick={() => setDataLoading(false)}>Set Data Loading False</button>
            <button onClick={() => setHistogramData(true)}>Set Histogram True</button>
          </div>
        );
      };

      const { user } = renderHook(() => ({}), {
        wrapper: ({ children }) => (
          <DuckDBProvider>
            <TestComponent />
            {children}
          </DuckDBProvider>
        ),
      });

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      // Initially dataloading should be true, histogram false
      expect(screen.getByTestId('dataloading')).toHaveTextContent('true');
      expect(screen.getByTestId('histogram')).toHaveTextContent('false');

      // Click buttons to change state
      screen.getByText('Set Data Loading False').click();
      screen.getByText('Set Histogram True').click();

      await waitFor(() => {
        expect(screen.getByTestId('dataloading')).toHaveTextContent('false');
        expect(screen.getByTestId('histogram')).toHaveTextContent('true');
      });
    });

    it('should handle cross filter state', async () => {
      const TestComponent = () => {
        const { crossFilter, setCrossFilter } = useDuckDB();
        
        return (
          <div>
            <div data-testid="crossfilter">{crossFilter ? 'has-filter' : 'no-filter'}</div>
            <button onClick={() => setCrossFilter({ test: 'filter' })}>Set Filter</button>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      expect(screen.getByTestId('crossfilter')).toHaveTextContent('no-filter');

      screen.getByText('Set Filter').click();

      await waitFor(() => {
        expect(screen.getByTestId('crossfilter')).toHaveTextContent('has-filter');
      });
    });
  });

  describe('createConnection function', () => {
    it('should create a new connection with proper settings', async () => {
      const TestComponent = () => {
        const { createConnection } = useDuckDB();
        const [connectionResult, setConnectionResult] = React.useState<string>('none');
        
        const handleCreateConnection = async () => {
          const conn = await createConnection();
          setConnectionResult(conn ? 'success' : 'failed');
        };

        return (
          <div>
            <div data-testid="connection-result">{connectionResult}</div>
            <button onClick={handleCreateConnection}>Create Connection</button>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      await waitFor(() => {
        expect(screen.getByTestId('connection-result')).toHaveTextContent('none');
      });

      screen.getByText('Create Connection').click();

      await waitFor(() => {
        expect(screen.getByTestId('connection-result')).toHaveTextContent('success');
      });

      // Verify connection setup calls
      expect(mockConnection.query).toHaveBeenCalledWith('LOAD icu');
      expect(mockConnection.query).toHaveBeenCalledWith("SET TimeZone='America/New_York'");
      expect(mockConnection.query).toHaveBeenCalledWith("SET temp_directory='browser-data/tmp'");
      expect(mockConnection.query).toHaveBeenCalledWith('PRAGMA threads=4');
      expect(mockConnection.query).toHaveBeenCalledWith("PRAGMA memory_limit='2GB'");
    });

    it('should return null when no DB instance exists', async () => {
      // Simulate no DB instance
      mockUseDuckDb.db = null;

      const TestComponent = () => {
        const { createConnection } = useDuckDB();
        const [connectionResult, setConnectionResult] = React.useState<string>('none');
        
        const handleCreateConnection = async () => {
          const conn = await createConnection();
          setConnectionResult(conn ? 'success' : 'failed');
        };

        return (
          <div>
            <div data-testid="connection-result">{connectionResult}</div>
            <button onClick={handleCreateConnection}>Create Connection</button>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      screen.getByText('Create Connection').click();

      await waitFor(() => {
        expect(screen.getByTestId('connection-result')).toHaveTextContent('failed');
      });
    });

    it('should handle connection creation errors', async () => {
      mockDB.connect.mockRejectedValue(new Error('Connection creation failed'));

      const TestComponent = () => {
        const { createConnection } = useDuckDB();
        const [connectionResult, setConnectionResult] = React.useState<string>('none');
        
        const handleCreateConnection = async () => {
          const conn = await createConnection();
          setConnectionResult(conn ? 'success' : 'failed');
        };

        return (
          <div>
            <div data-testid="connection-result">{connectionResult}</div>
            <button onClick={handleCreateConnection}>Create Connection</button>
          </div>
        );
      };

      render(
        <DuckDBProvider>
          <TestComponent />
        </DuckDBProvider>
      );

      screen.getByText('Create Connection').click();

      await waitFor(() => {
        expect(screen.getByTestId('connection-result')).toHaveTextContent('failed');
      });

      expect(consoleMocks.error).toHaveBeenCalledWith('Error creating connection:', expect.any(Error));
    });
  });

  describe('cleanup behavior', () => {
    it('should clean up on unmount', () => {
      const { unmount } = render(
        <DuckDBProvider>
          <div>Test</div>
        </DuckDBProvider>
      );

      unmount();

      // Should have attempted cleanup
      expect(window.removeEventListener).toHaveBeenCalledWith('beforeunload', expect.any(Function));
    });

    it('should handle beforeunload event', async () => {
      render(
        <DuckDBProvider>
          <div>Test</div>
        </DuckDBProvider>
      );

      // Get the beforeunload handler
      const beforeUnloadHandler = (window.addEventListener as any).mock.calls
        .find((call: any) => call[0] === 'beforeunload')?.[1];

      expect(beforeUnloadHandler).toBeDefined();

      // Should not throw when called
      expect(() => beforeUnloadHandler()).not.toThrow();
    });

    it('should handle connection cleanup errors gracefully', async () => {
      mockConnection.close.mockRejectedValue(new Error('Close failed'));
      mockConnection.query.mockRejectedValue(new Error('Query failed'));

      const { unmount } = render(
        <DuckDBProvider>
          <div>Test</div>
        </DuckDBProvider>
      );

      // Wait for initialization
      await waitFor(() => {
        expect(mockDB.connect).toHaveBeenCalled();
      });

      unmount();

      // Should not throw despite errors
      expect(consoleMocks.warn).toHaveBeenCalled();
    });
  });

  describe('multiple provider instances', () => {
    it('should reuse existing DuckDB instance', async () => {
      // First provider
      const { unmount: unmount1 } = render(
        <DuckDBProvider>
          <div data-testid="provider1">Provider 1</div>
        </DuckDBProvider>
      );

      await waitFor(() => {
        expect(mockDB.connect).toHaveBeenCalled();
      });

      const firstCallCount = mockDB.connect.mock.calls.length;

      // Second provider (should reuse instance)
      render(
        <DuckDBProvider>
          <div data-testid="provider2">Provider 2</div>
        </DuckDBProvider>
      );

      await waitFor(() => {
        expect(screen.getByTestId('provider2')).toBeInTheDocument();
      });

      // Should not have created additional connections for the same instance
      expect(mockDB.connect.mock.calls.length).toBe(firstCallCount);

      unmount1();
    });
  });
});