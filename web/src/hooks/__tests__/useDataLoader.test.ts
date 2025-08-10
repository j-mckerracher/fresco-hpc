import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useDataLoader } from '../useDataLoader';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

// Mock external dependencies
vi.mock('@uwdata/vgplot', () => ({
  Selection: {
    crossfilter: vi.fn(() => ({})),
  },
  coordinator: vi.fn(() => ({
    databaseConnector: vi.fn(),
  })),
  wasmConnector: vi.fn(() => ({})),
}));

vi.mock('@/utils/errorHandler', () => ({
  ErrorHandler: {
    handle: vi.fn((error) => ({ message: error.message || 'Unknown error' })),
  },
  ErrorType: {
    Database: 'database',
    Unknown: 'unknown',
  },
}));

vi.mock('@/utils/schema', () => ({
  createHpcJobTableSQL: vi.fn(() => 'CREATE TABLE job_data (...)'),
  generateDemoDataSQL: vi.fn(() => 'INSERT INTO job_data VALUES (...)'),
}));

vi.mock('@/util/export', () => ({
  exportDataAsCSV: vi.fn(),
}));

// Mock console methods
const consoleMocks = {
  log: vi.fn(),
  error: vi.fn(),
  warn: vi.fn(),
};

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
};

describe('useDataLoader', () => {
  let mockDB: AsyncDuckDB;
  let mockConnection: AsyncDuckDBConnection;
  let defaultProps: any;

  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock console
    global.console = {
      ...global.console,
      ...consoleMocks,
    };

    // Mock localStorage
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
    });

    // Mock connection
    mockConnection = {
      query: vi.fn(),
      close: vi.fn(),
      schema: {
        fields: [
          { name: 'time' },
          { name: 'value_cpuuser' },
          { name: 'value_memused' },
          { name: 'nhosts' },
          { name: 'account' },
        ],
      },
    } as any;

    // Mock database
    mockDB = {
      connect: vi.fn().mockResolvedValue(mockConnection),
    } as any;

    // Default props
    defaultProps = {
      db: mockDB,
      loading: false,
      error: null,
      dataloading: true,
      setDataLoading: vi.fn(),
      setCrossFilter: vi.fn(),
    };

    // Setup default query responses
    (mockConnection.query as any)
      .mockResolvedValueOnce(undefined) // LOAD icu
      .mockResolvedValueOnce(undefined) // SET TimeZone
      .mockResolvedValue({
        toArray: () => [{ count: 100 }],
        schema: {
          fields: [
            { name: 'time' },
            { name: 'value_cpuuser' },
            { name: 'value_memused' },
            { name: 'nhosts' },
            { name: 'account' },
          ],
        },
      });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('initialization', () => {
    it('should initialize with default values', () => {
      const { result } = renderHook(() => useDataLoader(defaultProps));

      expect(result.current.loadError).toBeNull();
      expect(result.current.availableColumns).toEqual([]);
      expect(result.current.dataTableName).toBe('job_data');
      expect(result.current.dataReady).toBe(false);
      expect(result.current.conn.current).toBeUndefined();
    });

    it('should provide all required functions', () => {
      const { result } = renderHook(() => useDataLoader(defaultProps));

      expect(typeof result.current.loadData).toBe('function');
      expect(typeof result.current.handleDownload).toBe('function');
      expect(typeof result.current.handleRetry).toBe('function');
    });
  });

  describe('loadData function', () => {
    it('should load data successfully with real data', async () => {
      // Mock existing table check
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [{ name: 'job_data' }] }) // Table exists check
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Count check
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Final count
        .mockResolvedValueOnce({
          toArray: () => [{ count: 100 }],
          schema: {
            fields: [
              { name: 'time' },
              { name: 'value_cpuuser' },
              { name: 'value_memused' },
            ],
          },
        }); // Schema check

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      expect(mockDB.connect).toHaveBeenCalled();
      expect(defaultProps.setDataLoading).toHaveBeenCalledWith(true);
      expect(defaultProps.setDataLoading).toHaveBeenCalledWith(false);
      expect(result.current.dataReady).toBe(true);
      expect(result.current.loadError).toBeNull();
    });

    it('should create demo data when useDemoData is true', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce(undefined) // DROP TABLE
        .mockResolvedValueOnce(undefined) // CREATE TABLE
        .mockResolvedValueOnce(undefined) // INSERT demo data
        .mockResolvedValueOnce({ toArray: () => [{ count: 500 }] }) // Demo data count
        .mockResolvedValueOnce({ toArray: () => [{ count: 500 }] }) // Final count
        .mockResolvedValueOnce({
          schema: {
            fields: [{ name: 'time' }, { name: 'value_cpuuser' }],
          },
        }); // Schema check

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData(true);
      });

      expect(result.current.dataReady).toBe(true);
      expect(result.current.loadError).toBeNull();
    });

    it('should handle database connection errors', async () => {
      (mockDB.connect as any).mockRejectedValue(new Error('Connection failed'));

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      expect(result.current.loadError).toBe('Connection failed');
      expect(result.current.dataReady).toBe(false);
      expect(defaultProps.setDataLoading).toHaveBeenCalledWith(false);
    });

    it('should handle no data available error', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [] }) // No existing table
        .mockRejectedValueOnce(new Error('No table job_data_small')); // No small table

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      expect(result.current.loadError).toContain('No real data available');
      expect(result.current.dataReady).toBe(false);
    });

    it('should not load when conditions are not met', async () => {
      const propsWithLoading = { ...defaultProps, loading: true };
      
      const { result } = renderHook(() => useDataLoader(propsWithLoading));

      await act(async () => {
        await result.current.loadData();
      });

      expect(mockDB.connect).not.toHaveBeenCalled();
    });

    it('should handle database errors from context', async () => {
      const propsWithError = { ...defaultProps, error: new Error('DuckDB initialization failed') };
      
      const { result } = renderHook(() => useDataLoader(propsWithError));

      await act(async () => {
        await result.current.loadData();
      });

      expect(result.current.loadError).toBe('DuckDB initialization failed');
      expect(result.current.dataReady).toBe(false);
    });
  });

  describe('column management', () => {
    it('should check available columns', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [{ name: 'job_data' }] }) // Table exists
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Count
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Final count
        .mockResolvedValueOnce({
          schema: {
            fields: [
              { name: 'time' },
              { name: 'value_cpuuser' },
              { name: 'nhosts' },
            ],
          },
        });

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      expect(result.current.availableColumns).toEqual(['time', 'value_cpuuser', 'nhosts']);
    });

    it('should create table with missing columns', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [{ name: 'job_data' }] }) // Table exists
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Count
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Final count
        .mockResolvedValueOnce({
          schema: {
            fields: [
              { name: 'time' },
              { name: 'value_cpuuser' }, // Missing other expected columns
            ],
          },
        }) // Schema check
        .mockResolvedValueOnce(undefined) // DROP TABLE IF EXISTS
        .mockResolvedValueOnce(undefined) // CREATE TABLE complete
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }); // Verify creation

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      expect(result.current.dataTableName).toBe('job_data_complete');
      expect(mockConnection.query).toHaveBeenCalledWith(
        expect.stringContaining('CREATE TABLE job_data_complete')
      );
    });
  });

  describe('handleDownload function', () => {
    beforeEach(() => {
      // Setup connection for download tests
      const { result } = renderHook(() => useDataLoader(defaultProps));
      result.current.conn.current = mockConnection;
    });

    it('should download data with date range from localStorage', async () => {
      localStorageMock.getItem.mockReturnValue(
        "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-31T23:59:59Z'"
      );

      const { exportDataAsCSV } = await import('@/util/export');
      
      const { result } = renderHook(() => useDataLoader(defaultProps));
      result.current.conn.current = mockConnection;

      await act(async () => {
        await result.current.handleDownload();
      });

      expect(exportDataAsCSV).toHaveBeenCalledWith(
        mockConnection,
        'job_data',
        'fresco-data-2024-01-01_to_2024-01-31',
        "time BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-31T23:59:59Z'"
      );
    });

    it('should download data with default filename when no stored query', async () => {
      localStorageMock.getItem.mockReturnValue(null);

      const { exportDataAsCSV } = await import('@/util/export');
      
      const { result } = renderHook(() => useDataLoader(defaultProps));
      result.current.conn.current = mockConnection;

      await act(async () => {
        await result.current.handleDownload();
      });

      expect(exportDataAsCSV).toHaveBeenCalledWith(
        mockConnection,
        'job_data',
        expect.stringMatching(/^fresco-data-\d{4}-\d{2}-\d{2}$/),
        ''
      );
    });

    it('should handle download errors', async () => {
      const { exportDataAsCSV } = await import('@/util/export');
      (exportDataAsCSV as any).mockRejectedValue(new Error('Export failed'));

      const { result } = renderHook(() => useDataLoader(defaultProps));
      result.current.conn.current = mockConnection;

      await expect(act(async () => {
        await result.current.handleDownload();
      })).rejects.toThrow('Failed to download data: Export failed');
    });

    it('should throw error when no connection available', async () => {
      const { result } = renderHook(() => useDataLoader(defaultProps));
      // Don't set connection

      await expect(act(async () => {
        await result.current.handleDownload();
      })).rejects.toThrow('Database connection not available');
    });
  });

  describe('handleRetry function', () => {
    it('should reset state and reload with demo data', async () => {
      const { result } = renderHook(() => useDataLoader(defaultProps));

      // Set some initial state
      await act(async () => {
        result.current.conn.current = mockConnection;
      });

      // Call retry
      await act(async () => {
        result.current.handleRetry();
      });

      expect(defaultProps.setDataLoading).toHaveBeenCalledWith(true);
      // Should call loadData with useDemoData = true
      expect(mockDB.connect).toHaveBeenCalled();
    });
  });

  describe('error handling', () => {
    it('should handle schema check errors gracefully', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [{ name: 'job_data' }] }) // Table exists
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Count
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Final count
        .mockRejectedValueOnce(new Error('Schema check failed')); // Schema error

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      // Should continue despite schema check error
      expect(result.current.dataReady).toBe(true);
      expect(consoleMocks.error).toHaveBeenCalledWith(
        'Error checking available columns:',
        expect.any(Object)
      );
    });

    it('should handle missing columns table creation errors', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [{ name: 'job_data' }] }) // Table exists
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Count
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Final count
        .mockResolvedValueOnce({
          schema: {
            fields: [{ name: 'time' }], // Missing columns
          },
        }) // Schema check
        .mockRejectedValueOnce(new Error('Table creation failed')); // Creation error

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      // Should fallback to original table name
      expect(result.current.dataTableName).toBe('job_data');
      expect(consoleMocks.error).toHaveBeenCalledWith(
        'Error creating complete table:',
        expect.any(Object)
      );
    });

    it('should handle connection close errors during cleanup', async () => {
      (mockConnection.close as any).mockRejectedValue(new Error('Close failed'));

      const { result } = renderHook(() => useDataLoader(defaultProps));
      result.current.conn.current = mockConnection;

      await act(async () => {
        await result.current.loadData();
      });

      expect(consoleMocks.warn).toHaveBeenCalledWith(
        'Warning when closing connection:',
        expect.any(Error)
      );
    });
  });

  describe('memoization', () => {
    it('should memoize expected columns', () => {
      const { result, rerender } = renderHook(() => useDataLoader(defaultProps));

      const firstRender = result.current;
      
      rerender();
      
      const secondRender = result.current;

      // The hook should maintain stable references for memoized values
      expect(firstRender.loadData).toBe(secondRender.loadData);
    });

    it('should update missing columns when available columns change', async () => {
      (mockConnection.query as any)
        .mockResolvedValueOnce(undefined) // LOAD icu
        .mockResolvedValueOnce(undefined) // SET TimeZone
        .mockResolvedValueOnce({ toArray: () => [{ name: 'job_data' }] }) // Table exists
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Count
        .mockResolvedValueOnce({ toArray: () => [{ count: 100 }] }) // Final count
        .mockResolvedValueOnce({
          schema: {
            fields: [
              { name: 'time' },
              { name: 'value_cpuuser' },
              { name: 'value_gpu' },
              { name: 'value_memused' },
              { name: 'value_memused_minus_diskcache' },
              { name: 'value_nfs' },
              { name: 'value_block' },
            ],
          },
        }); // All columns present

      const { result } = renderHook(() => useDataLoader(defaultProps));

      await act(async () => {
        await result.current.loadData();
      });

      // Should use original table since no columns are missing
      expect(result.current.dataTableName).toBe('job_data');
    });
  });
});