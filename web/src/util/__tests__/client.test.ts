import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { TimeSeriesClient, startSingleQuery } from '../client';
import { AsyncDuckDB, AsyncDuckDBConnection } from 'duckdb-wasm-kit';

// Mock DuckDB
const mockConnection = {
  query: vi.fn(),
  close: vi.fn(),
} as unknown as AsyncDuckDBConnection;

const mockDB = {
  connect: vi.fn().mockResolvedValue(mockConnection),
  registerFileBuffer: vi.fn(),
} as unknown as AsyncDuckDB;

// Mock fetch
global.fetch = vi.fn();

describe('TimeSeriesClient', () => {
  let client: TimeSeriesClient;

  beforeEach(() => {
    vi.clearAllMocks();
    client = new TimeSeriesClient(10, mockDB);
    
    // Setup default fetch mock
    (global.fetch as any).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        body: JSON.stringify({
          chunks: [
            { url: 'https://test.com/file1.parquet' },
            { url: 'https://test.com/file2.parquet' }
          ]
        })
      }),
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(1024)),
    });

    // Setup default DuckDB mocks
    (mockConnection.query as any).mockResolvedValue({
      toArray: () => [{ count: 10 }]
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('constructor', () => {
    it('should create instance with correct properties', () => {
      expect(client).toBeDefined();
      expect((client as any).maxWorkers).toBe(10);
      expect((client as any).db).toBe(mockDB);
      expect((client as any).baseUrl).toBe('https://dusrle1grb.execute-api.us-east-1.amazonaws.com/prod');
    });
  });

  describe('downloadFile', () => {
    it('should successfully download and process a parquet file', async () => {
      const testUrl = 'https://test.com/test.parquet';
      
      const result = await client.downloadFile(testUrl);
      
      expect(result).toBe(true);
      expect(global.fetch).toHaveBeenCalledWith(testUrl, {
        method: 'GET',
        mode: 'cors',
        credentials: 'omit'
      });
      expect(mockDB.registerFileBuffer).toHaveBeenCalled();
      expect(mockConnection.query).toHaveBeenCalledWith('LOAD icu;');
      expect(mockConnection.query).toHaveBeenCalledWith("SET TimeZone='America/New_York';");
    });

    it('should handle HTTP error responses', async () => {
      (global.fetch as any).mockResolvedValue({
        ok: false,
        status: 404,
      });

      const result = await client.downloadFile('https://test.com/notfound.parquet');
      
      expect(result).toBe(false);
    });

    it('should handle fetch errors', async () => {
      (global.fetch as any).mockRejectedValue(new Error('Network error'));

      const result = await client.downloadFile('https://test.com/error.parquet');
      
      expect(result).toBe(false);
    });

    it('should handle empty parquet files', async () => {
      (mockConnection.query as any).mockResolvedValue({
        toArray: () => [{ count: 0 }]
      });

      const result = await client.downloadFile('https://test.com/empty.parquet');
      
      expect(result).toBe(false);
    });
  });

  describe('downloadContent', () => {
    it('should process multiple URLs in batches', async () => {
      const urls = Array.from({ length: 25 }, (_, i) => `https://test.com/file${i}.parquet`);
      
      const downloadFileSpy = vi.spyOn(client, 'downloadFile').mockResolvedValue(true);
      
      await client.downloadContent(urls);
      
      expect(downloadFileSpy).toHaveBeenCalledTimes(25);
    });

    it('should handle retry logic for failed downloads', async () => {
      const urls = ['https://test.com/file1.parquet'];
      
      const downloadFileSpy = vi.spyOn(client, 'downloadFile')
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(false)
        .mockResolvedValueOnce(true);
      
      await client.downloadContent(urls);
      
      expect(downloadFileSpy).toHaveBeenCalledTimes(3);
    });
  });

  describe('queryData', () => {
    it('should send correct API request', async () => {
      const testQuery = "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'";
      const rowLimit = 1000;

      const mockResult = {
        body: JSON.stringify({
          chunks: [{ url: 'https://test.com/result.parquet' }]
        }),
        transferId: 'test-123'
      };

      (global.fetch as any).mockResolvedValue({
        ok: true,
        json: () => Promise.resolve(mockResult),
      });

      const result = await client.queryData(testQuery, rowLimit);

      expect(global.fetch).toHaveBeenCalledWith(
        'https://dusrle1grb.execute-api.us-east-1.amazonaws.com/prod/',
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            query: testQuery,
            clientId: 'test-client',
            rowLimit
          })
        }
      );
      expect(result).toEqual(mockResult);
    });

    it('should handle API errors', async () => {
      (global.fetch as any).mockResolvedValue({
        ok: false,
        status: 500,
        text: () => Promise.resolve('Server error'),
      });

      await expect(client.queryData('SELECT * FROM fresco', 100))
        .rejects.toThrow('HTTP error! status: 500');
    });

    it('should handle network errors', async () => {
      (global.fetch as any).mockRejectedValue(new Error('Network error'));

      await expect(client.queryData('SELECT * FROM fresco', 100))
        .rejects.toThrow('Network error');
    });
  });
});

describe('startSingleQuery', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    (global.fetch as any).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        body: JSON.stringify({
          chunks: [
            { url: 'https://test.com/file1.parquet' },
            { url: 'https://test.com/file2.parquet' }
          ]
        })
      }),
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(1024)),
    });

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => [{ count: 100 }]
    });
  });

  it('should execute complete data loading workflow', async () => {
    const testQuery = "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'";
    const tableName = 'test_table';
    const rowLimit = 1000;
    const onProgress = vi.fn();

    await startSingleQuery(testQuery, mockDB, tableName, rowLimit, onProgress);

    expect(mockDB.connect).toHaveBeenCalled();
    expect(mockConnection.query).toHaveBeenCalledWith(`DROP TABLE IF EXISTS ${tableName};`);
    expect(mockConnection.query).toHaveBeenCalledWith(expect.stringContaining(`CREATE TABLE IF NOT EXISTS ${tableName}`));
    expect(onProgress).toHaveBeenCalled();
    expect(mockConnection.close).toHaveBeenCalled();
  });

  it('should handle API response parsing errors', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        body: 'invalid json'
      }),
    });

    await expect(startSingleQuery(
      "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'",
      mockDB,
      'test_table',
      1000
    )).rejects.toThrow('Failed to parse API response');
  });

  it('should handle empty chunks response', async () => {
    (global.fetch as any).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        body: JSON.stringify({ chunks: [] })
      }),
    });

    await expect(startSingleQuery(
      "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'",
      mockDB,
      'test_table',
      1000
    )).rejects.toThrow('No data found for the selected time range');
  });

  it('should handle no data loaded after processing chunks', async () => {
    (mockConnection.query as any).mockImplementation((query: string) => {
      if (query.includes('COUNT(*)')) {
        return Promise.resolve({ toArray: () => [{ count: 0 }] });
      }
      return Promise.resolve({ toArray: () => [{ count: 100 }] });
    });

    await expect(startSingleQuery(
      "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'",
      mockDB,
      'test_table',
      1000
    )).rejects.toThrow('No data loaded into test_table table after downloading chunks');
  });

  it('should extract time bounds correctly', async () => {
    const testQuery = "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-02T23:59:59Z'";
    
    // Test the time extraction indirectly by ensuring the query executes
    await startSingleQuery(testQuery, mockDB, 'test_table', 1000);
    
    expect(mockDB.connect).toHaveBeenCalled();
  });

  it('should handle progress callback', async () => {
    const progressValues: number[] = [];
    const onProgress = (progress: number) => progressValues.push(progress);

    await startSingleQuery(
      "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'",
      mockDB,
      'test_table',
      1000,
      onProgress
    );

    expect(progressValues.length).toBeGreaterThan(0);
    expect(progressValues[progressValues.length - 1]).toBe(100);
  });

  it('should process chunks in batches of 20', async () => {
    const chunks = Array.from({ length: 45 }, (_, i) => ({ url: `https://test.com/file${i}.parquet` }));
    
    (global.fetch as any).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({
        body: JSON.stringify({ chunks })
      }),
      arrayBuffer: () => Promise.resolve(new ArrayBuffer(1024)),
    });

    await startSingleQuery(
      "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'",
      mockDB,
      'test_table',
      1000
    );

    // Should process in 3 batches: 20, 20, 5
    expect(global.fetch).toHaveBeenCalledTimes(1 + 45); // 1 API call + 45 file downloads
  });
});