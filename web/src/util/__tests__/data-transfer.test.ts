import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { saveQueryParameters, getQueryParameters, QueryParameters } from '../data-transfer';

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  removeItem: vi.fn(),
  clear: vi.fn(),
};

describe('data-transfer utilities', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock localStorage
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    });

    // Mock console.error
    global.console = {
      ...global.console,
      error: vi.fn(),
    };
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('saveQueryParameters', () => {
    it('should save query parameters to localStorage', () => {
      const testParams: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco WHERE time BETWEEN ? AND ?',
        rowCount: 1000,
      };

      saveQueryParameters(testParams);

      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        'fresco_query_params',
        JSON.stringify(testParams)
      );
    });

    it('should save parameters without rowCount', () => {
      const testParams: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco',
      };

      saveQueryParameters(testParams);

      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        'fresco_query_params',
        JSON.stringify(testParams)
      );
    });

    it('should handle complex SQL queries', () => {
      const testParams: QueryParameters = {
        startDate: '2024-01-01T00:00:00Z',
        endDate: '2024-01-31T23:59:59Z',
        sqlQuery: `
          SELECT 
            time, 
            value_cpuuser, 
            value_memused 
          FROM fresco 
          WHERE time BETWEEN '2024-01-01' AND '2024-01-31'
            AND value_cpuuser > 80
          ORDER BY time DESC
          LIMIT 5000
        `,
        rowCount: 5000,
      };

      saveQueryParameters(testParams);

      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        'fresco_query_params',
        JSON.stringify(testParams)
      );
    });

    it('should handle special characters in parameters', () => {
      const testParams: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: "SELECT * FROM fresco WHERE jobname LIKE '%test%' AND account = 'user@domain.com'",
      };

      saveQueryParameters(testParams);

      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        'fresco_query_params',
        JSON.stringify(testParams)
      );
    });
  });

  describe('getQueryParameters', () => {
    it('should retrieve query parameters from localStorage', () => {
      const testParams: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco',
        rowCount: 1000,
      };

      localStorageMock.getItem.mockReturnValue(JSON.stringify(testParams));

      const result = getQueryParameters();

      expect(localStorageMock.getItem).toHaveBeenCalledWith('fresco_query_params');
      expect(result).toEqual(testParams);
    });

    it('should return null when no parameters are stored', () => {
      localStorageMock.getItem.mockReturnValue(null);

      const result = getQueryParameters();

      expect(result).toBeNull();
    });

    it('should return null when localStorage item is empty string', () => {
      localStorageMock.getItem.mockReturnValue('');

      const result = getQueryParameters();

      expect(result).toBeNull();
    });

    it('should handle invalid JSON gracefully', () => {
      localStorageMock.getItem.mockReturnValue('invalid json {');

      const result = getQueryParameters();

      expect(result).toBeNull();
      expect(console.error).toHaveBeenCalledWith(
        'Error parsing stored query parameters:',
        expect.any(Error)
      );
    });

    it('should handle corrupted data gracefully', () => {
      localStorageMock.getItem.mockReturnValue('{"startDate": "2024-01-01", "endDate":}');

      const result = getQueryParameters();

      expect(result).toBeNull();
      expect(console.error).toHaveBeenCalledWith(
        'Error parsing stored query parameters:',
        expect.any(Error)
      );
    });

    it('should handle partially valid JSON', () => {
      const partialData = {
        startDate: '2024-01-01',
        // Missing endDate and sqlQuery
      };

      localStorageMock.getItem.mockReturnValue(JSON.stringify(partialData));

      const result = getQueryParameters();

      expect(result).toEqual(partialData);
    });

    it('should handle valid JSON with extra properties', () => {
      const extendedData = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco',
        rowCount: 1000,
        extraProperty: 'should be ignored',
        anotherExtra: 123,
      };

      localStorageMock.getItem.mockReturnValue(JSON.stringify(extendedData));

      const result = getQueryParameters();

      expect(result).toEqual(extendedData);
    });
  });

  describe('integration scenarios', () => {
    it('should maintain data integrity through save and retrieve cycle', () => {
      const originalParams: QueryParameters = {
        startDate: '2024-01-01T00:00:00Z',
        endDate: '2024-12-31T23:59:59Z',
        sqlQuery: `
          SELECT 
            time,
            submit_time,
            start_time,
            end_time,
            value_cpuuser,
            value_memused,
            value_gpu
          FROM fresco 
          WHERE time BETWEEN '2024-01-01' AND '2024-12-31'
            AND nhosts > 1
            AND value_cpuuser IS NOT NULL
          ORDER BY time DESC
          LIMIT 10000
        `,
        rowCount: 10000,
      };

      // Simulate the full cycle
      saveQueryParameters(originalParams);
      
      // Get the stored value and mock it for retrieval
      const storedValue = localStorageMock.setItem.mock.calls[0][1];
      localStorageMock.getItem.mockReturnValue(storedValue);
      
      const retrievedParams = getQueryParameters();

      expect(retrievedParams).toEqual(originalParams);
    });

    it('should handle multiple save operations', () => {
      const params1: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco LIMIT 100',
      };

      const params2: QueryParameters = {
        startDate: '2024-02-01',
        endDate: '2024-02-28',
        sqlQuery: 'SELECT * FROM fresco WHERE value_cpuuser > 50',
        rowCount: 5000,
      };

      saveQueryParameters(params1);
      saveQueryParameters(params2);

      expect(localStorageMock.setItem).toHaveBeenCalledTimes(2);
      expect(localStorageMock.setItem).toHaveBeenLastCalledWith(
        'fresco_query_params',
        JSON.stringify(params2)
      );
    });

    it('should handle localStorage quota exceeded error', () => {
      const largeParams: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * '.repeat(10000) + 'FROM fresco', // Very large query
      };

      localStorageMock.setItem.mockImplementation(() => {
        throw new DOMException('QuotaExceededError');
      });

      // Should throw a quota exceeded error
      expect(() => saveQueryParameters(largeParams)).toThrow('QuotaExceededError');
    });

    it('should handle localStorage access errors', () => {
      localStorageMock.getItem.mockImplementation(() => {
        throw new Error('localStorage access denied');
      });

      // Should throw since localStorage access error is not caught
      expect(() => getQueryParameters()).toThrow('localStorage access denied');
    });
  });

  describe('QueryParameters interface compliance', () => {
    it('should handle all required properties', () => {
      const completeParams: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco',
      };

      saveQueryParameters(completeParams);
      
      const storedValue = localStorageMock.setItem.mock.calls[0][1];
      localStorageMock.getItem.mockReturnValue(storedValue);
      
      const result = getQueryParameters();

      expect(result).toHaveProperty('startDate');
      expect(result).toHaveProperty('endDate');
      expect(result).toHaveProperty('sqlQuery');
    });

    it('should handle optional rowCount property', () => {
      const paramsWithoutRowCount: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco',
      };

      const paramsWithRowCount: QueryParameters = {
        startDate: '2024-01-01',
        endDate: '2024-01-31',
        sqlQuery: 'SELECT * FROM fresco',
        rowCount: 2500,
      };

      // Test without rowCount
      saveQueryParameters(paramsWithoutRowCount);
      let storedValue = localStorageMock.setItem.mock.calls[0][1];
      localStorageMock.getItem.mockReturnValue(storedValue);
      let result = getQueryParameters();
      expect(result?.rowCount).toBeUndefined();

      // Test with rowCount
      saveQueryParameters(paramsWithRowCount);
      storedValue = localStorageMock.setItem.mock.calls[1][1];
      localStorageMock.getItem.mockReturnValue(storedValue);
      result = getQueryParameters();
      expect(result?.rowCount).toBe(2500);
    });
  });
});