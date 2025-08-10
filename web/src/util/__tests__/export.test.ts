import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { exportDataAsCSV } from '../export';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

// Mock connection
const mockConnection = {
  query: vi.fn(),
} as unknown as AsyncDuckDBConnection;

// Mock DOM elements
const mockCreateElement = vi.fn();
const mockAppendChild = vi.fn();
const mockRemoveChild = vi.fn();
const mockClick = vi.fn();

// Mock URL.createObjectURL
const mockCreateObjectURL = vi.fn();
const mockRevokeObjectURL = vi.fn();

// Mock window.alert
const mockAlert = vi.fn();

describe('exportDataAsCSV', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock document.createElement
    global.document = {
      ...global.document,
      createElement: mockCreateElement,
      body: {
        appendChild: mockAppendChild,
        removeChild: mockRemoveChild,
      }
    } as any;

    // Mock URL
    global.URL = {
      createObjectURL: mockCreateObjectURL,
      revokeObjectURL: mockRevokeObjectURL,
    } as any;

    // Mock window.alert
    global.alert = mockAlert;

    // Mock Blob
    global.Blob = vi.fn().mockImplementation((content, options) => ({
      content,
      options,
    })) as any;

    // Setup mock link element
    const mockLink = {
      setAttribute: vi.fn(),
      click: mockClick,
      style: {},
    };
    mockCreateElement.mockReturnValue(mockLink);
    mockCreateObjectURL.mockReturnValue('blob:mock-url');
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should export data to CSV successfully', async () => {
    const mockData = [
      { id: 1, name: 'John', email: 'john@test.com', created_at: new Date('2024-01-01') },
      { id: 2, name: 'Jane', email: 'jane@test.com', created_at: new Date('2024-01-02') }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'users', 'test_export');

    expect(mockConnection.query).toHaveBeenCalledWith('SELECT * FROM users');
    expect(global.Blob).toHaveBeenCalledWith(
      [expect.stringContaining('id,name,email,created_at')],
      { type: 'text/csv;charset=utf-8;' }
    );
    expect(mockCreateObjectURL).toHaveBeenCalled();
    expect(mockCreateElement).toHaveBeenCalledWith('a');
    expect(mockClick).toHaveBeenCalled();
  });

  it('should handle filters in query', async () => {
    const mockData = [
      { id: 1, name: 'John', status: 'active' }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'users', 'filtered_export', "status = 'active'");

    expect(mockConnection.query).toHaveBeenCalledWith("SELECT * FROM users WHERE status = 'active'");
  });

  it('should handle empty filters gracefully', async () => {
    const mockData = [
      { id: 1, name: 'John' }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'users', 'export', '');

    expect(mockConnection.query).toHaveBeenCalledWith('SELECT * FROM users');
  });

  it('should handle string values with commas by quoting them', async () => {
    const mockData = [
      { id: 1, description: 'Item with, comma' },
      { id: 2, description: 'Normal item' }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'items', 'comma_test');

    const blobCall = (global.Blob as any).mock.calls[0];
    const csvContent = blobCall[0][0];
    
    expect(csvContent).toContain('"Item with, comma"');
    expect(csvContent).toContain('Normal item');
  });

  it('should format Date objects to ISO string', async () => {
    const testDate = new Date('2024-01-15T10:30:00Z');
    const mockData = [
      { id: 1, created_at: testDate }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'events', 'date_test');

    const blobCall = (global.Blob as any).mock.calls[0];
    const csvContent = blobCall[0][0];
    
    expect(csvContent).toContain(testDate.toISOString());
  });

  it('should handle null values', async () => {
    const mockData = [
      { id: 1, name: 'John', notes: null },
      { id: 2, name: null, notes: 'Some notes' }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'users', 'null_test');

    const blobCall = (global.Blob as any).mock.calls[0];
    const csvContent = blobCall[0][0];
    
    // Check that null values become empty strings
    expect(csvContent).toContain('John,');
    expect(csvContent).toContain(',Some notes');
  });

  it('should handle empty result set', async () => {
    (mockConnection.query as any).mockResolvedValue({
      toArray: () => []
    });

    await exportDataAsCSV(mockConnection, 'empty_table', 'empty_export');

    expect(mockAlert).toHaveBeenCalledWith(
      expect.stringContaining('Failed to export data: No data to export')
    );
    expect(global.Blob).not.toHaveBeenCalled();
  });

  it('should handle database query errors', async () => {
    (mockConnection.query as any).mockRejectedValue(new Error('Database connection failed'));

    await exportDataAsCSV(mockConnection, 'users', 'error_test');

    expect(mockAlert).toHaveBeenCalledWith(
      expect.stringContaining('Failed to export data: Database connection failed')
    );
    expect(global.Blob).not.toHaveBeenCalled();
  });

  it('should set correct link attributes', async () => {
    const mockData = [{ id: 1, name: 'Test' }];
    const mockLink = {
      setAttribute: vi.fn(),
      click: mockClick,
      style: {},
    };

    mockCreateElement.mockReturnValue(mockLink);
    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'test_table', 'my_export');

    expect(mockLink.setAttribute).toHaveBeenCalledWith('href', 'blob:mock-url');
    expect(mockLink.setAttribute).toHaveBeenCalledWith('download', 'my_export.csv');
    expect(mockLink.style.visibility).toBe('hidden');
  });

  it('should properly clean up DOM elements', async () => {
    const mockData = [{ id: 1, name: 'Test' }];
    const mockLink = {
      setAttribute: vi.fn(),
      click: mockClick,
      style: {},
    };

    mockCreateElement.mockReturnValue(mockLink);
    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'test_table', 'cleanup_test');

    expect(mockAppendChild).toHaveBeenCalledWith(mockLink);
    expect(mockClick).toHaveBeenCalled();
    expect(mockRemoveChild).toHaveBeenCalledWith(mockLink);
  });

  it('should handle complex data types', async () => {
    const mockData = [
      { 
        id: 1, 
        name: 'John',
        score: 95.5,
        active: true,
        data: { complex: 'object' }, // Objects should be stringified
        array: [1, 2, 3] // Arrays should be stringified
      }
    ];

    (mockConnection.query as any).mockResolvedValue({
      toArray: () => mockData
    });

    await exportDataAsCSV(mockConnection, 'complex_data', 'complex_test');

    const blobCall = (global.Blob as any).mock.calls[0];
    const csvContent = blobCall[0][0];
    
    expect(csvContent).toContain('John');
    expect(csvContent).toContain('95.5');
    expect(csvContent).toContain('true');
  });
});