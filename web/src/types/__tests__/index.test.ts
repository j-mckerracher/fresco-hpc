import { describe, it, expect } from 'vitest';
import {
  PlotType,
  ConnectionStatus,
  ErrorType,
  ExportFormat,
  type HpcJobData,
  type RawHpcJobData,
  type ApiResponse,
  type QueryResult,
  type QueryPayload,
  type ChartConfiguration,
  type ChartDataPoint,
  type ChartData,
  type VgPlotProps,
  type DatabaseConnection,
  type QueryExecutionResult,
  type DataLoadingProgress,
  type TimeRange,
  type DataLoadingOptions,
  type AppError,
  type ErrorHandler,
  type LoadingState,
  type DataAnalysisState,
  type ExportOptions,
  type Partial,
  type Required,
  type Awaited,
} from '../index';

describe('Types Module', () => {
  describe('Enums', () => {
    it('should have correct PlotType enum values', () => {
      expect(PlotType.LinePlot).toBe('line');
      expect(PlotType.NumericalHistogram).toBe('numerical_histogram');
      expect(PlotType.CategoricalHistogram).toBe('categorical_histogram');
      expect(PlotType.Scatter).toBe('scatter');
      expect(PlotType.Bar).toBe('bar');
    });

    it('should have correct ConnectionStatus enum values', () => {
      expect(ConnectionStatus.Disconnected).toBe('disconnected');
      expect(ConnectionStatus.Connecting).toBe('connecting');
      expect(ConnectionStatus.Connected).toBe('connected');
      expect(ConnectionStatus.Error).toBe('error');
    });

    it('should have correct ErrorType enum values', () => {
      expect(ErrorType.Database).toBe('database');
      expect(ErrorType.API).toBe('api');
      expect(ErrorType.Validation).toBe('validation');
      expect(ErrorType.Network).toBe('network');
      expect(ErrorType.Visualization).toBe('visualization');
      expect(ErrorType.Unknown).toBe('unknown');
    });

    it('should have correct ExportFormat enum values', () => {
      expect(ExportFormat.CSV).toBe('csv');
      expect(ExportFormat.JSON).toBe('json');
      expect(ExportFormat.Excel).toBe('xlsx');
      expect(ExportFormat.Parquet).toBe('parquet');
    });
  });

  describe('Type Interfaces', () => {
    it('should create valid HpcJobData objects', () => {
      const jobData: HpcJobData = {
        time: new Date('2024-01-01T00:00:00Z'),
        submit_time: new Date('2024-01-01T00:00:00Z'),
        start_time: new Date('2024-01-01T00:01:00Z'),
        end_time: new Date('2024-01-01T01:00:00Z'),
        timelimit: 3600,
        nhosts: 1,
        ncores: 8,
        account: 'test-account',
        queue: 'normal',
        host: 'node001',
        jid: 'job123',
        unit: '%',
        jobname: 'test-job',
        exitcode: '0',
        host_list: 'node001',
        username: 'testuser',
        value_cpuuser: 75.5,
        value_gpu: 0.0,
        value_memused: 4096.0,
        value_memused_minus_diskcache: 3072.0,
        value_nfs: 100.0,
        value_block: 50.0,
      };

      expect(jobData.time).toBeInstanceOf(Date);
      expect(jobData.nhosts).toBe(1);
      expect(jobData.account).toBe('test-account');
      expect(jobData.value_cpuuser).toBe(75.5);
    });

    it('should create valid RawHpcJobData objects with nulls', () => {
      const rawJobData: RawHpcJobData = {
        time: '2024-01-01T00:00:00Z',
        submit_time: null,
        start_time: '2024-01-01T00:01:00Z',
        end_time: null,
        timelimit: 3600,
        nhosts: null,
        ncores: 8,
        account: 'test-account',
        queue: null,
        host: 'node001',
        jid: 'job123',
        unit: null,
        jobname: 'test-job',
        exitcode: '0',
        host_list: 'node001',
        username: 'testuser',
        value_cpuuser: 75.5,
        value_gpu: null,
        value_memused: 4096.0,
        value_memused_minus_diskcache: null,
        value_nfs: 100.0,
        value_block: 50.0,
      };

      expect(rawJobData.time).toBe('2024-01-01T00:00:00Z');
      expect(rawJobData.submit_time).toBeNull();
      expect(rawJobData.nhosts).toBeNull();
      expect(rawJobData.value_cpuuser).toBe(75.5);
    });

    it('should create valid ApiResponse objects', () => {
      const successResponse: ApiResponse<HpcJobData[]> = {
        success: true,
        data: [],
        metadata: {
          total_records: 100,
          query_time: 250,
          cache_hit: true,
        },
      };

      const errorResponse: ApiResponse<never> = {
        success: false,
        error: 'Database connection failed',
      };

      expect(successResponse.success).toBe(true);
      expect(successResponse.data).toEqual([]);
      expect(successResponse.metadata?.total_records).toBe(100);

      expect(errorResponse.success).toBe(false);
      expect(errorResponse.error).toBe('Database connection failed');
    });

    it('should create valid QueryResult objects', () => {
      const queryResult: QueryResult = {
        transferId: 'transfer-123',
        body: JSON.stringify({ chunks: [{ url: 'https://example.com/data.parquet' }] }),
        metadata: {
          total_partitions: 5,
          estimated_size: 1024000,
          chunk_count: 10,
        },
        chunks: [
          { url: 'https://example.com/chunk1.parquet' },
          { url: 'https://example.com/chunk2.parquet' },
        ],
      };

      expect(queryResult.transferId).toBe('transfer-123');
      expect(queryResult.chunks).toHaveLength(2);
      expect(queryResult.metadata?.chunk_count).toBe(10);
    });

    it('should create valid QueryPayload objects', () => {
      const payload: QueryPayload = {
        query: "SELECT * FROM fresco WHERE time BETWEEN '2024-01-01' AND '2024-01-02'",
        clientId: 'client-123',
        rowLimit: 1000,
      };

      expect(payload.query).toContain('SELECT');
      expect(payload.clientId).toBe('client-123');
      expect(payload.rowLimit).toBe(1000);
    });

    it('should create valid ChartConfiguration objects', () => {
      const chartConfig: ChartConfiguration = {
        type: PlotType.LinePlot,
        x_axis: 'time',
        y_axis: 'value_cpuuser',
        title: 'CPU Usage Over Time',
        width: 800,
        height: 400,
        color: '#ff6b6b',
        filters: { account: 'test-account' },
        aggregation: 'avg',
        binning: {
          enabled: true,
          bin_count: 20,
        },
      };

      expect(chartConfig.type).toBe(PlotType.LinePlot);
      expect(chartConfig.x_axis).toBe('time');
      expect(chartConfig.binning?.enabled).toBe(true);
      expect(chartConfig.aggregation).toBe('avg');
    });

    it('should create valid ChartData objects', () => {
      const chartData: ChartData = {
        points: [
          { x: 1, y: 75.5, label: 'Point 1', color: '#ff0000' },
          { x: 2, y: 80.2, metadata: { jobId: 'job123' } },
        ],
        x_domain: [0, 100],
        y_domain: [0, 100],
        metadata: {
          total_points: 2,
          x_type: 'numerical',
          y_type: 'numerical',
          aggregation_applied: 'avg',
        },
      };

      expect(chartData.points).toHaveLength(2);
      expect(chartData.points[0].x).toBe(1);
      expect(chartData.points[0].y).toBe(75.5);
      expect(chartData.metadata.total_points).toBe(2);
      expect(chartData.metadata.x_type).toBe('numerical');
    });

    it('should create valid DataLoadingProgress objects', () => {
      const progress: DataLoadingProgress = {
        stage: 'downloading',
        progress: 75,
        message: 'Downloading chunk 3 of 4',
        chunks_processed: 3,
        total_chunks: 4,
        rows_loaded: 15000,
      };

      expect(progress.stage).toBe('downloading');
      expect(progress.progress).toBe(75);
      expect(progress.chunks_processed).toBe(3);
      expect(progress.total_chunks).toBe(4);
    });

    it('should create valid TimeRange objects', () => {
      const timeRange: TimeRange = {
        start: new Date('2024-01-01T00:00:00Z'),
        end: new Date('2024-01-31T23:59:59Z'),
        timezone: 'America/New_York',
      };

      expect(timeRange.start).toBeInstanceOf(Date);
      expect(timeRange.end).toBeInstanceOf(Date);
      expect(timeRange.timezone).toBe('America/New_York');
    });

    it('should create valid AppError objects', () => {
      const appError: AppError = {
        type: ErrorType.Database,
        message: 'Connection failed',
        details: 'Timeout after 30 seconds',
        timestamp: new Date(),
        stack: 'Error: Connection failed\n    at ...',
        context: {
          operation: 'connect',
          host: 'localhost',
          port: 5432,
        },
      };

      expect(appError.type).toBe(ErrorType.Database);
      expect(appError.message).toBe('Connection failed');
      expect(appError.timestamp).toBeInstanceOf(Date);
      expect(appError.context?.operation).toBe('connect');
    });

    it('should create valid LoadingState objects', () => {
      const loadingState: LoadingState = {
        isLoading: true,
        message: 'Loading data...',
        progress: 45,
      };

      expect(loadingState.isLoading).toBe(true);
      expect(loadingState.message).toBe('Loading data...');
      expect(loadingState.progress).toBe(45);
    });

    it('should create valid ExportOptions objects', () => {
      const exportOptions: ExportOptions = {
        format: ExportFormat.CSV,
        include_headers: true,
        filename: 'fresco-data-export',
        max_rows: 10000,
        selected_columns: ['time', 'value_cpuuser', 'value_memused'],
      };

      expect(exportOptions.format).toBe(ExportFormat.CSV);
      expect(exportOptions.include_headers).toBe(true);
      expect(exportOptions.selected_columns).toHaveLength(3);
    });
  });

  describe('Function Types', () => {
    it('should define valid ErrorHandler function type', () => {
      const errorHandler: ErrorHandler = (error: AppError) => {
        console.error('Error occurred:', error.message);
      };

      const testError: AppError = {
        type: ErrorType.Unknown,
        message: 'Test error',
        timestamp: new Date(),
      };

      expect(() => errorHandler(testError)).not.toThrow();
    });
  });

  describe('Utility Types', () => {
    it('should create Partial types correctly', () => {
      interface TestInterface {
        required: string;
        alsoRequired: number;
        optional?: boolean;
      }

      const partial: Partial<TestInterface> = {
        required: 'test',
        // alsoRequired can be omitted
      };

      expect(partial.required).toBe('test');
      expect(partial.alsoRequired).toBeUndefined();
    });

    it('should create Required types correctly', () => {
      interface TestInterface {
        required: string;
        optional?: number;
      }

      const required: Required<TestInterface> = {
        required: 'test',
        optional: 42, // Must be provided due to Required<>
      };

      expect(required.required).toBe('test');
      expect(required.optional).toBe(42);
    });

    it('should handle Awaited types correctly', () => {
      type PromiseString = Promise<string>;
      type PromiseNumber = Promise<number>;
      type NonPromise = string;

      // These are compile-time checks that the types work correctly
      const awaitedString: Awaited<PromiseString> = 'test';
      const awaitedNumber: Awaited<PromiseNumber> = 42;
      const nonPromise: Awaited<NonPromise> = 'direct';

      expect(awaitedString).toBe('test');
      expect(awaitedNumber).toBe(42);
      expect(nonPromise).toBe('direct');
    });
  });

  describe('Complex Type Interactions', () => {
    it('should handle VgPlotProps with proper types', () => {
      // Mock database objects for testing
      const mockDb = {} as any;
      const mockConn = {} as any;

      const vgPlotProps: VgPlotProps = {
        db: mockDb,
        conn: mockConn,
        crossFilter: { filter: 'test' },
        dbLoading: false,
        dataLoading: true,
        tableName: 'job_data',
        xAxis: 'time',
        columnName: 'value_cpuuser',
        plotType: PlotType.LinePlot,
        width: 800,
        height: 400,
        topCategories: 10,
        onError: (error: Error) => console.error(error),
        onDataChange: (data: ChartData) => console.log('Data changed', data),
      };

      expect(vgPlotProps.plotType).toBe(PlotType.LinePlot);
      expect(vgPlotProps.width).toBe(800);
      expect(vgPlotProps.dbLoading).toBe(false);
      expect(vgPlotProps.dataLoading).toBe(true);
    });

    it('should handle DataAnalysisState with complex nested types', () => {
      const analysisState: DataAnalysisState = {
        loading: {
          isLoading: true,
          message: 'Analyzing data...',
          progress: 60,
        },
        data_loaded: false,
        table_name: 'job_data_complete',
        available_columns: ['time', 'value_cpuuser', 'nhosts'],
        selected_columns: ['time', 'value_cpuuser'],
        filters: {
          account: 'research-group',
          time_range: {
            start: '2024-01-01',
            end: '2024-01-31',
          },
        },
        charts: [
          {
            type: PlotType.LinePlot,
            x_axis: 'time',
            y_axis: 'value_cpuuser',
            title: 'CPU Usage Timeline',
          },
          {
            type: PlotType.NumericalHistogram,
            x_axis: 'value_memused',
            y_axis: 'count',
            binning: {
              enabled: true,
              bin_count: 25,
            },
          },
        ],
        error: {
          type: ErrorType.Database,
          message: 'Connection timeout',
          timestamp: new Date(),
        },
      };

      expect(analysisState.loading.isLoading).toBe(true);
      expect(analysisState.charts).toHaveLength(2);
      expect(analysisState.charts[0].type).toBe(PlotType.LinePlot);
      expect(analysisState.charts[1].binning?.enabled).toBe(true);
      expect(analysisState.error?.type).toBe(ErrorType.Database);
    });

    it('should handle DataLoadingOptions with callback', () => {
      const progressCallback = (progress: DataLoadingProgress) => {
        console.log(`${progress.stage}: ${progress.progress}%`);
      };

      const loadingOptions: DataLoadingOptions = {
        time_range: {
          start: new Date('2024-01-01'),
          end: new Date('2024-01-31'),
          timezone: 'UTC',
        },
        row_limit: 5000,
        table_name: 'job_data_filtered',
        include_demo_fallback: true,
        progress_callback: progressCallback,
      };

      expect(loadingOptions.time_range.start).toBeInstanceOf(Date);
      expect(loadingOptions.row_limit).toBe(5000);
      expect(loadingOptions.include_demo_fallback).toBe(true);
      expect(typeof loadingOptions.progress_callback).toBe('function');
    });
  });

  describe('Type Guards and Validation', () => {
    it('should handle nullable properties in RawHpcJobData', () => {
      const processRawData = (raw: RawHpcJobData): Partial<HpcJobData> => {
        return {
          time: raw.time ? new Date(raw.time) : undefined,
          nhosts: raw.nhosts ?? undefined,
          value_cpuuser: raw.value_cpuuser ?? undefined,
          account: raw.account ?? undefined,
        };
      };

      const rawData: RawHpcJobData = {
        time: '2024-01-01T00:00:00Z',
        submit_time: null,
        start_time: null,
        end_time: null,
        timelimit: null,
        nhosts: 4,
        ncores: null,
        account: null,
        queue: null,
        host: null,
        jid: null,
        unit: null,
        jobname: null,
        exitcode: null,
        host_list: null,
        username: null,
        value_cpuuser: 85.2,
        value_gpu: null,
        value_memused: null,
        value_memused_minus_diskcache: null,
        value_nfs: null,
        value_block: null,
      };

      const processed = processRawData(rawData);

      expect(processed.time).toBeInstanceOf(Date);
      expect(processed.nhosts).toBe(4);
      expect(processed.value_cpuuser).toBe(85.2);
      expect(processed.account).toBeUndefined();
    });
  });
});