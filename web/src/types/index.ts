/**
 * Core type definitions for the FRESCO application
 * 
 * This file contains all the main TypeScript interfaces and types used throughout
 * the application to ensure type safety and better developer experience.
 */

import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

// ===================
// HPC Job Data Types
// ===================

/**
 * Represents a single HPC job record from the database
 */
export interface HpcJobData {
  /** Timestamp when the job was recorded */
  time: Date;
  /** Timestamp when the job was submitted */
  submit_time: Date;
  /** Timestamp when the job started */
  start_time: Date;
  /** Timestamp when the job ended */
  end_time: Date;
  /** Time limit for the job in seconds */
  timelimit: number;
  /** Number of hosts used */
  nhosts: number;
  /** Number of cores used */
  ncores: number;
  /** Account name */
  account: string;
  /** Queue name */
  queue: string;
  /** Host name */
  host: string;
  /** Job ID */
  jid: string;
  /** Unit of measurement */
  unit: string;
  /** Job name */
  jobname: string;
  /** Exit code */
  exitcode: string;
  /** List of hosts used */
  host_list: string;
  /** Username */
  username: string;
  /** CPU user value */
  value_cpuuser: number;
  /** GPU value */
  value_gpu: number;
  /** Memory used value */
  value_memused: number;
  /** Memory used minus disk cache */
  value_memused_minus_diskcache: number;
  /** NFS value */
  value_nfs: number;
  /** Block I/O value */
  value_block: number;
}

/**
 * Raw HPC job data as it comes from the database (with potential nulls)
 */
export interface RawHpcJobData {
  time: string | null;
  submit_time: string | null;
  start_time: string | null;
  end_time: string | null;
  timelimit: number | null;
  nhosts: number | null;
  ncores: number | null;
  account: string | null;
  queue: string | null;
  host: string | null;
  jid: string | null;
  unit: string | null;
  jobname: string | null;
  exitcode: string | null;
  host_list: string | null;
  username: string | null;
  value_cpuuser: number | null;
  value_gpu: number | null;
  value_memused: number | null;
  value_memused_minus_diskcache: number | null;
  value_nfs: number | null;
  value_block: number | null;
}

// ===================
// API Response Types
// ===================

/**
 * Generic API response wrapper
 */
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  metadata?: {
    total_records: number;
    query_time: number;
    [key: string]: unknown;
  };
}

/**
 * Query result from the time series API
 */
export interface QueryResult {
  transferId?: string;
  body: string;
  metadata?: {
    total_partitions: number;
    estimated_size: number;
    chunk_count: number;
  };
  chunks?: Array<{ url: string }>;
}

/**
 * Payload for API queries
 */
export interface QueryPayload {
  query: string;
  clientId: string;
  rowLimit: number;
}

// ===================
// Chart & Visualization Types
// ===================

/**
 * Available plot types for visualizations
 */
export enum PlotType {
  LinePlot = 'line',
  NumericalHistogram = 'numerical_histogram',
  CategoricalHistogram = 'categorical_histogram',
  Scatter = 'scatter',
  Bar = 'bar'
}

/**
 * Chart configuration for VGPlot visualizations
 */
export interface ChartConfiguration {
  type: PlotType;
  x_axis: string;
  y_axis: string;
  title?: string;
  width?: number;
  height?: number;
  color?: string;
  filters?: Record<string, unknown>;
  aggregation?: 'sum' | 'avg' | 'count' | 'min' | 'max';
  binning?: {
    enabled: boolean;
    bin_count?: number;
    bin_width?: number;
  };
}

/**
 * Chart data point for processed visualization data
 */
export interface ChartDataPoint {
  x: string | number;
  y: number;
  label?: string;
  color?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Processed chart data ready for visualization
 */
export interface ChartData {
  points: ChartDataPoint[];
  x_domain?: [number, number] | string[];
  y_domain?: [number, number];
  metadata: {
    total_points: number;
    x_type: 'numerical' | 'categorical' | 'temporal';
    y_type: 'numerical' | 'categorical';
    aggregation_applied?: string;
  };
}

// ===================
// VGPlot Integration Types
// ===================

/**
 * VGPlot component props
 */
export interface VgPlotProps {
  db: AsyncDuckDB;
  conn: AsyncDuckDBConnection;
  crossFilter: unknown; // Will be properly typed when cross-filtering is implemented
  dbLoading: boolean;
  dataLoading: boolean;
  tableName: string;
  xAxis?: string;
  columnName: string;
  plotType: PlotType;
  width: number;
  height: number;
  topCategories?: number;
  onError?: (error: Error) => void;
  onDataChange?: (data: ChartData) => void;
}

/**
 * VGPlot chart element (returned by VGPlot functions)
 */
export interface VGPlotElement {
  node: HTMLElement;
  update: (data: unknown) => void;
  destroy: () => void;
}

/**
 * VGPlot configuration object
 */
export interface VGPlotConfig {
  x: string;
  y: string;
  inset?: number;
  fill?: string;
  stroke?: string;
  width?: number;
  height?: number;
  marginLeft?: number;
  marginRight?: number;
  marginTop?: number;
  marginBottom?: number;
}

// ===================
// Database & Connection Types
// ===================

/**
 * Database connection status
 */
export enum ConnectionStatus {
  Disconnected = 'disconnected',
  Connecting = 'connecting',
  Connected = 'connected',
  Error = 'error'
}

/**
 * Database connection info
 */
export interface DatabaseConnection {
  db: AsyncDuckDB;
  connection: AsyncDuckDBConnection;
  status: ConnectionStatus;
  lastError?: string;
  connected_at?: Date;
}

/**
 * Query execution result
 */
export interface QueryExecutionResult {
  success: boolean;
  data?: unknown[];
  error?: string;
  execution_time: number;
  rows_affected?: number;
}

// ===================
// Data Loading & Processing Types
// ===================

/**
 * Data loading progress information
 */
export interface DataLoadingProgress {
  stage: 'querying' | 'downloading' | 'processing' | 'complete';
  progress: number; // 0-100
  message: string;
  chunks_processed?: number;
  total_chunks?: number;
  rows_loaded?: number;
  error?: string;
}

/**
 * Time range for data queries
 */
export interface TimeRange {
  start: Date;
  end: Date;
  timezone?: string;
}

/**
 * Data loading options
 */
export interface DataLoadingOptions {
  time_range: TimeRange;
  row_limit: number;
  table_name: string;
  include_demo_fallback?: boolean;
  progress_callback?: (progress: DataLoadingProgress) => void;
}

// ===================
// Error Handling Types
// ===================

/**
 * Application error types
 */
export enum ErrorType {
  Database = 'database',
  API = 'api',
  Validation = 'validation',
  Network = 'network',
  Visualization = 'visualization',
  Unknown = 'unknown'
}

/**
 * Structured error information
 */
export interface AppError {
  type: ErrorType;
  message: string;
  details?: string;
  timestamp: Date;
  stack?: string;
  context?: Record<string, unknown>;
}

/**
 * Error handler function type
 */
export type ErrorHandler = (error: AppError) => void;

// ===================
// Component State Types
// ===================

/**
 * Loading state for components
 */
export interface LoadingState {
  isLoading: boolean;
  message?: string;
  progress?: number;
}

/**
 * Data analysis page state
 */
export interface DataAnalysisState {
  loading: LoadingState;
  data_loaded: boolean;
  table_name: string;
  available_columns: string[];
  selected_columns: string[];
  filters: Record<string, unknown>;
  charts: ChartConfiguration[];
  error?: AppError;
}

// ===================
// Export and Utility Types
// ===================

/**
 * Data export format options
 */
export enum ExportFormat {
  CSV = 'csv',
  JSON = 'json',
  Excel = 'xlsx',
  Parquet = 'parquet'
}

/**
 * Data export options
 */
export interface ExportOptions {
  format: ExportFormat;
  include_headers: boolean;
  filename?: string;
  max_rows?: number;
  selected_columns?: string[];
}

/**
 * Utility type for making all properties optional
 */
export type Partial<T> = {
  [P in keyof T]?: T[P];
};

/**
 * Utility type for making all properties required
 */
export type Required<T> = {
  [P in keyof T]-?: T[P];
};

/**
 * Utility type for extracting the value type from a Promise
 */
export type Awaited<T> = T extends Promise<infer U> ? U : T;