/**
 * Component type definitions for FRESCO
 * 
 * This file contains TypeScript interfaces and enums for React components,
 * providing type safety for component props and state.
 */

import { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { ChartData, AppError } from "../types";

/**
 * Available plot types for data visualization
 */
enum PlotType {
  LinePlot = 'line',
  NumericalHistogram = 'numerical_histogram',
  CategoricalHistogram = 'categorical_histogram',
}

/**
 * Props for the VgPlot component
 */
export interface VgPlotProps {
  /** DuckDB database instance */
  db: AsyncDuckDB;
  /** DuckDB connection instance */
  conn: AsyncDuckDBConnection;
  /** Cross-filter object for interactive filtering */
  crossFilter: unknown; // TODO: Replace with proper cross-filter type when implemented
  /** Whether the database is currently loading */
  dbLoading: boolean;
  /** Whether data is currently being loaded */
  dataLoading: boolean;
  /** Name of the database table to query */
  tableName: string;
  /** X-axis column name (optional for some plot types) */
  xAxis?: string;
  /** Y-axis column name */
  columnName: string;
  /** Type of plot to render */
  plotType: PlotType;
  /** Chart width in pixels */
  width: number;
  /** Chart height in pixels */
  height: number;
  /** Number of top categories to show (for categorical histograms) */
  topCategories?: number;
  /** Error handler callback */
  onError?: (error: AppError) => void;
  /** Data change callback */
  onDataChange?: (data: ChartData) => void;
}

/**
 * Props for loading animation component
 */
export interface LoadingAnimationProps {
  /** Whether to show the loading animation */
  visible: boolean;
  /** Custom loading message */
  message?: string;
  /** Loading progress (0-100) */
  progress?: number;
}

/**
 * Props for error boundary component
 */
export interface ErrorBoundaryProps {
  /** Child components to wrap */
  children: React.ReactNode;
  /** Error handler callback */
  onError?: (error: AppError) => void;
  /** Custom error display component */
  fallback?: React.ComponentType<{ error: AppError; resetError: () => void }>;
}

/**
 * Props for data export component
 */
export interface DataExportProps {
  /** Database connection */
  conn: AsyncDuckDBConnection;
  /** Table name to export */
  tableName: string;
  /** Available columns */
  columns: string[];
  /** Whether export is in progress */
  isExporting: boolean;
  /** Export completion callback */
  onExportComplete?: (success: boolean) => void;
}

/**
 * Props for chart container component
 */
export interface ChartContainerProps {
  /** Chart title */
  title?: string;
  /** Chart component */
  children: React.ReactNode;
  /** Container width */
  width?: number;
  /** Container height */
  height?: number;
  /** Loading state */
  loading?: boolean;
  /** Error state */
  error?: AppError;
  /** Refresh callback */
  onRefresh?: () => void;
}

// ===================
// Constants
// ===================

/** Primary color for charts (Purdue Boilermaker Gold) */
const FILL_COLOR = "#CFB991";

/** Chart color palette */
export const CHART_COLORS = {
  PRIMARY: "#CFB991",
  SECONDARY: "#9D7C0D",
  SUCCESS: "#28a745",
  DANGER: "#dc3545",
  WARNING: "#ffc107",
  INFO: "#17a2b8",
  LIGHT: "#f8f9fa",
  DARK: "#343a40",
} as const;

/** Default chart dimensions */
export const DEFAULT_CHART_DIMENSIONS = {
  WIDTH: 800,
  HEIGHT: 400,
  MARGIN: {
    TOP: 20,
    RIGHT: 20,
    BOTTOM: 40,
    LEFT: 80,
  },
} as const;

// Export legacy name for backward compatibility
export { FILL_COLOR as BOIILERMAKER_GOLD, PlotType };