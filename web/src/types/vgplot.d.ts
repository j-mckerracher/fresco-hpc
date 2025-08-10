/**
 * TypeScript declarations for @uwdata/vgplot
 * 
 * This file provides proper TypeScript definitions for the VGPlot library
 * used in FRESCO for data visualization, replacing all 'any' types with
 * proper type definitions.
 */

import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

declare module '@uwdata/vgplot' {
  
  // ===================
  // Core Types
  // ===================

  /**
   * VGPlot chart element that can be rendered in the DOM
   */
  export interface VGPlotElement {
    node: HTMLElement;
    update: (data: unknown) => void;
    destroy: () => void;
  }

  /**
   * Data source configuration for VGPlot
   */
  export interface DataSource {
    name: string;
    data?: unknown[];
    sql?: string;
    [key: string]: unknown;
  }

  /**
   * Chart configuration options
   */
  export interface ChartConfig {
    x?: string;
    y?: string;
    fill?: string;
    stroke?: string;
    inset?: number;
    width?: number;
    height?: number;
    marginLeft?: number;
    marginRight?: number;
    marginTop?: number;
    marginBottom?: number;
    [key: string]: unknown;
  }

  /**
   * Domain specification for chart axes
   */
  export type Domain = number[] | string[] | Date[];

  /**
   * Filter specification for data transformations
   */
  export interface FilterSpec {
    as?: string;
    [key: string]: unknown;
  }

  /**
   * Highlight specification for interactive charts
   */
  export interface HighlightSpec {
    by: string | string[];
    [key: string]: unknown;
  }

  /**
   * Style specification for chart appearance
   */
  export interface StyleSpec {
    'font-size'?: string;
    color?: string;
    [key: string]: string | number | undefined;
  }

  /**
   * Menu configuration for interactive controls
   */
  export interface MenuConfig {
    label?: string;
    options?: string[] | { label: string; value: unknown }[];
    [key: string]: unknown;
  }

  // ===================
  // Core Functions
  // ===================

  /**
   * Create a coordinator for managing chart interactions
   */
  export function coordinator(): unknown;

  /**
   * Create a WebAssembly connector for DuckDB integration
   */
  export function wasmConnector(config: { 
    duckdb: AsyncDuckDB; 
    connection: AsyncDuckDBConnection; 
  }): unknown;

  /**
   * Create a plot with the given configuration
   */
  export function plot(...elements: VGPlotElement[]): VGPlotElement;

  /**
   * Create a data source from a table name or SQL query
   */
  export function from(source: string, options?: Record<string, unknown>): DataSource;

  // ===================
  // Chart Types
  // ===================

  /**
   * Create a rectangular bar chart (histogram)
   */
  export function rectY(
    data: DataSource | unknown[], 
    config: ChartConfig & { x: string; y: string; fill?: string }
  ): VGPlotElement;

  /**
   * Create a line chart
   */
  export function lineY(
    data: DataSource | unknown[], 
    config: ChartConfig & { x: string; y: string; stroke?: string }
  ): VGPlotElement;

  /**
   * Create a dot plot (scatter plot)
   */
  export function dotY(
    data: DataSource | unknown[], 
    config: ChartConfig & { x: string; y: string; stroke?: string }
  ): VGPlotElement;

  // ===================
  // Data Transformations
  // ===================

  /**
   * Create bins for histogram data
   */
  export function bin(column: string): unknown;

  /**
   * Count aggregation function
   */
  export function count(): unknown;

  // ===================
  // Layout Functions
  // ===================

  /**
   * Set the width of the chart
   */
  export function width(pixels: number): VGPlotElement;

  /**
   * Set the height of the chart
   */
  export function height(pixels: number): VGPlotElement;

  /**
   * Set the left margin of the chart
   */
  export function marginLeft(pixels: number): VGPlotElement;

  /**
   * Set the aspect ratio of the chart
   */
  export function aspectRatio(ratio: number): VGPlotElement;

  // ===================
  // Domain Functions
  // ===================

  /**
   * Set the X-axis domain
   */
  export function xDomain(domain: Domain): VGPlotElement;

  /**
   * Set the Y-axis domain
   */
  export function yDomain(domain: Domain): VGPlotElement;

  /**
   * Create a fixed domain specification
   */
  export function Fixed(domain: Domain): Domain;

  // ===================
  // Interaction Functions
  // ===================

  /**
   * Add X-axis interval selection
   */
  export function intervalX(config: FilterSpec): VGPlotElement;

  /**
   * Add X-axis toggle selection
   */
  export function toggleX(config: FilterSpec): VGPlotElement;

  /**
   * Add highlighting interaction
   */
  export function highlight(config: HighlightSpec): VGPlotElement;

  /**
   * Add pan and zoom interaction for X-axis
   */
  export function panZoomX(filter: FilterSpec): VGPlotElement;

  // ===================
  // Style Functions
  // ===================

  /**
   * Apply custom styles to the chart
   */
  export function style(styles: StyleSpec): VGPlotElement;

  // ===================
  // UI Components
  // ===================

  /**
   * Create an interactive menu control
   */
  export function menu(config: MenuConfig): VGPlotElement;

  // ===================
  // Selection Object
  // ===================

  /**
   * Selection object for managing chart selections
   */
  export const Selection: {
    single: (name: string) => unknown;
    crossfilter: (name: string) => unknown;
    intersect: (name: string) => unknown;
    union: (name: string) => unknown;
    [key: string]: unknown;
  };
}