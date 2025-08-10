/**
 * Chart constants and configuration for FRESCO visualizations
 * 
 * Centralized constants for column names, units, configurations,
 * and other chart-related settings.
 */

// Column display names mapping
export const COLUMN_PRETTY_NAMES = new Map([
    ["time", "Time"],
    ["submit_time", "Submit Time"],
    ["start_time", "Start Time"],
    ["end_time", "End Time"],
    ["timelimit", "Time Limit"],
    ["nhosts", "Number of Hosts"],
    ["ncores", "Number of Cores"],
    ["account", "Account"],
    ["queue", "Queue"],
    ["host", "Host"],
    ["jid", "Job ID"],
    ["unit", "Unit"],
    ["jobname", "Job Name"],
    ["exitcode", "Exit Code"],
    ["host_list", "Host List"],
    ["username", "Username"],
    ["value_cpuuser", "CPU Usage"],
    ["value_gpu", "GPU Usage"],
    ["value_memused", "Memory Used"],
    ["value_memused_minus_diskcache", "Memory Used Minus Disk Cache"],
    ["value_nfs", "NFS Usage"],
    ["value_block", "Block Usage"],
]);

// Column units mapping
export const COLUMN_UNITS = new Map([
    ["value_cpuuser", "CPU %"],
    ["value_gpu", "GPU %"],
    ["value_memused", "GB"],
    ["value_memused_minus_diskcache", "GB"],
    ["value_nfs", "MB/s"],
    ["value_block", "GB/s"],
]);

// Columns that require BigInt handling
export const BIGINT_COLUMNS = ["nhosts", "ncores"];

// Retry configuration
export const MAX_RETRY_ATTEMPTS = 5;
export const RETRY_DELAY_MS = 300;

// Chart dimensions and spacing
export const CHART_DEFAULTS = {
    MARGIN_LEFT: 75,
    MARGIN_RIGHT: 30,
    MARGIN_TOP: 30,
    MARGIN_BOTTOM: 55,
    MAX_WIDTH: 800,
    MAX_HEIGHT: 400,
    HISTOGRAM_HEIGHT: 300,
    INSET: 1,
    STROKE_WIDTH: 3,
    DOT_RADIUS: 5,
} as const;

// Column-specific configuration for special handling
export interface ColumnConfig {
    usePercentiles?: boolean;
    percentileLow?: number;
    percentileHigh?: number;
    thresholdValue?: number;
    labelSuffix?: string;
    enhancedVisualization?: boolean;
}

export const COLUMN_CONFIGS: Record<string, ColumnConfig> = {
    value_cpuuser: {
        usePercentiles: true,
        percentileLow: 0.01,
        percentileHigh: 0.99,
        thresholdValue: 1000,
        labelSuffix: " (excluding outliers)"
    },
    value_nfs: {
        usePercentiles: true,
        percentileLow: 0.02,
        percentileHigh: 0.98,
        thresholdValue: 10,
        labelSuffix: " (excluding outliers)"
    },
    value_block: {
        enhancedVisualization: true
    }
};

// Color scheme
export const CHART_COLORS = {
    PRIMARY: "#CFB991", // Boilermaker Gold
    SECONDARY: "#9D7C0D",
    HISTOGRAM: "#CFB991", // Boilermaker Gold for histograms
    CATEGORICAL: "#CFB991", // Boilermaker Gold for categorical charts
    WHITE: "#FFFFFF",
    BLACK: "#000000",
    TRANSPARENT: "transparent"
} as const;