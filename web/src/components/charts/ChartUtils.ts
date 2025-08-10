/**
 * Chart utility functions for FRESCO visualizations
 * 
 * Provides helper functions for chart creation, data processing,
 * and configuration management.
 */

import { PlotType } from '@/components/component_types';
import { COLUMN_PRETTY_NAMES, COLUMN_UNITS, BIGINT_COLUMNS } from './ChartConstants';

/**
 * Generate appropriate plot title based on plot type and columns
 */
export const getPlotTitle = (plotType: PlotType, columnName: string, xAxis: string = ""): string => {
    const prettyColumn = COLUMN_PRETTY_NAMES.get(columnName) || columnName;

    switch (plotType) {
        case PlotType.CategoricalHistogram:
            return `Frequency of ${prettyColumn}`;
        case PlotType.LinePlot:
            const prettyXAxis = COLUMN_PRETTY_NAMES.get(xAxis) || xAxis;
            return `${prettyColumn} over ${prettyXAxis}`;
        case PlotType.NumericalHistogram:
            return `${prettyColumn} Distribution`;
        default:
            return prettyColumn;
    }
};

/**
 * Generate unique view name for database operations
 */
export const generateViewName = (tableName: string, columnName: string, suffix: string = ""): string => {
    const uniqueId = `${Date.now()}_${Math.floor(Math.random() * 10000)}`;
    const cleanColumn = columnName.replace(/[^a-zA-Z0-9]/g, '_');
    return `${tableName}_${suffix}${cleanColumn}_${uniqueId}`;
};

/**
 * Check if column needs special scaling for small values
 */
export const needsSpecialScaling = (columnName: string, min: number, max: number): boolean => {
    if (columnName === 'value_block') return true;
    if (Math.abs(max) < 0.01 && Math.abs(min) < 0.01) return true;
    return false;
};

/**
 * Generate Y-axis label with units and suffix
 */
export const getYAxisLabel = (columnName: string, suffix: string = ""): string => {
    const prettyName = COLUMN_PRETTY_NAMES.get(columnName) || columnName;
    const unit = COLUMN_UNITS.get(columnName);
    const unitStr = unit ? ` (${unit})` : '';
    return `${prettyName}${suffix}${unitStr}`;
};

/**
 * Check if column is a BigInt column requiring special handling
 */
export const isBigIntColumn = (columnName: string): boolean => {
    return BIGINT_COLUMNS.includes(columnName);
};

/**
 * Calculate appropriate chart dimensions based on window size and constraints
 */
export const calculateChartDimensions = (
    windowWidth: number,
    windowHeight: number,
    widthRatio: number,
    heightRatio: number,
    maxWidth: number = 800,
    maxHeight: number = 400
): { width: number; height: number } => {
    const width = Math.min(windowWidth * widthRatio, maxWidth);
    const height = Math.min(windowHeight * heightRatio, maxHeight);
    
    return { width, height };
};

/**
 * Calculate optimal number of categories for categorical charts based on available width
 */
export const calculateOptimalCategories = (
    availableWidth: number,
    requestedCategories?: number,
    pixelsPerCategory: number = 100,
    minCategories: number = 5
): number => {
    const maxCategories = Math.max(minCategories, Math.floor(availableWidth / pixelsPerCategory));
    return requestedCategories ? Math.min(requestedCategories, maxCategories) : maxCategories;
};

/**
 * Create Y-domain with appropriate buffering
 */
export const createYDomain = (yMin: number, yMax: number, bufferRatio: number = 0.1): [number, number] => {
    const yRange = yMax - yMin;
    const yBuffer = Math.max(yRange * bufferRatio, Math.abs(yMax) * bufferRatio || 1) * bufferRatio;
    return [yMin - yBuffer, yMax + yBuffer];
};

/**
 * Validate that required parameters are present for chart creation
 */
export const validateChartParams = (
    tableName: string,
    columnName: string,
    xAxis?: string
): void => {
    if (!tableName) {
        throw new Error('Table name is required');
    }
    if (!columnName) {
        throw new Error('Column name is required');
    }
    // xAxis is optional, only validate if provided
    if (xAxis !== undefined && !xAxis) {
        throw new Error('X-axis column name cannot be empty when specified');
    }
};

/**
 * Extract column name for scaled operations
 */
export const getScaledColumnName = (columnName: string, operation: 'double' | 'scaled'): string => {
    switch (operation) {
        case 'double':
            return `${columnName}_double`;
        case 'scaled':
            return `${columnName}_scaled`;
        default:
            return columnName;
    }
};

/**
 * Get appropriate SQL cast for column transformation
 */
export const getColumnCast = (columnName: string, targetType: 'DOUBLE' | 'BIGINT'): string => {
    return `CAST(${columnName} AS ${targetType})`;
};

/**
 * Create scaling factor text for display
 */
export const getScalingFactorText = (factor: number): string => {
    if (factor === 1000000) {
        return '(×10⁻⁶)';
    }
    if (factor === 1000) {
        return '(×10⁻³)';
    }
    return `(×10⁻${Math.log10(factor)})`;
};