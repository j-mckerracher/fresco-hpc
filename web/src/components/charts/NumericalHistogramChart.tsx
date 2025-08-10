/**
 * Numerical Histogram Chart Component
 * 
 * Specialized component for creating numerical distribution histograms
 * with configurable binning and outlier filtering.
 */

import React from 'react';
import * as vg from '@uwdata/vgplot';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { 
    createHistogramView 
} from './DatabaseQueries';
import { 
    generateViewName, 
    getYAxisLabel, 
    calculateChartDimensions 
} from './ChartUtils';
import { getNumericalHistogramStyle } from './ChartStyles';
import { CHART_COLORS, CHART_DEFAULTS, COLUMN_CONFIGS } from './ChartConstants';
import { ErrorHandler, ErrorType } from '@/utils/errorHandler';

interface NumericalHistogramChartProps {
    /** Database instance */
    db: AsyncDuckDB;
    /** Database connection */
    conn: AsyncDuckDBConnection;
    /** Cross filter for interactions */
    crossFilter: unknown;
    /** Table name to query */
    tableName: string;
    /** Column name for histogram */
    columnName: string;
    /** Window dimensions */
    windowWidth: number;
    windowHeight: number;
    /** Chart size ratios */
    width: number;
    height: number;
    /** Number of bins for histogram */
    bins?: number;
}

/**
 * Create histogram bars with proper styling
 */
function createHistogramBars(viewName: string, columnName: string) {
    return vg.rectY(vg.from(viewName), {
        x: "bin_start",
        x2: "bin_end",
        y: "count",
        fill: CHART_COLORS.HISTOGRAM,
        stroke: CHART_COLORS.BLACK,
        strokeWidth: 0.5,
        fillOpacity: 0.7,
        tip: { 
            format: {
                x: (d: number) => `${columnName}: ${d.toFixed(2)}`,
                y: (d: number) => `Count: ${d}`
            }
        }
    });
}

/**
 * Create plot configuration for numerical histogram
 */
function createHistogramConfiguration(
    crossFilter: unknown,
    windowWidth: number,
    windowHeight: number,
    width: number,
    height: number,
    columnName: string
) {
    const { width: chartWidth, height: chartHeight } = calculateChartDimensions(
        windowWidth,
        windowHeight,
        width,
        height,
        CHART_DEFAULTS.MAX_WIDTH
    );

    return [
        vg.panZoomX(crossFilter),
        vg.marginLeft(CHART_DEFAULTS.MARGIN_LEFT),
        vg.marginBottom(CHART_DEFAULTS.MARGIN_BOTTOM),
        vg.marginTop(CHART_DEFAULTS.MARGIN_TOP),
        vg.marginRight(CHART_DEFAULTS.MARGIN_RIGHT),
        vg.width(chartWidth),
        vg.height(chartHeight),
        vg.xScale('linear'),
        vg.yScale('linear'),
        vg.xLabel(getYAxisLabel(columnName)),
        vg.yLabel("Count")
    ];
}

/**
 * Numerical Histogram Chart Component
 */
export const NumericalHistogramChart: React.FC<NumericalHistogramChartProps> = ({
    conn,
    crossFilter,
    tableName,
    columnName,
    windowWidth,
    windowHeight,
    width,
    height,
    bins = 50
}) => {
    /**
     * Create numerical histogram with appropriate binning
     */
    const createNumericalHistogram = async (): Promise<HTMLElement> => {
        try {
            const config = COLUMN_CONFIGS[columnName];
            
            // Generate unique view name for histogram
            const viewName = generateViewName(tableName, columnName, "hist_");
            
            // Create histogram view with specified number of bins
            await createHistogramView(
                conn,
                viewName,
                tableName,
                columnName,
                bins,
                config?.usePercentiles ? {
                    percentileLow: config.percentileLow || 1,
                    percentileHigh: config.percentileHigh || 99
                } : undefined
            );

            // Create the histogram plot
            const plot = vg.plot(
                createHistogramBars(viewName, columnName),
                ...createHistogramConfiguration(
                    crossFilter,
                    windowWidth,
                    windowHeight,
                    width,
                    height,
                    columnName
                ),
                vg.style(getNumericalHistogramStyle())
            );

            return plot;
        } catch (error) {
            throw ErrorHandler.handle(error, 'NumericalHistogramChart.createNumericalHistogram', ErrorType.Visualization);
        }
    };

    return { createNumericalHistogram };
};

export default NumericalHistogramChart;