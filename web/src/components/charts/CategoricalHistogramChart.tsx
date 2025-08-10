/**
 * Categorical Histogram Chart Component
 * 
 * Specialized component for creating categorical distribution bar charts
 * with automatic sorting and color coding.
 */

import React from 'react';
import * as vg from '@uwdata/vgplot';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { createCategoricalView } from './DatabaseQueries';
import { 
    generateViewName, 
    getYAxisLabel, 
    calculateChartDimensions 
} from './ChartUtils';
import { getCategoricalHistogramStyle } from './ChartStyles';
import { CHART_COLORS, CHART_DEFAULTS } from './ChartConstants';
import { ErrorHandler, ErrorType } from '@/utils/errorHandler';

interface CategoricalHistogramChartProps {
    /** Database instance */
    db: AsyncDuckDB;
    /** Database connection */
    conn: AsyncDuckDBConnection;
    /** Cross filter for interactions */
    crossFilter: unknown;
    /** Table name to query */
    tableName: string;
    /** Column name for categorical data */
    columnName: string;
    /** Window dimensions */
    windowWidth: number;
    windowHeight: number;
    /** Chart size ratios */
    width: number;
    height: number;
    /** Maximum number of categories to show */
    maxCategories?: number;
}

/**
 * Create categorical bars with proper styling and tooltips
 */
function createCategoricalBars(viewName: string, columnName: string) {
    return vg.barY(vg.from(viewName), {
        x: "category",
        y: "count",
        fill: CHART_COLORS.CATEGORICAL,
        stroke: CHART_COLORS.BLACK,
        strokeWidth: 0.5,
        fillOpacity: 0.8,
        tip: { 
            format: {
                x: (d: string) => `${columnName}: ${d}`,
                y: (d: number) => `Count: ${d}`
            }
        }
    });
}

/**
 * Create plot configuration for categorical histogram
 */
function createCategoricalConfiguration(
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
        vg.marginLeft(CHART_DEFAULTS.MARGIN_LEFT),
        vg.marginBottom(Math.max(CHART_DEFAULTS.MARGIN_BOTTOM, 80)), // Extra space for category labels
        vg.marginTop(CHART_DEFAULTS.MARGIN_TOP),
        vg.marginRight(CHART_DEFAULTS.MARGIN_RIGHT),
        vg.width(chartWidth),
        vg.height(chartHeight),
        vg.xScale('band'),
        vg.yScale('linear'),
        vg.xLabel(getYAxisLabel(columnName)),
        vg.yLabel("Count"),
        vg.xTickRotate(-45) // Rotate labels for better readability
    ];
}

/**
 * Categorical Histogram Chart Component
 */
export const CategoricalHistogramChart: React.FC<CategoricalHistogramChartProps> = ({
    conn,
    crossFilter,
    tableName,
    columnName,
    windowWidth,
    windowHeight,
    width,
    height,
    maxCategories = 20
}) => {
    /**
     * Create categorical histogram with sorted categories
     */
    const createCategoricalHistogram = async (): Promise<HTMLElement> => {
        try {
            // Generate unique view name for categorical data
            const viewName = generateViewName(tableName, columnName, "cat_");
            
            // Create categorical view with category counts
            await createCategoricalView(
                conn,
                viewName,
                tableName,
                columnName,
                maxCategories
            );

            // Create the categorical histogram plot
            const plot = vg.plot(
                createCategoricalBars(viewName, columnName),
                ...createCategoricalConfiguration(
                    crossFilter,
                    windowWidth,
                    windowHeight,
                    width,
                    height,
                    columnName
                ),
                vg.style(getCategoricalHistogramStyle())
            );

            return plot;
        } catch (error) {
            throw ErrorHandler.handle(error, 'CategoricalHistogramChart.createCategoricalHistogram', ErrorType.Visualization);
        }
    };

    return { createCategoricalHistogram };
};

export default CategoricalHistogramChart;