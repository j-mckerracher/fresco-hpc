/**
 * Line Plot Chart Component
 * 
 * Specialized component for creating time-series line plots with
 * optional percentile-based outlier filtering.
 */

import React from 'react';
import * as vg from '@uwdata/vgplot';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { 
    createStandardAggregatedView, 
    createPercentileBasedView, 
    getDataStats
} from './DatabaseQueries';
import { 
    generateViewName, 
    getYAxisLabel, 
    calculateChartDimensions,
    createYDomain
} from './ChartUtils';
import { getLinePlotStyle } from './ChartStyles';
import { CHART_COLORS, CHART_DEFAULTS, COLUMN_CONFIGS } from './ChartConstants';
import { ErrorHandler, ErrorType } from '@/utils/errorHandler';

interface LinePlotChartProps {
    /** Database instance */
    db: AsyncDuckDB;
    /** Database connection */
    conn: AsyncDuckDBConnection;
    /** Cross filter for interactions */
    crossFilter: unknown;
    /** Table name to query */
    tableName: string;
    /** Column name for Y-axis */
    columnName: string;
    /** Column name for X-axis */
    xAxis: string;
    /** Window dimensions */
    windowWidth: number;
    windowHeight: number;
    /** Chart size ratios */
    width: number;
    height: number;
}

/**
 * Create line plot elements (line, area, dots)
 */
function createLinePlotElements(viewName: string, includeRange: boolean = true) {
    const elements = [
        vg.lineY(vg.from(viewName), {
            x: "hour",
            y: "avg_value",
            stroke: CHART_COLORS.PRIMARY,
            strokeWidth: CHART_DEFAULTS.STROKE_WIDTH,
        })
    ];

    if (includeRange) {
        elements.push(
            vg.areaY(vg.from(viewName), {
                x: "hour",
                y1: "min_value",
                y2: "max_value",
                fillOpacity: 0.2,
                fill: CHART_COLORS.PRIMARY
            })
        );
    }

    elements.push(
        vg.dotY(vg.from(viewName), {
            x: "hour",
            y: "avg_value",
            fill: CHART_COLORS.PRIMARY,
            stroke: CHART_COLORS.BLACK,
            strokeWidth: 1,
            r: CHART_DEFAULTS.DOT_RADIUS
        })
    );

    return elements;
}

/**
 * Create plot configuration with margins, scales, and domains
 */
function createPlotConfiguration(
    crossFilter: unknown,
    windowWidth: number,
    width: number,
    yMin?: number,
    yMax?: number
) {
    const { width: chartWidth } = calculateChartDimensions(
        windowWidth, 
        400, // Fixed height for line plots
        width, 
        1,
        CHART_DEFAULTS.MAX_WIDTH
    );

    const config = [
        vg.panZoomX(crossFilter),
        vg.marginLeft(CHART_DEFAULTS.MARGIN_LEFT),
        vg.marginBottom(CHART_DEFAULTS.MARGIN_BOTTOM),
        vg.marginTop(CHART_DEFAULTS.MARGIN_TOP),
        vg.marginRight(CHART_DEFAULTS.MARGIN_RIGHT),
        vg.width(chartWidth),
        vg.height(CHART_DEFAULTS.MAX_HEIGHT),
        vg.xScale('time'),
        vg.yScale('linear'),
        vg.xLabel("Time")
    ];

    if (yMin !== undefined && yMax !== undefined) {
        const [yDomainMin, yDomainMax] = createYDomain(yMin, yMax);
        config.push(vg.yDomain([yDomainMin, yDomainMax]));
    }

    return config;
}

/**
 * Line Plot Chart Component
 */
export const LinePlotChart: React.FC<LinePlotChartProps> = ({
    conn,
    crossFilter,
    tableName,
    columnName,
    xAxis,
    windowWidth,
    width
}) => {
    /**
     * Create line plot with appropriate data processing
     */
    const createLinePlot = async (): Promise<HTMLElement> => {
        try {
            const stats = await getDataStats(conn, tableName, columnName);
            const config = COLUMN_CONFIGS[columnName];
            let viewName: string;
            let plot: HTMLElement;

            // Handle special column configurations with percentile filtering
            if (config?.usePercentiles && 
                config.thresholdValue &&
                Math.abs(stats.min_val) > config.thresholdValue) {

                viewName = generateViewName(tableName, columnName, "robust_");
                await createPercentileBasedView(
                    conn,
                    viewName,
                    tableName,
                    columnName,
                    xAxis,
                    config.percentileLow!,
                    config.percentileHigh!
                );

                const robustStats = await getDataStats(conn, viewName, "avg_value");

                plot = vg.plot(
                    ...createLinePlotElements(viewName, true),
                    ...createPlotConfiguration(
                        crossFilter,
                        windowWidth,
                        width,
                        robustStats.min_val,
                        robustStats.max_val
                    ),
                    vg.yLabel(getYAxisLabel(columnName, config.labelSuffix || "")),
                    vg.style(getLinePlotStyle())
                );
            } else {
                // Standard aggregated view
                viewName = generateViewName(tableName, columnName, "agg_");
                await createStandardAggregatedView(conn, viewName, tableName, columnName, xAxis);

                plot = vg.plot(
                    ...createLinePlotElements(viewName, config?.enhancedVisualization),
                    ...createPlotConfiguration(
                        crossFilter,
                        windowWidth,
                        width,
                        stats.min_val,
                        stats.max_val
                    ),
                    vg.yLabel(getYAxisLabel(columnName)),
                    vg.style(getLinePlotStyle())
                );
            }

            return plot;
        } catch (error) {
            throw ErrorHandler.handle(error, 'LinePlotChart.createLinePlot', ErrorType.Visualization);
        }
    };

    return { createLinePlot };
};

export default LinePlotChart;