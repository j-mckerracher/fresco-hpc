/**
 * Refactored VgPlot Component
 * 
 * Main orchestrator component that manages chart creation using
 * specialized chart components for different visualization types.
 */

import React, { useEffect, useRef, useState, useMemo } from 'react';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { PlotType } from '@/types';
import { LinePlotChart } from './LinePlotChart';
import { NumericalHistogramChart } from './NumericalHistogramChart';
import { CategoricalHistogramChart } from './CategoricalHistogramChart';
import { validateTableAndColumn, checkDataAvailability } from './DatabaseQueries';

interface VgPlotProps {
    /** Database instance */
    db: AsyncDuckDB;
    /** Database connection */
    conn: AsyncDuckDBConnection;
    /** Cross filter for interactions */
    crossFilter: unknown;
    /** Table name to query */
    tableName: string;
    /** Column name for visualization */
    columnName: string;
    /** Chart type */
    plotType: PlotType;
    /** X-axis column for line plots */
    xAxis?: string;
    /** Window dimensions */
    windowWidth: number;
    windowHeight: number;
    /** Chart size ratios */
    width: number;
    height: number;
    /** Number of bins for histograms */
    bins?: number;
    /** Maximum categories for categorical charts */
    maxCategories?: number;
}

/**
 * Determine if column contains categorical data
 */
const isCategoricalColumn = (columnName: string): boolean => {
    const categoricalColumns = [
        'account', 'queue', 'host', 'username', 'jobname', 
        'exitcode', 'unit', 'host_list'
    ];
    return categoricalColumns.includes(columnName);
};

/**
 * Main VgPlot Component
 */
export const VgPlotRefactored: React.FC<VgPlotProps> = React.memo(({
    db,
    conn,
    crossFilter,
    tableName,
    columnName,
    plotType,
    xAxis = 'start_time',
    windowWidth,
    windowHeight,
    width,
    height,
    bins = 50,
    maxCategories = 20
}) => {
    const containerRef = useRef<HTMLDivElement>(null);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [dataAvailable, setDataAvailable] = useState(false);

    // Memoize the categorical check to prevent recalculation
    const isColumnCategorical = useMemo(() => 
        isCategoricalColumn(columnName), 
        [columnName]
    );

    useEffect(() => {
        const createVisualization = async () => {
            if (!containerRef.current || !db || !conn) return;

            try {
                setIsLoading(true);
                setError(null);

                // Validate table and column existence
                await validateTableAndColumn(conn, tableName, columnName, xAxis);

                // Check data availability
                const dataCount = await checkDataAvailability(
                    conn, 
                    tableName, 
                    columnName, 
                    plotType === PlotType.LinePlot ? xAxis : undefined
                );

                if (dataCount === 0) {
                    setDataAvailable(false);
                    setError(`No data available for column "${columnName}"`);
                    return;
                }

                setDataAvailable(true);

                let chartElement: HTMLElement;

                // Create appropriate chart based on plot type and column type
                switch (plotType) {
                    case PlotType.LinePlot: {
                        if (!xAxis) {
                            throw new Error('X-axis column required for line plots');
                        }
                        const lineChart = LinePlotChart({
                            db,
                            conn,
                            crossFilter,
                            tableName,
                            columnName,
                            xAxis,
                            windowWidth,
                            windowHeight,
                            width,
                            height
                        });
                        chartElement = await lineChart.createLinePlot();
                        break;
                    }

                    case PlotType.Histogram: {
                        if (isColumnCategorical) {
                            const categoricalChart = CategoricalHistogramChart({
                                db,
                                conn,
                                crossFilter,
                                tableName,
                                columnName,
                                windowWidth,
                                windowHeight,
                                width,
                                height,
                                maxCategories
                            });
                            chartElement = await categoricalChart.createCategoricalHistogram();
                        } else {
                            const numericalChart = NumericalHistogramChart({
                                db,
                                conn,
                                crossFilter,
                                tableName,
                                columnName,
                                windowWidth,
                                windowHeight,
                                width,
                                height,
                                bins
                            });
                            chartElement = await numericalChart.createNumericalHistogram();
                        }
                        break;
                    }

                    default:
                        throw new Error(`Unsupported plot type: ${plotType}`);
                }

                // Clear previous chart and append new one
                containerRef.current.innerHTML = '';
                containerRef.current.appendChild(chartElement);

            } catch (err) {
                console.error('Error creating visualization:', err);
                setError(`Failed to create ${plotType.toLowerCase()}: ${err instanceof Error ? err.message : 'Unknown error'}`);
                setDataAvailable(false);
            } finally {
                setIsLoading(false);
            }
        };

        createVisualization();
    }, [
        db, 
        conn, 
        crossFilter, 
        tableName, 
        columnName, 
        plotType, 
        xAxis, 
        windowWidth, 
        windowHeight, 
        width, 
        height, 
        bins, 
        maxCategories,
        isColumnCategorical
    ]);

    // Error state
    if (error) {
        return (
            <div className="p-4 border border-red-300 rounded-md bg-red-50">
                <h3 className="text-sm font-medium text-red-800 mb-2">
                    Visualization Error
                </h3>
                <p className="text-red-700 text-sm">{error}</p>
                {!dataAvailable && (
                    <p className="text-red-600 text-xs mt-2">
                        Try selecting a different column or date range.
                    </p>
                )}
            </div>
        );
    }

    // Loading state
    if (isLoading) {
        return (
            <div className="flex items-center justify-center p-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                <span className="ml-3 text-gray-600">Creating visualization...</span>
            </div>
        );
    }

    return (
        <div className="relative w-full">
            <div ref={containerRef} className="w-full" />
        </div>
    );
});

VgPlotRefactored.displayName = 'VgPlotRefactored';

export default VgPlotRefactored;