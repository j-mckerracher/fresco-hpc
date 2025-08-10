/**
 * Visualization grid component for displaying charts
 * 
 * Renders histograms and line plots based on selected columns
 */

import React, { useMemo, useCallback } from 'react';
import VgPlot from '@/components/vgplot';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { PlotType } from '@/components/component_types';
import { column_pretty_names } from '@/components/vgplot';

interface VisualizationGridProps {
    /** Selected histogram columns */
    histogramColumns: { value: string; label: string }[];
    /** Selected line plot columns */
    linePlotColumns: { value: string; label: string }[];
    /** Column name to numerical type mapping */
    valueToNumerical: Map<string, boolean>;
    /** Database instance */
    db: AsyncDuckDB;
    /** Database connection */
    conn: AsyncDuckDBConnection | undefined;
    /** Cross filter instance */
    crossFilter: unknown;
    /** Whether database is loading */
    dbLoading: boolean;
    /** Whether data is loading */
    dataLoading: boolean;
    /** Name of the data table */
    tableName: string;
}

/**
 * Grid layout for displaying multiple visualizations
 */
export const VisualizationGrid: React.FC<VisualizationGridProps> = React.memo(({
    histogramColumns,
    linePlotColumns,
    valueToNumerical,
    db,
    conn,
    crossFilter,
    dbLoading,
    dataLoading,
    tableName
}) => {
    // Memoize the histogram chart renderer to prevent unnecessary recreations
    const renderHistogramChart = useCallback((col: { value: string; label: string }) => (
        <VgPlot
            key={`histogram-${col.value}`}
            db={db}
            conn={conn}
            crossFilter={crossFilter}
            dbLoading={dbLoading}
            dataLoading={dataLoading}
            tableName={tableName}
            columnName={col.value}
            width={0.75}
            height={0.4}
            topCategories={15}
            plotType={
                valueToNumerical.get(col.value)
                    ? PlotType.NumericalHistogram
                    : PlotType.CategoricalHistogram
            }
        />
    ), [db, conn, crossFilter, dbLoading, dataLoading, tableName, valueToNumerical]);

    // Memoize the line plot chart renderer
    const renderLinePlotChart = useCallback((col: { value: string; label: string }) => (
        <div 
            key={`lineplot-${col.value}`} 
            className="w-full mb-12 p-4 bg-zinc-900 rounded-lg border border-zinc-800"
        >
            <h2 className="text-xl text-center mb-4 text-purdue-boilermakerGold">
                {column_pretty_names.get(col.value) || col.value} over Time
            </h2>
            <VgPlot
                db={db}
                conn={conn}
                crossFilter={crossFilter}
                dbLoading={dbLoading}
                dataLoading={dataLoading}
                tableName={tableName}
                xAxis="time"
                columnName={col.value}
                width={0.6}
                height={0.75}
                plotType={PlotType.LinePlot}
            />
        </div>
    ), [db, conn, crossFilter, dbLoading, dataLoading, tableName]);

    // Memoize empty state message
    const emptyState = useMemo(() => (
        <div className="w-full flex items-center justify-center py-12">
            <div className="text-center text-gray-400">
                <p className="text-xl mb-2">No columns selected for visualization</p>
                <p>Choose columns from the sidebar to display charts</p>
            </div>
        </div>
    ), []);

    // Early return for no connection
    if (!conn) {
        return (
            <div className="flex-1 flex items-center justify-center">
                <div className="text-white">Database connection not available</div>
            </div>
        );
    }

    const hasNoColumns = histogramColumns.length === 0 && linePlotColumns.length === 0;

    return (
        <div className="flex gap-y-6 flex-row flex-wrap min-w-[25%] max-w-[75%] justify-between px-5">
            {/* Render histogram charts */}
            {histogramColumns.map(renderHistogramChart)}

            {/* Render line plot charts */}
            {linePlotColumns.map(renderLinePlotChart)}

            {/* Show message if no columns are selected */}
            {hasNoColumns && emptyState}
        </div>
    );
});

VisualizationGrid.displayName = 'VisualizationGrid';

export default VisualizationGrid;