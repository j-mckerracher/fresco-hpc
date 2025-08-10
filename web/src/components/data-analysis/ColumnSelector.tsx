/**
 * Column selector component for choosing visualization columns
 * 
 * Allows users to select which columns to display as histograms or line plots
 */

import React, { useMemo } from 'react';
import MultiSelect from '@/components/multi-select';
import Vgmenu from '@/components/vgmenu';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

interface ColumnOption {
    value: string;
    label: string;
    numerical: boolean;
    linePlot: boolean;
}

interface ColumnSelectorProps {
    /** Available column options */
    columnOptions: ColumnOption[];
    /** Columns available in the database */
    availableColumns: string[];
    /** Currently selected histogram columns */
    histogramColumns: { value: string; label: string }[];
    /** Currently selected line plot columns */
    linePlotColumns: { value: string; label: string }[];
    /** Callback when histogram columns change */
    onHistogramColumnsChange: (columns: { value: string; label: string }[]) => void;
    /** Callback when line plot columns change */
    onLinePlotColumnsChange: (columns: { value: string; label: string }[]) => void;
    /** Database instance for VgMenu */
    db: AsyncDuckDB;
    /** Database connection for VgMenu */
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
 * Side panel for selecting visualization columns and filters
 */
export const ColumnSelector: React.FC<ColumnSelectorProps> = React.memo(({
    columnOptions,
    availableColumns,
    histogramColumns,
    linePlotColumns,
    onHistogramColumnsChange,
    onLinePlotColumnsChange,
    db,
    conn,
    crossFilter,
    dbLoading,
    dataLoading,
    tableName
}) => {
    // Memoize filtered options to prevent unnecessary recalculations
    const histogramOptions = useMemo(() => 
        columnOptions.filter(item =>
            availableColumns.includes(item.value) && !item.linePlot
        ), [columnOptions, availableColumns]
    );

    const linePlotOptions = useMemo(() => 
        columnOptions.filter(item =>
            item.linePlot && availableColumns.includes(item.value)
        ), [columnOptions, availableColumns]
    );

    // Memoize filtered selected columns
    const filteredHistogramColumns = useMemo(() =>
        histogramColumns.filter(col =>
            availableColumns.includes(col.value) &&
            !columnOptions.find(item => item.value === col.value)?.linePlot
        ), [histogramColumns, availableColumns, columnOptions]
    );

    const filteredLinePlotColumns = useMemo(() =>
        linePlotColumns.filter(col =>
            availableColumns.includes(col.value)
        ), [linePlotColumns, availableColumns]
    );

    return (
        <div className="w-1/4 px-4 flex flex-col gap-4">
            {/* Histogram Column Selector */}
            <div>
                <h1 className="text-white text-lg mb-2">
                    Choose columns to show as histograms:
                </h1>
                <MultiSelect
                    options={histogramOptions}
                    selected={filteredHistogramColumns}
                    onChange={onHistogramColumnsChange}
                    className=""
                />
            </div>

            {/* Line Plot Column Selector */}
            <div>
                <h1 className="text-white text-lg mb-2">
                    Choose columns to show as line plots:
                </h1>
                <MultiSelect
                    options={linePlotOptions}
                    selected={filteredLinePlotColumns}
                    onChange={onLinePlotColumnsChange}
                    className=""
                />
            </div>

            {/* Host Filter Menu */}
            {conn && (
                <div>
                    <h1 className="text-white text-lg mb-2">Filter by host:</h1>
                    <Vgmenu
                        db={db}
                        conn={conn}
                        crossFilter={crossFilter}
                        dbLoading={dbLoading}
                        dataLoading={dataLoading}
                        tableName={tableName}
                        columnName="host"
                        width={1200}
                        label="Choose a specific host: "
                    />
                </div>
            )}
        </div>
    );
});

ColumnSelector.displayName = 'ColumnSelector';

export default ColumnSelector;