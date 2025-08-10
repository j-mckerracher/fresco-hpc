/**
 * Custom hook for managing column selection in data analysis
 * 
 * Handles the selection of columns for different visualization types
 * and provides filtering based on available columns.
 */

import { useState, useMemo, useCallback } from 'react';

interface ColumnOption {
    value: string;
    label: string;
    numerical: boolean;
    linePlot: boolean;
}

interface SelectedColumn {
    value: string;
    label: string;
}

interface UseColumnSelectionProps {
    /** All available column options */
    columnOptions: ColumnOption[];
    /** Columns actually available in the database */
    availableColumns: string[];
}

interface UseColumnSelectionReturn {
    /** Selected histogram columns */
    histogramColumns: SelectedColumn[];
    /** Selected line plot columns */
    linePlotColumns: SelectedColumn[];
    /** Map of column values to numerical status */
    valueToNumerical: Map<string, boolean>;
    /** Function to set histogram columns */
    setHistogramColumns: (columns: SelectedColumn[]) => void;
    /** Function to set line plot columns */
    setLinePlotColumns: (columns: SelectedColumn[]) => void;
    /** Available options for histograms */
    availableHistogramOptions: ColumnOption[];
    /** Available options for line plots */
    availableLinePlotOptions: ColumnOption[];
}

/**
 * Hook for managing column selection state and filtering
 */
export const useColumnSelection = ({
    columnOptions,
    availableColumns
}: UseColumnSelectionProps): UseColumnSelectionReturn => {
    
    const [histogramColumns, setHistogramColumns] = useState<SelectedColumn[]>([
        { value: 'time', label: 'Time' }
    ]);
    
    const [linePlotColumns, setLinePlotColumns] = useState<SelectedColumn[]>([]);

    /**
     * Create a map for quick lookup of numerical status
     */
    const valueToNumerical = useMemo(() => {
        return new Map(columnOptions.map(col => [col.value, col.numerical]));
    }, [columnOptions]);

    /**
     * Get available options for histograms
     * Excludes line plot columns and only includes available columns
     */
    const availableHistogramOptions = useMemo(() => {
        return columnOptions.filter(item =>
            availableColumns.includes(item.value) && !item.linePlot
        );
    }, [columnOptions, availableColumns]);

    /**
     * Get available options for line plots
     * Only includes line plot columns that are available
     */
    const availableLinePlotOptions = useMemo(() => {
        return columnOptions.filter(item =>
            item.linePlot && availableColumns.includes(item.value)
        );
    }, [columnOptions, availableColumns]);

    /**
     * Filter histogram columns to only include valid ones
     */
    const handleSetHistogramColumns = useCallback((columns: SelectedColumn[]) => {
        // Filter to only include columns that are available and not line plot columns
        const validColumns = columns.filter(col => {
            const columnOption = columnOptions.find(opt => opt.value === col.value);
            return availableColumns.includes(col.value) && 
                   columnOption && 
                   !columnOption.linePlot;
        });
        
        setHistogramColumns(validColumns);
    }, [columnOptions, availableColumns]);

    /**
     * Filter line plot columns to only include valid ones
     */
    const handleSetLinePlotColumns = useCallback((columns: SelectedColumn[]) => {
        // Filter to only include columns that are available and are line plot columns
        const validColumns = columns.filter(col => {
            const columnOption = columnOptions.find(opt => opt.value === col.value);
            return availableColumns.includes(col.value) && 
                   columnOption && 
                   columnOption.linePlot;
        });
        
        setLinePlotColumns(validColumns);
    }, [columnOptions, availableColumns]);

    return {
        histogramColumns,
        linePlotColumns,
        valueToNumerical,
        setHistogramColumns: handleSetHistogramColumns,
        setLinePlotColumns: handleSetLinePlotColumns,
        availableHistogramOptions,
        availableLinePlotOptions
    };
};

export default useColumnSelection;