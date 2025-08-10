/**
 * Refactored Data Analysis Page
 * 
 * This is the main data analysis interface for FRESCO, refactored into
 * smaller, focused components and custom hooks for better maintainability.
 */

import { useEffect, useState } from 'react';
import { useDuckDB } from '@/context/DuckDBContext';
import { useDataLoader } from '@/hooks/useDataLoader';
import { useColumnSelection } from '@/hooks/useColumnSelection';
import { DataLoadingState } from '@/components/data-analysis/DataLoadingState';
import { DataAnalysisToolbar } from '@/components/data-analysis/DataAnalysisToolbar';
import { ColumnSelector } from '@/components/data-analysis/ColumnSelector';
import { VisualizationGrid } from '@/components/data-analysis/VisualizationGrid';
import { ErrorHandler, ErrorType } from '@/utils/errorHandler';

// Column definitions - could be moved to a separate constants file
const COLUMN_NAMES = [
    { value: "time", label: "Time", numerical: true, linePlot: false },
    { value: "account", label: "Account", numerical: false, linePlot: false },
    { value: "queue", label: "Queue", numerical: false, linePlot: false },
    { value: "host", label: "Host", numerical: false, linePlot: false },
    { value: "exitcode", label: "Exit Code", numerical: false, linePlot: false },
    { value: "value_cpuuser", label: "CPU Usage", numerical: false, linePlot: true },
    { value: "value_gpu", label: "GPU Usage", numerical: false, linePlot: true },
    { value: "value_memused", label: "Memory Used", numerical: false, linePlot: true },
    { value: "value_memused_minus_diskcache", label: "Memory Used Minus Disk Cache", numerical: false, linePlot: true },
    { value: "value_nfs", label: "NFS Usage", numerical: false, linePlot: true },
    { value: "value_block", label: "Block Usage", numerical: false, linePlot: true },
    { value: "timelimit", label: "Time Limit", numerical: true, linePlot: false },
    { value: "username", label: "Username", numerical: false, linePlot: false }
];

/**
 * Main data analysis page component
 */
const DataAnalysisPage = () => {
    console.log('DataAnalysis component rendered');

    // DuckDB context
    const {
        db,
        loading,
        error,
        dataloading,
        setDataLoading,
        crossFilter,
        setCrossFilter
    } = useDuckDB();

    // Data loading hook
    const {
        conn,
        loadError,
        availableColumns,
        dataTableName,
        dataReady,
        loadData,
        handleDownload,
        handleRetry
    } = useDataLoader({
        db,
        loading,
        error,
        dataloading,
        setDataLoading,
        setCrossFilter
    });

    // Column selection hook
    const {
        histogramColumns,
        linePlotColumns,
        valueToNumerical,
        setHistogramColumns,
        setLinePlotColumns
    } = useColumnSelection({
        columnOptions: COLUMN_NAMES,
        availableColumns
    });

    // Download state
    const [downloading, setDownloading] = useState(false);

    // Load data on component mount
    useEffect(() => {
        loadData(false);

        // Set timeout to detect loading hangs
        if (dataloading) {
            console.log('Setting hang detection timeout');
            const hangTimeout = setTimeout(() => {
                console.error('LOADING HANG DETECTED: Loading process has taken more than 3 minutes');
                console.log('Current state:', {
                    db: !!db,
                    loading,
                    dataloading,
                    dataTableName,
                    dataReady,
                    conn: !!conn.current
                });
            }, 180000); // 3 minutes

            return () => clearTimeout(hangTimeout);
        }
    }, [loadData, dataloading, db, loading, dataTableName, dataReady]);

    // Log loading state changes
    const shouldShowLoading = !db || !conn.current || dataloading;
    console.log('Loading state:', {
        shouldShowLoading,
        db: !!db,
        conn: !!conn.current,
        dataloading,
        loading
    });

    /**
     * Handle CSV download with error handling
     */
    const handleDownloadWithState = async () => {
        try {
            setDownloading(true);
            await handleDownload();
        } catch (error) {
            const appError = ErrorHandler.handle(error, 'DataAnalysisPage.handleDownload', ErrorType.Unknown);
            alert(appError.message);
        } finally {
            setDownloading(false);
        }
    };

    return (
        <div className="bg-black min-h-screen flex flex-col">
            {/* Show loading state, error state, or main content */}
            <DataLoadingState
                isLoading={shouldShowLoading}
                dataReady={dataReady}
                loadError={loadError}
                onRetry={handleRetry}
            />

            {/* Main content - only render when data is ready */}
            {!shouldShowLoading && dataReady && !loadError && (
                <>
                    {console.log('Rendering main content')}
                    
                    {/* Toolbar with download and navigation */}
                    <DataAnalysisToolbar
                        downloading={downloading}
                        onDownload={handleDownloadWithState}
                    />

                    {/* Main layout with sidebar and visualization grid */}
                    <div className="flex flex-row-reverse min-w-screen">
                        {/* Column selector sidebar */}
                        <ColumnSelector
                            columnOptions={COLUMN_NAMES}
                            availableColumns={availableColumns}
                            histogramColumns={histogramColumns}
                            linePlotColumns={linePlotColumns}
                            onHistogramColumnsChange={setHistogramColumns}
                            onLinePlotColumnsChange={setLinePlotColumns}
                            db={db}
                            conn={conn.current}
                            crossFilter={crossFilter}
                            dbLoading={loading}
                            dataLoading={dataloading}
                            tableName={dataTableName}
                        />

                        {/* Visualization grid */}
                        <VisualizationGrid
                            histogramColumns={histogramColumns}
                            linePlotColumns={linePlotColumns}
                            valueToNumerical={valueToNumerical}
                            db={db}
                            conn={conn.current}
                            crossFilter={crossFilter}
                            dbLoading={loading}
                            dataLoading={dataloading}
                            tableName={dataTableName}
                        />
                    </div>
                </>
            )}
        </div>
    );
};

export default DataAnalysisPage;