/**
 * Custom hook for managing data loading in the data analysis page
 * 
 * Handles database connections, data loading, column validation,
 * and table creation with missing columns.
 */

import { useState, useRef, useCallback, useMemo } from 'react';
import { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import * as vg from '@uwdata/vgplot';
import { ErrorHandler, ErrorType } from '@/utils/errorHandler';
import { createHpcJobTableSQL, generateDemoDataSQL } from '@/utils/schema';

interface UseDataLoaderProps {
    /** DuckDB database instance */
    db: AsyncDuckDB | null;
    /** Whether database is loading */
    loading: boolean;
    /** Database error if any */
    error: Error | null;
    /** Whether data is currently loading */
    dataloading: boolean;
    /** Function to set data loading state */
    setDataLoading: (loading: boolean) => void;
    /** Cross filter setter */
    setCrossFilter: (filter: unknown) => void;
}

interface UseDataLoaderReturn {
    /** Database connection reference */
    conn: React.MutableRefObject<AsyncDuckDBConnection | undefined>;
    /** Loading error message */
    loadError: string | null;
    /** Available database columns */
    availableColumns: string[];
    /** Name of the current data table */
    dataTableName: string;
    /** Whether data is ready for visualization */
    dataReady: boolean;
    /** Function to load data */
    loadData: (useDemoData?: boolean) => Promise<void>;
    /** Function to handle download */
    handleDownload: () => Promise<void>;
    /** Function to retry loading with demo data */
    handleRetry: () => void;
}

/**
 * Hook for managing data loading operations
 */
export const useDataLoader = ({
    db,
    loading,
    error,
    dataloading,
    setDataLoading,
    setCrossFilter
}: UseDataLoaderProps): UseDataLoaderReturn => {
    const [loadError, setLoadError] = useState<string | null>(null);
    const [availableColumns, setAvailableColumns] = useState<string[]>([]);
    const [dataTableName, setDataTableName] = useState<string>('job_data');
    const [dataReady, setDataReady] = useState(false);
    const conn = useRef<AsyncDuckDBConnection | undefined>(undefined);

    // Memoize expected columns to prevent recreation on every render
    const expectedColumns = useMemo(() => [
        'value_cpuuser', 'value_gpu', 'value_memused',
        'value_memused_minus_diskcache', 'value_nfs', 'value_block'
    ], []);

    // Memoize missing columns calculation
    const missingColumns = useMemo(() => 
        expectedColumns.filter(col => !availableColumns.includes(col)),
        [expectedColumns, availableColumns]
    );

    /**
     * Check which columns are available in the database
     */
    const checkAvailableColumns = useCallback(async (): Promise<void> => {
        if (!conn.current) return;

        try {
            console.log('Checking available columns in job_data table...');

            const schemaCheck = await conn.current.query('SELECT * FROM job_data LIMIT 0');
            const columns = schemaCheck.schema.fields.map(f => f.name);
            
            setAvailableColumns(columns);

            const missingColumnsInDb = expectedColumns.filter(col => !columns.includes(col));
            if (missingColumnsInDb.length > 0) {
                console.warn('Missing expected columns:', missingColumnsInDb);
            }
        } catch (err) {
            const error = ErrorHandler.handle(err, 'useDataLoader.checkAvailableColumns', ErrorType.Database);
            console.error('Error checking available columns:', error);
        }
    }, []);

    /**
     * Create table with missing columns as defaults
     */
    const createMissingColumnsTable = useCallback(async (): Promise<void> => {
        if (!conn.current) return;

        try {
            console.log('Creating table with complete columns...');

            if (missingColumns.length === 0) {
                console.log('No missing columns need to be added');
                setDataTableName('job_data');
                return;
            }

            console.log('Creating table with missing columns:', missingColumns);

            // Drop existing complete table
            await conn.current.query('DROP TABLE IF EXISTS job_data_complete');

            // Create SQL for missing columns with zeros
            const missingColumnsSql = missingColumns.map(col => `0 as ${col}`).join(', ');

            // Create table with all existing columns plus missing ones
            await conn.current.query(`
                CREATE TABLE job_data_complete AS
                SELECT *, ${missingColumnsSql}
                FROM job_data;
            `);

            // Verify table creation
            const tableCheck = await conn.current.query('SELECT COUNT(*) as count FROM job_data_complete');
            const count = tableCheck.toArray()[0].count;
            console.log(`Created complete table with ${count} rows`);

            setDataTableName('job_data_complete');
        } catch (err) {
            const error = ErrorHandler.handle(err, 'useDataLoader.createMissingColumnsTable', ErrorType.Database);
            console.error('Error creating complete table:', error);
            setDataTableName('job_data');
        }
    }, [missingColumns]);

    /**
     * Create demo data for testing
     */
    const createDemoData = useCallback(async (connection: AsyncDuckDBConnection): Promise<void> => {
        try {
            console.log('Creating demo data...');

            // Drop existing tables
            await connection.query('DROP TABLE IF EXISTS job_data');

            // Create the job_data table using schema utility
            await connection.query(createHpcJobTableSQL('job_data', true));

            // Generate demo data
            const now = new Date();
            const startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); // 7 days ago
            
            const demoDataSQL = generateDemoDataSQL(
                'job_data',
                500,
                startDate.toISOString(),
                now.toISOString()
            );

            await connection.query(demoDataSQL);

            // Verify data was loaded
            const countCheck = await connection.query('SELECT COUNT(*) as count FROM job_data');
            const rowCount = countCheck.toArray()[0].count;
            console.log(`Created demo data with ${rowCount} rows`);

            if (rowCount === 0) {
                throw new Error('Failed to create demo data');
            }
        } catch (err) {
            const error = ErrorHandler.handle(err, 'useDataLoader.createDemoData', ErrorType.Database);
            console.error('Error creating demo data:', error);
            throw error;
        }
    }, []);

    /**
     * Main data loading function
     */
    const loadData = useCallback(async (useDemoData = false): Promise<void> => {
        console.log('loadData called:', {
            loading,
            db: !!db,
            dataloading,
            conn: !!conn.current,
            useDemoData
        });

        if (!loading && db && dataloading) {
            try {
                console.log('Starting data load');
                setDataLoading(true);
                setLoadError(null);
                setDataTableName('job_data');

                // Close existing connection
                if (conn.current) {
                    try {
                        await conn.current.close();
                        console.log('Closed existing database connection');
                    } catch (closeErr) {
                        console.warn('Warning when closing connection:', closeErr);
                    }
                }

                // Create new connection
                try {
                    conn.current = await db.connect();
                    console.log('Created new database connection');
                } catch (connErr) {
                    const error = ErrorHandler.handle(connErr, 'useDataLoader.loadData.connect', ErrorType.Database);
                    setLoadError(error.message);
                    setDataLoading(false);
                    return;
                }

                // Set up environment
                try {
                    await conn.current.query('LOAD icu');
                    await conn.current.query("SET TimeZone='America/New_York'");
                    console.log('Database environment setup complete');
                } catch (envErr) {
                    console.warn('Non-critical warning when setting up environment:', envErr);
                }

                let dataLoaded = false;

                if (useDemoData) {
                    // Create demo data directly
                    await createDemoData(conn.current);
                    dataLoaded = true;
                } else {
                    // Try to load from existing data
                    console.log('Checking for job_data_small table...');

                    try {
                        const existingCheck = await conn.current.query(`
                            SELECT name FROM sqlite_master 
                            WHERE type='table' AND name='job_data'
                        `);

                        if (existingCheck.toArray().length === 0) {
                            const result = await conn.current.query('SELECT COUNT(*) as count FROM job_data_small');
                            const count = result.toArray()[0].count;
                            console.log(`Found job_data_small table with ${count} rows`);

                            if (count > 0) {
                                await conn.current.query(`
                                    CREATE TABLE job_data AS
                                    SELECT * FROM job_data_small
                                `);

                                const verifyResult = await conn.current.query('SELECT COUNT(*) as count FROM job_data');
                                const newCount = verifyResult.toArray()[0].count;
                                console.log(`Successfully created job_data with ${newCount} rows`);

                                dataLoaded = newCount > 0;
                            }
                        } else {
                            const countCheck = await conn.current.query('SELECT COUNT(*) as count FROM job_data');
                            const rowCount = countCheck.toArray()[0].count;

                            if (rowCount > 0) {
                                console.log(`job_data already exists with ${rowCount} rows`);
                                dataLoaded = true;
                            }
                        }
                    } catch (err) {
                        console.log('Table check error:', err);
                    }
                }

                if (!dataLoaded) {
                    throw new Error('No real data available for the selected time period');
                }

                // Verify final data
                const countCheck = await conn.current.query('SELECT COUNT(*) as count FROM job_data');
                const rowCount = countCheck.toArray()[0].count;

                if (rowCount === 0) {
                    throw new Error('No data available for analysis');
                }

                console.log(`Final row count: ${rowCount} rows in job_data table`);

                // Initialize crossfilter and coordinator
                const newCrossFilter = vg.Selection.crossfilter();
                setCrossFilter(newCrossFilter);

                console.log('Setting up vgplot coordinator');
                vg.coordinator().databaseConnector(
                    vg.wasmConnector({
                        duckdb: db,
                        connection: conn.current,
                    })
                );

                // Check available columns and create complete table
                await checkAvailableColumns();
                await createMissingColumnsTable();

                console.log('Data load complete');
                setDataLoading(false);
                setDataReady(true);

            } catch (err) {
                const error = ErrorHandler.handle(err, 'useDataLoader.loadData', ErrorType.Database);
                console.error('Error in loadData:', error);
                setLoadError(error.message);
                setDataLoading(false);
                setDataReady(false);
            }
        }

        if (error) {
            const appError = ErrorHandler.handle(error, 'useDataLoader.loadData.duckdbError', ErrorType.Database);
            console.error('DuckDB Error:', appError);
            setLoadError(appError.message);
            setDataReady(false);
        }
    }, [db, loading, dataloading, error, setDataLoading, setCrossFilter, checkAvailableColumns, createMissingColumnsTable, createDemoData]);

    /**
     * Handle CSV download
     */
    const handleDownload = useCallback(async (): Promise<void> => {
        if (!conn.current) {
            throw new Error('Database connection not available');
        }

        try {
            // Dynamic import to avoid issues with SSR
            const { exportDataAsCSV } = await import('@/util/export');

            const sqlQuery = localStorage.getItem('SQLQuery');
            let fileName = 'fresco-data';
            let filters = '';

            if (sqlQuery) {
                console.log('Using stored query for CSV export:', sqlQuery);

                const dateRangeMatch = sqlQuery.match(/time BETWEEN '([^']+)' AND '([^']+)'/i);
                if (dateRangeMatch && dateRangeMatch.length >= 3) {
                    const startDate = new Date(dateRangeMatch[1]);
                    const endDate = new Date(dateRangeMatch[2]);

                    const startStr = startDate.toISOString().split('T')[0];
                    const endStr = endDate.toISOString().split('T')[0];

                    fileName = `fresco-data-${startStr}_to_${endStr}`;
                    filters = `time BETWEEN '${dateRangeMatch[1]}' AND '${dateRangeMatch[2]}'`;
                }
            } else {
                const now = new Date();
                const dateString = now.toISOString().split('T')[0];
                fileName = `fresco-data-${dateString}`;
            }

            await exportDataAsCSV(conn.current, dataTableName, fileName, filters);
            console.log('CSV export completed successfully');

        } catch (error) {
            const appError = ErrorHandler.handle(error, 'useDataLoader.handleDownload', ErrorType.Unknown);
            console.error('Download error:', appError);
            throw new Error(`Failed to download data: ${appError.message}`);
        }
    }, [dataTableName]);

    /**
     * Retry loading with demo data
     */
    const handleRetry = useCallback((): void => {
        setDataLoading(true);
        setLoadError(null);
        setDataReady(false);
        loadData(true);
    }, [loadData, setDataLoading]);

    return {
        conn,
        loadError,
        availableColumns,
        dataTableName,
        dataReady,
        loadData,
        handleDownload,
        handleRetry
    };
};

export default useDataLoader;