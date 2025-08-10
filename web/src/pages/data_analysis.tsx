import { useCallback, useEffect, useRef, useState } from "react";
import * as vg from "@uwdata/vgplot";
import VgPlot from "@/components/vgplot";
import { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { PlotType } from "@/components/component_types";
import MultiSelect from "@/components/multi-select";
import Vgmenu from "@/components/vgmenu";
import dynamic from 'next/dynamic';
import { exportDataAsCSV } from "@/util/export";
import { useDuckDB } from "@/context/DuckDBContext";
import { useNavigation } from "@/util/navigation";
import { column_pretty_names } from "@/components/vgplot";
import Header from "@/components/Header";

// Import LoadingAnimation with no SSR
const LoadingAnimation = dynamic(() => import('@/components/LoadingAnimation'), {
    ssr: false,
    loading: () => (
        <div className="fixed inset-0 flex flex-col items-center justify-center bg-black z-50">
            <div className="w-12 h-12 rounded-full bg-purdue-boilermakerGold animate-ping" />
            <p className="mt-4 text-xl text-white">Loading data...</p>
        </div>
    )
});

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

const value_to_numerical = new Map(
    COLUMN_NAMES.map((col) => [col.value, col.numerical])
);

const DataAnalysisPage = () => {
    console.log('DataAnalysis component rendered');


    // Use the context instead of the hook directly
    const {
        db,
        loading,
        error,
        dataloading,
        setDataLoading,
        histogramData,
        crossFilter,
        setCrossFilter
    } = useDuckDB();

    const [loadError, setLoadError] = useState<string | null>(null);
    const [downloading, setDownloading] = useState(false);
    const [histogramColumns, setHistogramColumns] = useState<
        { value: string; label: string }[]
    >([{ value: "time", label: "Time" }]);
    const [linePlotColumns, setLinePlotColumns] = useState<
        { value: string; label: string }[]
    >([]);
    const [availableColumns, setAvailableColumns] = useState<string[]>([]);
    const [dataTableName, setDataTableName] = useState<string>("job_data");
    const conn = useRef<AsyncDuckDBConnection | undefined>(undefined);
    const { navigate } = useNavigation();
    const [dataReady, setDataReady] = useState(false);

    // Function to check which columns are actually available in the database
    const checkAvailableColumns = async () => {
        if (!conn.current) return;

        try {
            console.log("Checking available columns in job_data table...");

            // Query the table schema
            const schemaCheck = await conn.current.query(`
        SELECT * FROM job_data LIMIT 0
      `);

            // Extract column names from schema
            const columns = schemaCheck.schema.fields.map(f => f.name);
            // console.log("Available columns:", columns);

            // Update state with available columns
            setAvailableColumns(columns);

            // Log which expected columns are missing
            const expectedColumns = [
                "value_cpuuser", "value_gpu", "value_memused",
                "value_memused_minus_diskcache", "value_nfs", "value_block"
            ];

            const missingColumns = expectedColumns.filter(col => !columns.includes(col));
            if (missingColumns.length > 0) {
                console.warn("Missing expected columns:", missingColumns);
            }
        } catch (err) {
            console.error("Error checking available columns:", err);
        }
    };

    // Function to add missing columns as a view with default values
    // New code
    const addMissingColumns = async () => {
        if (!conn.current) return;

        try {
            console.log("Adding missing columns as view...");

            // Check which columns we need to add
            const expectedColumns = [
                "value_cpuuser", "value_gpu", "value_memused",
                "value_memused_minus_diskcache", "value_nfs", "value_block"
            ];

            const missingColumns = expectedColumns.filter(col => !availableColumns.includes(col));

            if (missingColumns.length === 0) {
                console.log("No missing columns need to be added");
                setDataTableName("job_data");
                return;
            }

            console.log("Creating view with missing columns:", missingColumns);

            // First drop the view if it exists to avoid conflicts
            try {
                await conn.current.query(`DROP VIEW IF EXISTS job_data_with_missing`);
            } catch (dropErr) {
                console.warn("Warning when dropping view:", dropErr);
                // Continue despite errors - the view might not exist
            }

            // Create SQL for missing columns with zeros
            const missingColumnsSql = missingColumns.map(col => `0 as ${col}`).join(', ');

            // Create view with all existing columns plus missing ones
            try {
                await conn.current.query(`
                CREATE VIEW job_data_with_missing AS
                SELECT
                  *,
                  ${missingColumnsSql}
                FROM job_data;
            `);
                console.log("Successfully created view with missing columns");
            } catch (createErr) {
                console.error("Failed to create view with missing columns:", createErr);
                // Fall back to using the base table instead
                console.log("Falling back to using job_data table directly");
                setDataTableName("job_data");
                return;
            }

            // Verify the view was created correctly
            try {
                // Check the view to make sure all columns are present
                const viewCheck = await conn.current.query(`
                SELECT * FROM job_data_with_missing LIMIT 1
            `);

                const viewColumns = viewCheck.schema.fields.map(f => f.name);
                console.log("Columns in enhanced view:", viewColumns);

                // Check that all expected columns are present
                const allExpectedColumns = [...availableColumns, ...missingColumns];
                const missingAfterCreation = allExpectedColumns.filter(col => !viewColumns.includes(col));

                if (missingAfterCreation.length > 0) {
                    console.error("View creation incomplete. Still missing columns:", missingAfterCreation);
                    // Fall back to using the base table
                    setDataTableName("job_data");
                    return;
                }

                // Update availableColumns with the verified columns
                setAvailableColumns(viewColumns);

                // Only update dataTableName once we're sure the view is good
                console.log("Initial view check passed, now verifying view is accessible...");

                // Add a slight delay to ensure view is committed
                await new Promise(resolve => setTimeout(resolve, 100));

                // Do a secondary verification that the view exists and is accessible
                const viewExists = await verifyViewExists("job_data_with_missing");
                if (viewExists) {
                    console.log("View verification successful, using job_data_with_missing");
                    setDataTableName("job_data_with_missing");
                } else {
                    console.warn("View verification failed in second check, falling back to job_data");
                    setDataTableName("job_data");
                }
            } catch (verifyErr) {
                console.error("Error verifying view:", verifyErr);
                // Fall back to using the base table
                console.log("View verification failed, falling back to job_data table");
                setDataTableName("job_data");
            }
        } catch (err) {
            console.error("Error in addMissingColumns:", err);
            // Fall back to base table on any error
            setDataTableName("job_data");
        }
    };

    const createMissingColumnsTable = async () => {
        if (!conn.current) return;

        try {
            console.log("Creating table with complete columns...");

            // Check which columns we need to add
            const expectedColumns = [
                "value_cpuuser", "value_gpu", "value_memused",
                "value_memused_minus_diskcache", "value_nfs", "value_block"
            ];

            const missingColumns = expectedColumns.filter(col => !availableColumns.includes(col));

            if (missingColumns.length === 0) {
                console.log("No missing columns need to be added");
                setDataTableName("job_data");
                return;
            }

            console.log("Creating table with missing columns:", missingColumns);

            // Drop the table if it exists
            try {
                await conn.current.query(`DROP TABLE IF EXISTS job_data_complete`);
            } catch (dropErr) {
                console.warn("Warning when dropping table:", dropErr);
            }

            // Create SQL for missing columns with zeros
            const missingColumnsSql = missingColumns.map(col => `0 as ${col}`).join(', ');

            // Create table with all existing columns plus missing ones
            try {
                await conn.current.query(`
        CREATE TABLE job_data_complete AS
        SELECT
          *,
          ${missingColumnsSql}
        FROM job_data;
      `);
                console.log("Successfully created table with missing columns");

                // Verify table was created correctly
                const tableCheck = await conn.current.query(`
        SELECT COUNT(*) as count FROM job_data_complete
      `);
                const count = tableCheck.toArray()[0].count;
                console.log(`Created complete table with ${count} rows`);

                // Set the dataTableName to use the new table
                setDataTableName("job_data_complete");
            } catch (err) {
                console.error("Error creating complete table:", err);
                setDataTableName("job_data");
            }
        } catch (err) {
            console.error("Error in createMissingColumnsTable:", err);
            setDataTableName("job_data");
        }
    };

    const verifyViewExists = async (viewName: string): Promise<boolean> => {
        if (!conn.current) return false;

        try {
            // First check if it exists as a view
            const viewCheck = await conn.current.query(`
      SELECT name FROM sqlite_master 
      WHERE type='view' AND name='${viewName}'
    `);

            if (viewCheck.toArray().length > 0) {
                // Double-check we can actually query it
                const dataCheck = await conn.current.query(`
        SELECT * FROM ${viewName} LIMIT 1
      `);
                console.log(`View ${viewName} verified and accessible`);
                return true;
            }

            return false;
        } catch (err) {
            console.error(`Error verifying view ${viewName}:`, err);
            return false;
        }
    };

    const handleDownload = async () => {
        if (!conn.current) {
            alert("Database connection not available");
            return;
        }

        try {
            setDownloading(true);

            // Get the SQL query from localStorage
            const sqlQuery = localStorage.getItem("SQLQuery");
            let fileName = "fresco-data";
            let filters = "";

            // If we have a stored query, extract the date range for the filename and filter
            if (sqlQuery) {
                console.log("Using stored query for CSV export:", sqlQuery);

                // Extract date range from the SQL query using regex
                const dateRangeMatch = sqlQuery.match(/time BETWEEN '([^']+)' AND '([^']+)'/i);

                if (dateRangeMatch && dateRangeMatch.length >= 3) {
                    const startDate = new Date(dateRangeMatch[1]);
                    const endDate = new Date(dateRangeMatch[2]);

                    // Format dates for filename: YYYY-MM-DD
                    const startStr = startDate.toISOString().split('T')[0];
                    const endStr = endDate.toISOString().split('T')[0];

                    // Use date range in filename
                    fileName = `fresco-data-${startStr}_to_${endStr}`;

                    // Create filter for the CSV export
                    filters = `time BETWEEN '${dateRangeMatch[1]}' AND '${dateRangeMatch[2]}'`;

                    console.log(`Using date range filter: ${filters}`);
                    console.log(`Using filename: ${fileName}.csv`);
                } else {
                    console.warn("Could not extract date range from query, using default filename");
                }
            } else {
                console.warn("No stored query found, exporting all data with current date");
                // Fallback to current date if no query is stored
                const now = new Date();
                const dateString = now.toISOString().split('T')[0];
                fileName = `fresco-data-${dateString}`;
            }

            // Pass the filters to the export function
            await exportDataAsCSV(conn.current, dataTableName, fileName, filters);

            console.log("CSV export completed successfully");
        } catch (error) {
            console.error("Download error:", error);
            alert("Failed to download data: " + (error instanceof Error ? error.message : "Unknown error"));
        } finally {
            setDownloading(false);
        }
    };

    // Function to create sample data
    const createDemoData = async (connection: AsyncDuckDBConnection) => {
        try {
            console.log("Creating demo data...");

            // First drop existing tables if they exist
            await connection.query("DROP TABLE IF EXISTS job_data");

            // Create the job_data table
            await connection.query(`
        CREATE TABLE job_data (
          time TIMESTAMP,
          nhosts BIGINT,
          ncores BIGINT,
          account VARCHAR,
          queue VARCHAR,
          host VARCHAR,
          value_cpuuser DOUBLE,
          value_gpu DOUBLE,
          value_memused DOUBLE,
          value_memused_minus_diskcache DOUBLE,
          value_nfs DOUBLE,
          value_block DOUBLE
        )
      `);

            // Generate demo data
            const now = new Date();
            const startDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000); // 7 days ago

            // Create data points
            const timeRange = now.getTime() - startDate.getTime();
            const pointCount = 500;

            // Create values for a single batch insert
            const values = [];
            for (let i = 0; i < pointCount; i++) {
                const pointTime = new Date(startDate.getTime() + (timeRange * (i / pointCount)));
                const cpuValue = 50 + 40 * Math.sin(i / (pointCount / 10));
                const memValue = 30 + 20 * Math.cos(i / (pointCount / 15));
                const blockValue = 25 + 15 * Math.sin(i / (pointCount / 8)); // Add block value

                values.push(`('${pointTime.toISOString()}', 
          ${1 + Math.floor(Math.random() * 4)}, 
          ${4 + Math.floor(Math.random() * 28)}, 
          'research_${["cs", "physics", "bio"][Math.floor(Math.random() * 3)]}', 
          '${["normal", "high", "low"][Math.floor(Math.random() * 3)]}', 
          'node${100 + Math.floor(Math.random() * 100)}', 
          ${cpuValue}, 
          ${Math.random() > 0.9 ? 'NULL' : Math.random() * 10}, 
          ${memValue},
          ${memValue * 0.8},
          ${Math.random() * 5},
          ${blockValue})`);
            }

            // Insert in smaller batches to avoid query size limits
            const batchSize = 100;
            for (let i = 0; i < values.length; i += batchSize) {
                const batch = values.slice(i, i + batchSize);
                const batchQuery = `
          INSERT INTO job_data (time, nhosts, ncores, account, queue, host, value_cpuuser, value_gpu, value_memused, value_memused_minus_diskcache, value_nfs, value_block)
          VALUES ${batch.join(",")};
        `;
                await connection.query(batchQuery);
            }

            console.log(`Created demo data with ${pointCount} points`);

            // Verify data was loaded
            const countCheck = await connection.query("SELECT COUNT(*) as count FROM job_data");
            const rowCount = countCheck.toArray()[0].count;
            console.log(`Loaded ${rowCount} rows into job_data table`);

            if (rowCount === 0) {
                throw new Error("Failed to create demo data");
            }

            return true;
        } catch (err) {
            console.error("Error creating demo data:", err);
            throw err;
        }
    };

    // Function to load data, with option to force demo data
    const loadData = useCallback(async (useDemoData = false) => {
        console.log('loadData called:', {
            loading,
            db: !!db,
            dataloading,
            conn: !!conn.current,
            useDemoData,
            histogramData
        });

        // At the beginning of loadData function
        console.log(`loadData: STARTED with params:`, {
            useDemoData,
            db: !!db,
            loading,
            dataloading,
            memory: window.performance?.memory ? {
                jsHeapSizeLimit: Math.round(window.performance.memory.jsHeapSizeLimit / 1048576) + "MB",
                totalJSHeapSize: Math.round(window.performance.memory.totalJSHeapSize / 1048576) + "MB",
                usedJSHeapSize: Math.round(window.performance.memory.usedJSHeapSize / 1048576) + "MB"
            } : "Not available"
        });

        if (!loading && db && dataloading) {
            try {
                console.log('Starting data load');
                setDataLoading(true);
                setLoadError(null);

                // Reset the table name to avoid inconsistencies
                setDataTableName("job_data");

                // Close any existing connection
                if (conn.current) {
                    try {
                        await conn.current.close();
                        console.log("Closed existing database connection");
                    } catch (closeErr) {
                        console.warn("Warning when closing connection:", closeErr);
                    }
                }

                // Create a new connection
                try {
                    conn.current = await db.connect();
                    console.log("Created new database connection");
                } catch (connErr) {
                    console.error("Failed to create database connection:", connErr);
                    setLoadError("Could not connect to database: " +
                        (connErr instanceof Error ? connErr.message : "Unknown error"));
                    setDataLoading(false);
                    return;
                }

                // Set up environment with better error handling
                try {
                    await conn.current.query("LOAD icu");
                    await conn.current.query("SET TimeZone='America/New_York'");
                    console.log("Database environment setup complete");
                } catch (envErr) {
                    console.warn("Non-critical warning when setting up environment:", envErr);
                    // Continue despite errors - these settings aren't critical
                }

                // If using demo data or no query exists, create demo data directly
                const shouldCreateDemoData = useDemoData;
                let dataLoaded = false;

                if (!shouldCreateDemoData) {
                    console.log("Checking for job_data_small table...");

                    // First, check if job_data_small exists by actually running a query
                    // This is more reliable than checking for existence
                    try {
                        // First check if job_data already exists
                        const existingCheck = await conn.current.query(`
                          SELECT name FROM sqlite_master 
                          WHERE type='table' AND name='job_data'
                        `);

                        if (existingCheck.toArray().length === 0) {
                            // Now check for job_data_small
                            const result = await conn.current.query(`
                                SELECT COUNT(*) as count FROM job_data_small
                              `);

                            const count = result.toArray()[0].count;
                            console.log(`Found job_data_small table with ${count} rows`);

                            if (count > 0) {
                                try {
                                    // Try to copy the data with Unicode handling
                                    console.log("Creating job_data from job_data_small with Unicode handling...");

                                    // First create an empty table with the same schema
                                    await conn.current.query(`
                                        CREATE TABLE job_data AS
                                        SELECT * FROM job_data_small LIMIT 0
                                    `);

                                    // Then insert data with error handling for Unicode issues
                                    try {
                                        // Try the direct insert first
                                        await conn.current.query(`
                                            INSERT INTO job_data
                                            SELECT * FROM job_data_small
                                        `);

                                        // Verify copy was successful
                                        const verifyResult = await conn.current.query(`
                                            SELECT COUNT(*) as count FROM job_data
                                        `);

                                        const newCount = verifyResult.toArray()[0].count;
                                        console.log(`Successfully created job_data with ${newCount} rows`);

                                        if (newCount > 0) {
                                            dataLoaded = true;
                                        } else {
                                            throw new Error("job_data was created but has 0 rows");
                                        }
                                    } catch (unicodeError) {
                                        console.warn("Unicode error detected, falling back to using job_data_small directly:", unicodeError);

                                        // Drop the empty job_data table
                                        await conn.current.query(`DROP TABLE IF EXISTS job_data`);

                                        // Set dataTableName to job_data_small to use it directly
                                        setDataTableName("job_data_small");
                                        dataLoaded = true;
                                        return;
                                    }
                                } catch (tableError) {
                                    console.error("Error creating job_data table:", tableError);
                                    // Fall back to using job_data_small directly
                                    setDataTableName("job_data_small");
                                    dataLoaded = true;
                                }
                            }
                        } else {
                            // job_data already exists, check if it has data
                            const countCheck = await conn.current.query("SELECT COUNT(*) as count FROM job_data");
                            const rowCount = countCheck.toArray()[0].count;

                            if (rowCount > 0) {
                                console.log(`job_data already exists with ${rowCount} rows`);
                                dataLoaded = true;
                            }
                        }
                    } catch (err) {
                        console.log("Table check error:", err);
                        // If job_data_small doesn't exist, we'll handle that later
                    }
                }

                // If we haven't loaded data yet, don't create demo data
                if (!dataLoaded) {
                    console.log("No data loaded yet, but not creating demo data");
                    throw new Error("No real data available for the selected time period");
                }

                // Verify data was loaded
                const countCheck = await conn.current.query("SELECT COUNT(*) as count FROM job_data");
                const rowCount = countCheck.toArray()[0].count;

                if (rowCount === 0) {
                    throw new Error("No data available for analysis");
                }

                console.log(`Final row count: ${rowCount} rows in job_data table`);

                // Initialize crossfilter before setting up the coordinator
                const newCrossFilter = vg.Selection.crossfilter();
                setCrossFilter(newCrossFilter);

                // Set up the coordinator
                console.log("Setting up vgplot coordinator");
                vg.coordinator().databaseConnector(
                    vg.wasmConnector({
                        duckdb: db,
                        connection: conn.current,
                    })
                );

                // Check available columns and create a complete table
                await checkAvailableColumns();
                await createMissingColumnsTable();

                // Add a delay to ensure table is fully committed
                await new Promise(resolve => setTimeout(resolve, 200));

                // Verify the table exists after waiting
                try {
                    const finalCheck = await conn.current.query(`
                        SELECT COUNT(*) as count FROM ${dataTableName}
                      `);
                    const finalCount = finalCheck.toArray()[0].count;
                    console.log(`Final verification: ${dataTableName} has ${finalCount} rows`);
                } catch (err) {
                    console.error(`Failed to verify ${dataTableName}, falling back to job_data`);
                    setDataTableName("job_data");
                }

                try {
                    const finalTableCheck = await conn.current.query(`
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name='${dataTableName}'
                    `);

                    if (finalTableCheck.toArray().length === 0) {
                        // Try to check if it's a view instead
                        const viewCheck = await conn.current.query(`
                            SELECT name FROM sqlite_master 
                            WHERE type='view' AND name='${dataTableName}'
                        `);

                        if (viewCheck.toArray().length === 0) {
                            console.error(`Table/view "${dataTableName}" does not exist after data load!`);
                            // Fall back to job_data if it exists
                            const baseTableCheck = await conn.current.query(`
                                SELECT name FROM sqlite_master 
                                WHERE type='table' AND name='job_data'
                            `);

                            if (baseTableCheck.toArray().length > 0) {
                                console.log("Falling back to job_data table");
                                setDataTableName("job_data");
                            } else {
                                throw new Error(`Neither "${dataTableName}" nor "job_data" exist in the database`);
                            }
                        } else {
                            console.log(`"${dataTableName}" exists as a view`);
                        }
                    } else {
                        console.log(`"${dataTableName}" exists as a table`);
                    }
                } catch (err) {
                    console.error("Error checking final table consistency:", err);
                    // Don't throw here, as we already have data loaded, just log the error
                }

                console.log('Data load complete');
                setDataLoading(false);
            } catch (err) {
                console.error('Error in loadData:', err);
                setLoadError(err instanceof Error ? err.message : 'Unknown error loading data');
                setDataLoading(false);
            }
        }
        if (error) {
            console.error('DuckDB Error:', error);
            setLoadError('Database error: ' + (error instanceof Error ? error.message : 'Unknown error'));
        }
    }, [dataloading, db, error, loading, histogramData, setCrossFilter, setDataLoading]);

    useEffect(() => {
        loadData(false);

        // Set a timeout to detect loading hangs
        if (dataloading) {
            console.log("Setting hang detection timeout");
            const hangTimeout = setTimeout(() => {
                console.error("LOADING HANG DETECTED: Loading process has taken more than 3 minutes");
                console.log("Current state:", {
                    db: !!db,
                    loading,
                    dataloading,
                    histogramData,
                    dataTableName,
                    dataReady,
                    conn: !!conn.current
                });
            }, 180000); // 3 minutes

            return () => clearTimeout(hangTimeout);
        }
    }, [loadData, dataloading, db, loading, histogramData, dataTableName, dataReady]);

    // Log whenever loading state changes
    const shouldShowLoading = !db || !conn.current || dataloading;

    console.log('Loading state:', {
        shouldShowLoading,
        db: !!db,
        conn: !!conn.current,
        dataloading,
        loading
    });

    const handleRetry = () => {
        setDataLoading(true);
        setLoadError(null);
        // Force using demo data
        loadData(true);
    };

    useEffect(() => {
        const checkDataReady = async () => {
            if (db && conn.current && !dataloading && !loading) {
                try {
                    // First check if the specified table exists
                    const tableCheck = await conn.current.query(`
                    SELECT name FROM sqlite_master 
                    WHERE type='table' AND name='${dataTableName}'
                `);

                    if (tableCheck.toArray().length === 0) {
                        // Table doesn't exist, try job_data_small as fallback
                        console.warn(`Table ${dataTableName} not found, checking for job_data_small`);

                        const smallTableCheck = await conn.current.query(`
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name='job_data_small'
                    `);

                        if (smallTableCheck.toArray().length > 0) {
                            console.log("Falling back to job_data_small");
                            setDataTableName("job_data_small");
                            setDataReady(true);
                            return;
                        } else {
                            throw new Error("Neither main nor fallback table exists");
                        }
                    }

                    // Verify we can actually query the table
                    const check = await conn.current.query(`
                    SELECT COUNT(*) as count FROM ${dataTableName} LIMIT 1
                `);
                    console.log(`Data readiness check: ${dataTableName} is accessible`);
                    setDataReady(true);
                } catch (err) {
                    console.error(`Data readiness check failed for ${dataTableName}:`, err);
                    // Try job_data_small as a final fallback
                    try {
                        const fallbackCheck = await conn.current.query(`
                        SELECT COUNT(*) as count FROM job_data_small LIMIT 1
                    `);
                        console.log("Falling back to job_data_small after error");
                        setDataTableName("job_data_small");
                        setDataReady(true);
                    } catch (fallbackErr) {
                        console.error("Fallback also failed:", fallbackErr);
                        setDataReady(false);
                        setLoadError("Unable to access data tables. Try selecting a different time range.");
                    }
                }
            } else {
                setDataReady(false);
            }
        };

        checkDataReady();
    }, [db, conn.current, dataloading, loading, dataTableName]);

    return (
        <div className="bg-black min-h-screen flex flex-col">
            {shouldShowLoading || !dataReady ? (
                <>
                    {console.log('Rendering loading state')}
                    <LoadingAnimation />
                </>
            ) : loadError ? (
                <div className="flex flex-col items-center justify-center p-8 text-white">
                    <div className="bg-zinc-900 p-6 rounded-lg max-w-2xl text-center">
                        <h2 className="text-2xl text-red-500 mb-4">Error Loading Data</h2>
                        <p className="mb-6">{loadError}</p>
                        <div className="flex gap-4 justify-center">
                            <button
                                onClick={handleRetry}
                                className="px-4 py-2 bg-purdue-boilermakerGold text-black rounded-md">
                                Try Again with Demo Data
                            </button>
                            <button
                                onClick={() => navigate('/query_builder')}
                                className="px-4 py-2 bg-gray-700 text-white rounded-md">
                                Return to Query Builder
                            </button>
                        </div>
                    </div>
                </div>
            ) : (
                <>
                    {console.log('Rendering main content')}
                    <Header />
                    {/* Add download button at the top */}
                    <div className="flex justify-end p-4">
                        <button
                            onClick={handleDownload}
                            disabled={downloading}
                            className={`px-4 py-2 rounded-md transition-colors ${
                                downloading
                                    ? 'bg-gray-500 cursor-not-allowed'
                                    : 'bg-purdue-boilermakerGold text-black hover:bg-purdue-rush'
                            }`}
                        >
                            {downloading ? 'Downloading...' : 'Download Data as CSV'}
                        </button>
                        <a href="/query_builder">
                            <div className="px-4 py-2 bg-zinc-800 text-white rounded-md hover:bg-zinc-700 transition-colors">
                                Back to Query Builder
                            </div>
                        </a>

                    </div>


                    <div className="flex flex-row-reverse min-w-scren">
                        <div className="w-1/4 px-4 flex flex-col gap-4">
                            <div>
                                <h1 className="text-white text-lg">Choose columns to show as histograms:</h1>
                                <MultiSelect
                                    options={COLUMN_NAMES.filter(item =>
                                        availableColumns.includes(item.value) && !item.linePlot
                                    )}
                                    selected={histogramColumns.filter(col =>
                                        availableColumns.includes(col.value) &&
                                        !COLUMN_NAMES.find(item => item.value === col.value)?.linePlot
                                    )}
                                    onChange={setHistogramColumns}
                                    className=""
                                />
                            </div>
                            <div>
                                <h1 className="text-white text-lg">Choose columns to show as line plots:</h1>
                                <MultiSelect
                                    options={COLUMN_NAMES.filter((item) =>
                                        item.linePlot && availableColumns.includes(item.value)
                                    )}
                                    selected={linePlotColumns.filter(col =>
                                        availableColumns.includes(col.value)
                                    )}
                                    onChange={setLinePlotColumns}
                                    className=""
                                />
                            </div>
                            <Vgmenu
                                db={db}
                                conn={conn.current}
                                crossFilter={crossFilter}
                                dbLoading={loading}
                                dataLoading={dataloading}
                                tableName={dataTableName}
                                columnName={"host"}
                                width={1200}
                                label={"Choose a specific host: "}
                            />
                        </div>
                        <div className="flex gap-y-6 flex-row flex-wrap min-w-[25%] max-w-[75%] justify-between px-5">
                            {histogramColumns.map((col) => (
                                <VgPlot
                                    key={col.value}
                                    db={db}
                                    conn={conn.current}
                                    crossFilter={crossFilter}
                                    dbLoading={loading}
                                    dataLoading={dataloading}
                                    tableName={dataTableName}
                                    columnName={col.value}
                                    width={0.75}
                                    height={0.4}
                                    topCategories={15} // Show top 15 categories, adjust as needed
                                    plotType={
                                        value_to_numerical.get(col.value)
                                            ? PlotType.NumericalHistogram
                                            : PlotType.CategoricalHistogram
                                    }
                                />
                            ))}
                            {linePlotColumns.map((col) => (
                                <div key={col.value} className="w-full mb-12 p-4 bg-zinc-900 rounded-lg border border-zinc-800">
                                    <h2 className="text-xl text-center mb-4 text-purdue-boilermakerGold">
                                        {column_pretty_names.get(col.value) || col.value} over Time
                                    </h2>
                                    <VgPlot
                                        db={db}
                                        conn={conn.current}
                                        crossFilter={crossFilter}
                                        dbLoading={loading}
                                        dataLoading={dataloading}
                                        tableName={dataTableName}
                                        xAxis="time"
                                        columnName={col.value}
                                        width={0.75}
                                        height={0.6}
                                        plotType={PlotType.LinePlot}
                                    />
                                </div>
                            ))}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
};

export default DataAnalysisPage;
export { column_pretty_names };