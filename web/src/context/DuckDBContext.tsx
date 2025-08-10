import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';
import { AsyncDuckDB } from 'duckdb-wasm-kit';
import { useDuckDb } from 'duckdb-wasm-kit';
import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

interface DuckDBContextType {
    db: AsyncDuckDB | null;
    loading: boolean;
    error: Error | null;
    dataloading: boolean;
    setDataLoading: (loading: boolean) => void;
    histogramData: boolean;
    setHistogramData: (data: boolean) => void;
    crossFilter: any;
    setCrossFilter: (filter: any) => void;
    connection: AsyncDuckDBConnection | null;
    createConnection: () => Promise<AsyncDuckDBConnection | null>;
}

const DuckDBContext = createContext<DuckDBContextType>({
    db: null,
    loading: true,
    error: null,
    dataloading: true,
    setDataLoading: () => {},
    histogramData: false,
    setHistogramData: () => {},
    crossFilter: null,
    setCrossFilter: () => {},
    connection: null,
    createConnection: async () => null
});

export const useDuckDB = () => useContext(DuckDBContext);

interface DuckDBProviderProps {
    children: ReactNode;
}

// Create a single instance of DuckDB that persists across renders
let duckDBInstance: AsyncDuckDB | null = null;
let connectionInstance: AsyncDuckDBConnection | null = null;

export const DuckDBProvider: React.FC<DuckDBProviderProps> = ({ children }) => {
    // Use duckDBKit but keep our persistent instance if it exists
    const duckDBKit = useDuckDb();

    // Create state for other variables we need to track
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<Error | null>(null);
    const [dataloading, setDataLoading] = useState(true);
    const [histogramData, setHistogramData] = useState(false);
    const [crossFilter, setCrossFilter] = useState<any>(null);

    // Initialize the database once
    useEffect(() => {
        const initDB = async () => {
            console.log("DuckDBContext: Starting database initialization");
            try {
                // If we already have a DB instance, use it
                if (duckDBInstance) {
                    console.log("DuckDBContext: Using existing DuckDB instance");
                    setLoading(false);
                    return;
                }

                // Wait for duckDBKit to initialize
                console.log("DuckDBContext: Waiting for duckDBKit, status:",
                    {loading: duckDBKit.loading, hasDB: !!duckDBKit.db});

                if (!duckDBKit.loading && duckDBKit.db) {
                    console.log('DuckDBContext: DuckDB initialized successfully');
                    duckDBInstance = duckDBKit.db;
                    setLoading(false);

                    // Initialize a reusable connection
                    try {
                        console.log("DuckDBContext: Creating initial connection");
                        connectionInstance = await duckDBInstance.connect();
                        console.log("DuckDBContext: Connection created, setting up environment");

                        await connectionInstance.query("LOAD icu");
                        console.log("DuckDBContext: ICU loaded");

                        await connectionInstance.query("SET TimeZone='America/New_York'");
                        console.log("DuckDBContext: Timezone set");

                        await connectionInstance.query("SET temp_directory='browser-data/tmp'");
                        console.log("DuckDBContext: Temp directory set");

                        await connectionInstance.query("PRAGMA memory_limit='20GB'");
                        console.log("DuckDBContext: Memory limit set");

                    } catch (err) {
                        console.error('DuckDBContext: Initial connection setup error:', err);
                    }
                } else {
                    console.log("DuckDBContext: Still waiting for duckDBKit initialization");
                }
            } catch (err) {
                console.error('DuckDBContext: Error initializing DuckDB:', err);
                setError(err instanceof Error ? err : new Error('Unknown error initializing DuckDB'));
                setLoading(false);
            }
        };

        initDB();
    }, [duckDBKit.db, duckDBKit.loading]);

    // Cleanup function for when component unmounts
    useEffect(() => {
        return () => {
            if (connectionInstance) {
                try {
                    // Clean up temp directory explicitly before closing
                    connectionInstance.query("CALL IF EXISTS delete_files_in_directory('browser-data/tmp')").catch(err => {
                        console.warn('Error cleaning temp files on unmount:', err);
                    });
                    connectionInstance.close().catch(err => {
                        console.warn('Error closing connection on unmount:', err);
                    });
                } catch (err) {
                    console.warn('Error during cleanup:', err);
                }
            }
            
            if (duckDBInstance) {
                try {
                    // Properly terminate DuckDB instance
                    duckDBInstance.terminate();
                    duckDBInstance = null;
                    connectionInstance = null;
                } catch (err) {
                    console.warn('Error terminating DuckDB:', err);
                }
            }
        };
    }, []);

    // Add cleanup on page unload
    useEffect(() => {
        const handleBeforeUnload = () => {
            if (connectionInstance) {
                try {
                    // Attempt to clean up temp files when page unloads
                    connectionInstance.query("CALL IF EXISTS delete_files_in_directory('browser-data/tmp')").catch(() => {
                        // Silent catch - beforeunload handlers need to be fast
                    });
                } catch (err) {
                    // Suppress errors during page unload
                }
            }
        };
        
        window.addEventListener('beforeunload', handleBeforeUnload);
        return () => {
            window.removeEventListener('beforeunload', handleBeforeUnload);
        };
    }, []);

    // Function to get or create a connection
    const createConnection = async (): Promise<AsyncDuckDBConnection | null> => {
        if (!duckDBInstance) return null;

        try {
            // Create a new connection if needed
            if (!connectionInstance) {
                console.log('DEBUG: Creating new DuckDB connection');
                connectionInstance = await duckDBInstance.connect();

                // Apply all necessary settings
                await connectionInstance.query("LOAD icu");
                await connectionInstance.query("SET TimeZone='America/New_York'");

                // Add temporary directory for disk offloading
                await connectionInstance.query("SET temp_directory='browser-data/tmp'");

                // Additional settings for stability
                await connectionInstance.query("PRAGMA threads=4");
                await connectionInstance.query("PRAGMA memory_limit='2GB'");

                console.log('DEBUG: DuckDB connection initialized with settings');
            }

            return connectionInstance;
        } catch (err) {
            console.error('Error creating connection:', err);
            return null;
        }
    };

    return (
        <DuckDBContext.Provider
            value={{
                db: duckDBInstance,
                loading,
                error,
                dataloading,
                setDataLoading,
                histogramData,
                setHistogramData,
                crossFilter,
                setCrossFilter,
                connection: connectionInstance,
                createConnection
            }}
        >
            {children}
        </DuckDBContext.Provider>
    );
};
