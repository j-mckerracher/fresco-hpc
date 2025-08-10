/**
 * Database optimization utilities for DuckDB performance
 * 
 * Provides query optimization, caching, and indexing strategies
 * specifically tailored for FRESCO's data analysis workloads.
 */

import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { ErrorHandler, ErrorType } from './errorHandler';

/**
 * Query cache for frequently accessed data
 */
const queryCache = new Map<string, { data: unknown; timestamp: number; ttl: number }>();

/**
 * Default cache TTL in milliseconds (5 minutes)
 */
const DEFAULT_CACHE_TTL = 5 * 60 * 1000;

/**
 * Generate cache key from query and parameters
 */
function generateCacheKey(query: string, params?: Record<string, unknown>): string {
    const paramString = params ? JSON.stringify(params) : '';
    return `${query}:${paramString}`;
}

/**
 * Check if cached result is still valid
 */
function isCacheValid(timestamp: number, ttl: number): boolean {
    return Date.now() - timestamp < ttl;
}

/**
 * Execute query with caching support
 */
export async function executeQueryWithCache(
    conn: AsyncDuckDBConnection,
    query: string,
    params?: Record<string, unknown>,
    cacheTtl: number = DEFAULT_CACHE_TTL
): Promise<unknown> {
    const cacheKey = generateCacheKey(query, params);
    const cached = queryCache.get(cacheKey);

    // Return cached result if valid
    if (cached && isCacheValid(cached.timestamp, cached.ttl)) {
        console.log('Returning cached result for query:', query.substring(0, 50) + '...');
        return cached.data;
    }

    try {
        // Execute query
        const result = await conn.query(query);
        const data = result.toArray();

        // Cache the result
        queryCache.set(cacheKey, {
            data,
            timestamp: Date.now(),
            ttl: cacheTtl
        });

        return data;
    } catch (error) {
        throw ErrorHandler.handle(error, 'executeQueryWithCache', ErrorType.Database);
    }
}

/**
 * Clear cache entries older than specified age
 */
export function clearOldCacheEntries(maxAge: number = DEFAULT_CACHE_TTL): void {
    for (const [key, value] of queryCache.entries()) {
        if (!isCacheValid(value.timestamp, maxAge)) {
            queryCache.delete(key);
        }
    }
}

/**
 * Clear all cache entries
 */
export function clearCache(): void {
    queryCache.clear();
}

/**
 * Get cache statistics
 */
export function getCacheStats(): { size: number; entries: number } {
    const entries = Array.from(queryCache.values());
    const size = JSON.stringify(entries).length;
    return { size, entries: queryCache.size };
}

/**
 * Optimize table for analytical queries
 */
export async function optimizeTableForAnalytics(
    conn: AsyncDuckDBConnection,
    tableName: string
): Promise<void> {
    try {
        console.log(`Optimizing table ${tableName} for analytics...`);

        // Create optimized columnar layout
        await conn.query(`
            PRAGMA table_info('${tableName}')
        `);

        // Update table statistics for better query planning
        await conn.query(`
            ANALYZE ${tableName}
        `);

        console.log(`Table ${tableName} optimization complete`);
    } catch (error) {
        throw ErrorHandler.handle(error, 'optimizeTableForAnalytics', ErrorType.Database);
    }
}

/**
 * Optimize specific queries for time-series data
 */
export class TimeSeriesQueryOptimizer {
    private conn: AsyncDuckDBConnection;

    constructor(conn: AsyncDuckDBConnection) {
        this.conn = conn;
    }

    /**
     * Create optimized aggregated view for time-series analysis
     */
    async createOptimizedTimeSeriesView(
        viewName: string,
        tableName: string,
        timeColumn: string,
        valueColumn: string,
        granularity: 'minute' | 'hour' | 'day' = 'hour'
    ): Promise<void> {
        try {
            const query = `
                CREATE OR REPLACE TEMPORARY VIEW ${viewName} AS
                WITH time_series AS (
                    SELECT 
                        date_trunc('${granularity}', ${timeColumn}) as time_bucket,
                        ${valueColumn},
                        COUNT(*) as point_count
                    FROM ${tableName}
                    WHERE ${timeColumn} IS NOT NULL 
                        AND ${valueColumn} IS NOT NULL
                    GROUP BY date_trunc('${granularity}', ${timeColumn}), ${valueColumn}
                ),
                aggregated AS (
                    SELECT 
                        time_bucket,
                        AVG(${valueColumn}) as avg_value,
                        MIN(${valueColumn}) as min_value,
                        MAX(${valueColumn}) as max_value,
                        STDDEV(${valueColumn}) as std_value,
                        SUM(point_count) as total_points,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ${valueColumn}) as q25,
                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ${valueColumn}) as median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ${valueColumn}) as q75
                    FROM time_series
                    GROUP BY time_bucket
                )
                SELECT * FROM aggregated
                ORDER BY time_bucket
            `;

            await executeQueryWithCache(this.conn, query, { viewName, tableName, timeColumn, valueColumn, granularity });
        } catch (error) {
            throw ErrorHandler.handle(error, 'TimeSeriesQueryOptimizer.createOptimizedTimeSeriesView', ErrorType.Database);
        }
    }

    /**
     * Create optimized histogram data with smart binning
     */
    async createOptimizedHistogramData(
        viewName: string,
        tableName: string,
        columnName: string,
        bins: number = 50,
        removeOutliers: boolean = true
    ): Promise<void> {
        try {
            let dataSource = tableName;
            const sourceColumn = columnName;

            // Optionally remove outliers using IQR method
            if (removeOutliers) {
                const outlierViewName = `${viewName}_no_outliers`;
                await this.conn.query(`
                    CREATE OR REPLACE TEMPORARY VIEW ${outlierViewName} AS
                    WITH stats AS (
                        SELECT
                            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ${columnName}) as q25,
                            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ${columnName}) as q75
                        FROM ${tableName}
                        WHERE ${columnName} IS NOT NULL
                    ),
                    bounds AS (
                        SELECT
                            q25 - 1.5 * (q75 - q25) as lower_bound,
                            q75 + 1.5 * (q75 - q25) as upper_bound
                        FROM stats
                    )
                    SELECT t.${columnName}
                    FROM ${tableName} t, bounds b
                    WHERE t.${columnName} BETWEEN b.lower_bound AND b.upper_bound
                        AND t.${columnName} IS NOT NULL
                `);
                dataSource = outlierViewName;
            }

            // Create smart histogram with equal-width bins
            const query = `
                CREATE OR REPLACE TEMPORARY VIEW ${viewName} AS
                WITH data_range AS (
                    SELECT
                        MIN(${sourceColumn}) as min_val,
                        MAX(${sourceColumn}) as max_val,
                        COUNT(*) as total_count
                    FROM ${dataSource}
                    WHERE ${sourceColumn} IS NOT NULL
                ),
                bin_width AS (
                    SELECT
                        min_val,
                        max_val,
                        total_count,
                        (max_val - min_val) / ${bins} as width
                    FROM data_range
                ),
                binned_data AS (
                    SELECT
                        ${sourceColumn},
                        CASE 
                            WHEN bw.width = 0 THEN 0
                            ELSE LEAST(${bins - 1}, 
                                FLOOR((${sourceColumn} - bw.min_val) / NULLIF(bw.width, 0))
                            )
                        END as bin_index,
                        bw.min_val,
                        bw.width
                    FROM ${dataSource} d, bin_width bw
                    WHERE d.${sourceColumn} IS NOT NULL
                )
                SELECT
                    bin_index,
                    min_val + bin_index * width as bin_start,
                    min_val + (bin_index + 1) * width as bin_end,
                    COUNT(*) as count,
                    AVG(${sourceColumn}) as avg_value
                FROM binned_data
                GROUP BY bin_index, min_val, width
                ORDER BY bin_index
            `;

            await executeQueryWithCache(this.conn, query, { viewName, tableName, columnName, bins, removeOutliers });
        } catch (error) {
            throw ErrorHandler.handle(error, 'TimeSeriesQueryOptimizer.createOptimizedHistogramData', ErrorType.Database);
        }
    }
}

/**
 * Memory management utilities for DuckDB
 */
export class DuckDBMemoryManager {
    private conn: AsyncDuckDBConnection;

    constructor(conn: AsyncDuckDBConnection) {
        this.conn = conn;
    }

    /**
     * Set optimal memory configuration for FRESCO workloads
     */
    async optimizeMemorySettings(): Promise<void> {
        try {
            // Set memory limit to 1.5GB to avoid browser crashes
            await this.conn.query("SET memory_limit='1.5GB'");
            
            // Set thread count to 4 for better performance on multi-core systems
            await this.conn.query("SET threads=4");
            
            // Enable aggressive garbage collection for temp objects
            await this.conn.query("SET temp_directory='/tmp/duckdb'");
            
            console.log('DuckDB memory settings optimized');
        } catch (error) {
            console.warn('Failed to optimize memory settings:', error);
        }
    }

    /**
     * Clean up temporary objects and views
     */
    async cleanupTempObjects(): Promise<void> {
        try {
            // Get list of temporary views
            const views = await executeQueryWithCache(
                this.conn,
                "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW' AND table_schema = 'temp'"
            );

            // Drop old temporary views
            if (Array.isArray(views)) {
                for (const view of views) {
                    const viewRecord = view as { table_name: string };
                    try {
                        await this.conn.query(`DROP VIEW IF EXISTS temp.${viewRecord.table_name}`);
                    } catch (err) {
                        // Ignore errors when dropping views
                        console.warn(`Failed to drop view ${viewRecord.table_name}:`, err);
                    }
                }
            }

            // Force garbage collection
            await this.conn.query("CHECKPOINT");
            
            console.log('Temporary objects cleaned up');
        } catch (error) {
            console.warn('Failed to cleanup temp objects:', error);
        }
    }

    /**
     * Get memory usage statistics
     */
    async getMemoryStats(): Promise<{ used: string; limit: string; available: string }> {
        try {
            const result = await executeQueryWithCache(
                this.conn,
                "SELECT current_setting('memory_limit') as limit"
            );
            
            return {
                used: 'Unknown',
                limit: Array.isArray(result) && result.length > 0 ? (result[0] as { limit: string }).limit : 'Unknown',
                available: 'Unknown'
            };
        } catch (error) {
            console.warn('Failed to get memory stats:', error);
            return { used: 'Unknown', limit: 'Unknown', available: 'Unknown' };
        }
    }
}

/**
 * Auto-cleanup interval for cache (runs every 10 minutes)
 */
setInterval(() => {
    clearOldCacheEntries();
}, 10 * 60 * 1000);