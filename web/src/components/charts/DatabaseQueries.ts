/**
 * Database query functions for chart data processing
 * 
 * Handles all database operations for chart data preparation,
 * validation, and view creation.
 */

import { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { ErrorHandler, ErrorType } from '@/utils/errorHandler';

/**
 * Data statistics interface
 */
export interface DataStats {
    min_val: number;
    max_val: number;
    count: number;
    null_count?: number;
}

/**
 * Validate that table and required columns exist
 */
export async function validateTableAndColumn(
    conn: AsyncDuckDBConnection,
    tableName: string,
    columnName: string,
    xAxis?: string
): Promise<void> {
    try {
        // Check if table exists
        const tableCheck = await conn.query(`
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='${tableName}'
        `);

        if (tableCheck.toArray().length === 0) {
            throw new Error(`Table "${tableName}" not found`);
        }

        // Get available columns
        const columnCheck = await conn.query(`SELECT * FROM ${tableName} LIMIT 1`);
        const columns = columnCheck.schema.fields.map((f: { name: string }) => f.name);

        // Check main column
        if (!columns.includes(columnName)) {
            const matchingColumn = columns.find((c: string) =>
                c.toLowerCase() === columnName.toLowerCase()
            );

            if (!matchingColumn) {
                throw new Error(`Column "${columnName}" not found in table`);
            }
        }

        // Check x-axis column for line plots
        if (xAxis && !columns.includes(xAxis)) {
            throw new Error(`X-axis column "${xAxis}" not found in table`);
        }
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.validateTableAndColumn', ErrorType.Database);
    }
}

/**
 * Get statistical information about a column
 */
export async function getDataStats(
    conn: AsyncDuckDBConnection,
    tableName: string,
    columnName: string
): Promise<DataStats> {
    try {
        const result = await conn.query(`
            SELECT 
                MIN(${columnName}) as min_val,
                MAX(${columnName}) as max_val,
                COUNT(*) as count,
                COUNT(CASE WHEN ${columnName} IS NULL THEN 1 END) as null_count
            FROM ${tableName}
            WHERE ${columnName} IS NOT NULL
        `);

        return result.toArray()[0];
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.getDataStats', ErrorType.Database);
    }
}

/**
 * Check data availability for given columns
 */
export async function checkDataAvailability(
    conn: AsyncDuckDBConnection,
    tableName: string,
    columnName: string,
    xAxis?: string
): Promise<number> {
    try {
        const whereClause = xAxis
            ? `${columnName} IS NOT NULL AND ${columnName} != 0 AND ${xAxis} IS NOT NULL`
            : `${columnName} IS NOT NULL`;

        const result = await conn.query(`
            SELECT COUNT(*) as count 
            FROM ${tableName}
            WHERE ${whereClause}
        `);

        return result.toArray()[0].count;
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.checkDataAvailability', ErrorType.Database);
    }
}

/**
 * Create standard aggregated view for time series data
 */
export async function createStandardAggregatedView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string,
    xAxis: string
): Promise<void> {
    try {
        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            SELECT 
                date_trunc('hour', ${xAxis}) as hour,
                AVG(${columnName}) as avg_value,
                MIN(${columnName}) as min_value,
                MAX(${columnName}) as max_value,
                COUNT(*) as count
            FROM ${tableName}
            WHERE ${columnName} IS NOT NULL AND ${xAxis} IS NOT NULL
            GROUP BY date_trunc('hour', ${xAxis})
            ORDER BY hour
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createStandardAggregatedView', ErrorType.Database);
    }
}

/**
 * Create percentile-based view for outlier-resistant visualization
 */
export async function createPercentileBasedView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string,
    xAxis: string,
    percentileLow: number,
    percentileHigh: number
): Promise<void> {
    try {
        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            WITH percentiles AS (
                SELECT
                    PERCENTILE_CONT(${percentileLow}) WITHIN GROUP (ORDER BY ${columnName}) AS p_low,
                    PERCENTILE_CONT(${percentileHigh}) WITHIN GROUP (ORDER BY ${columnName}) AS p_high
                FROM ${tableName}
                WHERE ${columnName} IS NOT NULL AND ${xAxis} IS NOT NULL
            ),
            robust_data AS (
                SELECT 
                    t.${xAxis},
                    t.${columnName}
                FROM ${tableName} t, percentiles p
                WHERE 
                    t.${columnName} IS NOT NULL AND 
                    t.${xAxis} IS NOT NULL AND
                    t.${columnName} BETWEEN p.p_low AND p.p_high
            )
            SELECT 
                date_trunc('hour', ${xAxis}) as hour,
                AVG(${columnName}) as avg_value,
                MIN(${columnName}) as min_value,
                MAX(${columnName}) as max_value,
                COUNT(*) as count
            FROM robust_data
            GROUP BY date_trunc('hour', ${xAxis})
            ORDER BY hour
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createPercentileBasedView', ErrorType.Database);
    }
}

/**
 * Create BigInt conversion view for numerical histograms
 */
export async function createBigIntView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string
): Promise<void> {
    try {
        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            SELECT 
                *,
                CAST(${columnName} AS DOUBLE) as ${columnName}_double
            FROM ${tableName}
            WHERE ${columnName} IS NOT NULL
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createBigIntView', ErrorType.Database);
    }
}

/**
 * Create scaled view for small values
 */
export async function createScaledView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string,
    scaleFactor: number = 1000000
): Promise<void> {
    try {
        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            SELECT 
                *,
                ${columnName} * ${scaleFactor} as ${columnName}_scaled
            FROM ${tableName}
            WHERE ${columnName} IS NOT NULL
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createScaledView', ErrorType.Database);
    }
}

/**
 * Create top N categories view for categorical histograms
 */
export async function createTopNCategoriesView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string,
    topN: number = 10
): Promise<void> {
    try {
        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            WITH category_counts AS (
                SELECT 
                    ${columnName},
                    COUNT(*) as count
                FROM ${tableName}
                WHERE ${columnName} IS NOT NULL
                GROUP BY ${columnName}
                ORDER BY count DESC
            ),
            ranked AS (
                SELECT
                    ${columnName},
                    count,
                    ROW_NUMBER() OVER (ORDER BY count DESC) as rank
                FROM category_counts
            )
            SELECT
                CASE
                    WHEN rank <= ${topN} THEN ${columnName}
                    ELSE 'Others'
                END as category,
                SUM(count) as count
            FROM ranked
            GROUP BY 
                CASE
                    WHEN rank <= ${topN} THEN ${columnName}
                    ELSE 'Others'
                END
            ORDER BY 
                CASE WHEN category = 'Others' THEN 1 ELSE 0 END,
                count DESC
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createTopNCategoriesView', ErrorType.Database);
    }
}

/**
 * Create histogram view with specified number of bins
 */
export async function createHistogramView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string,
    bins: number,
    percentileOptions?: { percentileLow: number; percentileHigh: number }
): Promise<void> {
    try {
        let sourceQuery = tableName;
        const sourceColumn = columnName;

        // If percentile filtering is requested, create intermediate view
        if (percentileOptions) {
            const tempViewName = `${viewName}_filtered`;
            await conn.query(`
                CREATE TEMPORARY VIEW ${tempViewName} AS
                WITH percentiles AS (
                    SELECT
                        PERCENTILE_CONT(${percentileOptions.percentileLow}) WITHIN GROUP (ORDER BY ${columnName}) AS p_low,
                        PERCENTILE_CONT(${percentileOptions.percentileHigh}) WITHIN GROUP (ORDER BY ${columnName}) AS p_high
                    FROM ${tableName}
                    WHERE ${columnName} IS NOT NULL
                )
                SELECT t.${columnName}
                FROM ${tableName} t, percentiles p
                WHERE 
                    t.${columnName} IS NOT NULL AND 
                    t.${columnName} BETWEEN p.p_low AND p.p_high
            `);
            sourceQuery = tempViewName;
        }

        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            WITH stats AS (
                SELECT
                    MIN(${sourceColumn}) as min_val,
                    MAX(${sourceColumn}) as max_val
                FROM ${sourceQuery}
                WHERE ${sourceColumn} IS NOT NULL
            ),
            bins AS (
                SELECT
                    generate_series(0, ${bins - 1}) as bin_index,
                    s.min_val + (s.max_val - s.min_val) * generate_series(0, ${bins - 1}) / ${bins} as bin_start,
                    s.min_val + (s.max_val - s.min_val) * generate_series(1, ${bins}) / ${bins} as bin_end
                FROM stats s
            )
            SELECT
                b.bin_start,
                b.bin_end,
                COUNT(t.${sourceColumn}) as count
            FROM bins b
            LEFT JOIN ${sourceQuery} t ON t.${sourceColumn} >= b.bin_start AND t.${sourceColumn} < b.bin_end
            WHERE t.${sourceColumn} IS NOT NULL OR b.bin_index = ${bins - 1}
            GROUP BY b.bin_index, b.bin_start, b.bin_end
            ORDER BY b.bin_start
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createHistogramView', ErrorType.Database);
    }
}

/**
 * Create categorical view with category counts
 */
export async function createCategoricalView(
    conn: AsyncDuckDBConnection,
    viewName: string,
    tableName: string,
    columnName: string,
    maxCategories: number = 20
): Promise<void> {
    try {
        await conn.query(`
            CREATE TEMPORARY VIEW ${viewName} AS
            WITH category_counts AS (
                SELECT 
                    ${columnName} as category,
                    COUNT(*) as count
                FROM ${tableName}
                WHERE ${columnName} IS NOT NULL
                GROUP BY ${columnName}
                ORDER BY count DESC
            ),
            ranked AS (
                SELECT
                    category,
                    count,
                    ROW_NUMBER() OVER (ORDER BY count DESC) as rank
                FROM category_counts
            )
            SELECT
                CASE
                    WHEN rank <= ${maxCategories} THEN category
                    ELSE 'Others'
                END as category,
                SUM(count) as count
            FROM ranked
            GROUP BY 
                CASE
                    WHEN rank <= ${maxCategories} THEN category
                    ELSE 'Others'
                END
            ORDER BY 
                CASE WHEN category = 'Others' THEN 1 ELSE 0 END,
                count DESC
        `);
    } catch (error) {
        throw ErrorHandler.handle(error, 'DatabaseQueries.createCategoricalView', ErrorType.Database);
    }
}

/**
 * Drop temporary view if it exists
 */
export async function dropViewIfExists(
    conn: AsyncDuckDBConnection,
    viewName: string
): Promise<void> {
    try {
        await conn.query(`DROP VIEW IF EXISTS ${viewName}`);
    } catch (error) {
        // Ignore errors when dropping views that don't exist
        console.warn(`Warning when dropping view ${viewName}:`, error);
    }
}