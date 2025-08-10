import { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

/**
 * Exports data from a DuckDB table to a CSV file
 * @param conn DuckDB connection
 * @param tableName Name of the table to export
 * @param fileName Name of the file to download (without extension)
 * @param filters Optional SQL WHERE clause filters
 */
export async function exportDataAsCSV(
    conn: AsyncDuckDBConnection,
    tableName: string,
    fileName: string,
    filters?: string
): Promise<void> {
    try {
        // Build the query, adding filters if provided
        let query = `SELECT * FROM ${tableName}`;
        if (filters && filters.trim()) {
            query += ` WHERE ${filters}`;
        }

        console.log(`Executing export query: ${query}`);

        // Execute the query
        const result = await conn.query(query);

        // Convert to CSV
        const rows = result.toArray();
        if (rows.length === 0) {
            throw new Error("No data to export");
        }

        // Get column headers
        const headers = Object.keys(rows[0]);

        // Create CSV content
        let csvContent = headers.join(",") + "\n";
        rows.forEach(row => {
            const values = headers.map(header => {
                const value = row[header];
                // Handle string values with commas by quoting them
                if (typeof value === 'string' && value.includes(',')) {
                    return `"${value}"`;
                }
                // Format date values
                if (value instanceof Date) {
                    return value.toISOString();
                }
                return value === null ? '' : value;
            });
            csvContent += values.join(",") + "\n";
        });

        // Create blob and download
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.setAttribute('href', url);
        link.setAttribute('download', `${fileName}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        console.log(`Exported ${rows.length} rows to ${fileName}.csv`);
    } catch (error) {
        console.error("Error exporting data:", error);
        alert("Failed to export data: " + (error instanceof Error ? error.message : "Unknown error"));
    }
}