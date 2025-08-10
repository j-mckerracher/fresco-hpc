export interface QueryParameters {
    startDate: string;
    endDate: string;
    sqlQuery: string;
    rowCount?: number;
}

/**
 * Saves query parameters to localStorage for use between pages
 * during the migration to SPA architecture
 */
export function saveQueryParameters(params: QueryParameters): void {
    localStorage.setItem("fresco_query_params", JSON.stringify(params));
}

/**
 * Retrieves query parameters from localStorage
 */
export function getQueryParameters(): QueryParameters | null {
    const storedParams = localStorage.getItem("fresco_query_params");
    if (!storedParams) return null;

    try {
        return JSON.parse(storedParams) as QueryParameters;
    } catch (error) {
        console.error("Error parsing stored query parameters:", error);
        return null;
    }
}