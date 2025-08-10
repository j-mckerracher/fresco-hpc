"use client";
import * as vg from "@uwdata/vgplot";
import React, {useCallback, useEffect, useRef, useState} from "react";
import {VgPlotProps} from "@/components/component_types";
import {BOIILERMAKER_GOLD, PlotType} from "./component_types";

// ============= Constants & Configuration =============

export const column_pretty_names = new Map([
    ["time", "Time"],
    ["submit_time", "Submit Time"],
    ["start_time", "Start Time"],
    ["end_time", "End Time"],
    ["timelimit", "Time Limit"],
    ["nhosts", "Number of Hosts"],
    ["ncores", "Number of Cores"],
    ["account", "Account"],
    ["queue", "Queue"],
    ["host", "Host"],
    ["jid", "Job ID"],
    ["unit", "Unit"],
    ["jobname", "Job Name"],
    ["exitcode", "Exit Code"],
    ["host_list", "Host List"],
    ["username", "Username"],
    ["value_cpuuser", "CPU Usage"],
    ["value_gpu", "GPU Usage"],
    ["value_memused", "Memory Used"],
    ["value_memused_minus_diskcache", "Memory Used Minus Disk Cache"],
    ["value_nfs", "NFS Usage"],
    ["value_block", "Block Usage"],
]);

export const column_units = new Map([
    ["value_cpuuser", "CPU %"],
    ["value_gpu", "GPU %"],
    ["value_memused", "GB"],
    ["value_memused_minus_diskcache", "GB"],
    ["value_nfs", "MB/s"],
    ["value_block", "GB/s"],
]);

const BIGINT_COLUMNS = ["nhosts", "ncores"];
const MAX_RETRY_ATTEMPTS = 5;
const RETRY_DELAY_MS = 300;

// Column-specific configuration for special handling
const COLUMN_CONFIGS = {
    value_cpuuser: {
        usePercentiles: true,
        percentileLow: 0.01,
        percentileHigh: 0.99,
        thresholdValue: 1000,
        labelSuffix: " (excluding outliers)"
    },
    value_nfs: {
        usePercentiles: true,
        percentileLow: 0.02,
        percentileHigh: 0.98,
        thresholdValue: 10,
        labelSuffix: " (excluding outliers)"
    },
    value_block: {
        enhancedVisualization: true
    }
};

// ============= Utility Functions =============

const getPlotTitle = (plotType: PlotType, columnName: string, xAxis: string = ""): string => {
    const prettyColumn = column_pretty_names.get(columnName) || columnName;

    switch (plotType) {
        case PlotType.CategoricalHistogram:
            return `Frequency of ${prettyColumn}`;
        case PlotType.LinePlot:
            const prettyXAxis = column_pretty_names.get(xAxis) || xAxis;
            return `${prettyColumn} over ${prettyXAxis}`;
        case PlotType.NumericalHistogram:
            return `${prettyColumn} Distribution`;
        default:
            return prettyColumn;
    }
};

const generateViewName = (tableName: string, columnName: string, suffix: string = ""): string => {
    const uniqueId = `${Date.now()}_${Math.floor(Math.random() * 10000)}`;
    const cleanColumn = columnName.replace(/[^a-zA-Z0-9]/g, '_');
    return `${tableName}_${suffix}${cleanColumn}_${uniqueId}`;
};

const needsSpecialScaling = (columnName: string, min: number, max: number): boolean => {
    if (columnName === 'value_block') return true;
    if (Math.abs(max) < 0.01 && Math.abs(min) < 0.01) return true;
    return false;
};

const getYAxisLabel = (columnName: string, suffix: string = ""): string => {
    const prettyName = column_pretty_names.get(columnName) || columnName;
    const unit = column_units.get(columnName);
    const unitStr = unit ? ` (${unit})` : '';
    return `${prettyName}${suffix}${unitStr}`;
};

// ============= Plot Style Configuration =============

const getBaseStyle = () => ({
    color: "#FFFFFF",
    backgroundColor: "transparent",
    fontSize: "14px",
    ".vgplot-x-axis line, .vgplot-y-axis line": {
        stroke: "#FFFFFF",
    },
    ".vgplot-x-axis text, .vgplot-y-axis text": {
        fill: "#FFFFFF",
    }
});

const getLinePlotStyle = () => ({
    ...getBaseStyle(),
    ".vgplot-marks path": {
        strokeWidth: "3px"
    },
    ".vgplot-marks circle": {
        r: "5px"
    },
    ".vgplot-marks": {
        opacity: 1,
        pointerEvents: "all"
    }
});

const getHistogramStyle = () => ({
    ...getBaseStyle(),
    "font-size": "0.8rem"
});

// ============= Database Query Helpers =============

interface DataStats {
    min_val: number;
    max_val: number;
    count: number;
    null_count?: number;
}

async function validateTableAndColumn(
    conn: any,
    tableName: string,
    columnName: string,
    xAxis?: string
): Promise<void> {
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
    const columns = columnCheck.schema.fields.map((f: any) => f.name);

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
}

async function getDataStats(
    conn: any,
    tableName: string,
    columnName: string
): Promise<DataStats> {
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
}

async function checkDataAvailability(
    conn: any,
    tableName: string,
    columnName: string,
    xAxis?: string
): Promise<number> {
    const whereClause = xAxis
        ? `${columnName} IS NOT NULL AND ${columnName} != 0 AND ${xAxis} IS NOT NULL`
        : `${columnName} IS NOT NULL`;

    const result = await conn.query(`
        SELECT COUNT(*) as count 
        FROM ${tableName}
        WHERE ${whereClause}
    `);

    return result.toArray()[0].count;
}

// ============= View Creation Functions =============

async function createStandardAggregatedView(
    conn: any,
    viewName: string,
    tableName: string,
    columnName: string,
    xAxis: string
): Promise<void> {
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
}

async function createPercentileBasedView(
    conn: any,
    viewName: string,
    tableName: string,
    columnName: string,
    xAxis: string,
    percentileLow: number,
    percentileHigh: number
): Promise<void> {
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
}

// ============= Plot Creation Functions =============

function createLinePlotElements(viewName: string, includeRange: boolean = true) {
    const elements = [
        vg.lineY(vg.from(viewName), {
            x: "hour",
            y: "avg_value",
            stroke: BOIILERMAKER_GOLD,
            strokeWidth: 3,
        })
    ];

    if (includeRange) {
        elements.push(
            vg.areaY(vg.from(viewName), {
                x: "hour",
                y1: "min_value",
                y2: "max_value",
                fillOpacity: 0.2,
                fill: BOIILERMAKER_GOLD
            })
        );
    }

    elements.push(
        vg.dotY(vg.from(viewName), {
            x: "hour",
            y: "avg_value",
            fill: BOIILERMAKER_GOLD,
            stroke: "#000000",
            strokeWidth: 1,
            r: 5
        })
    );

    return elements;
}

function createPlotConfiguration(
    crossFilter: any,
    windowWidth: number,
    width: number,
    yMin?: number,
    yMax?: number
) {
    const config = [
        vg.panZoomX(crossFilter),
        vg.marginLeft(75),
        vg.marginBottom(50),
        vg.marginTop(30),
        vg.marginRight(30),
        vg.width(Math.min(windowWidth * width, 800)),
        vg.height(400),
        vg.xScale('time'),
        vg.yScale('linear'),
        vg.xLabel("Time")
    ];

    if (yMin !== undefined && yMax !== undefined) {
        const yRange = yMax - yMin;
        const yBuffer = Math.max(yRange * 0.1, Math.abs(yMax) * 0.1 || 1) * 0.1;
        config.push(vg.yDomain([yMin - yBuffer, yMax + yBuffer]));
    }

    return config;
}

// ============= Main Component =============

const VgPlot: React.FC<VgPlotProps> = ({
                                           db,
                                           conn,
                                           crossFilter,
                                           dbLoading,
                                           dataLoading,
                                           tableName,
                                           xAxis = "",
                                           columnName,
                                           plotType,
                                           width,
                                           height,
                                           topCategories,
                                       }) => {
    const [windowWidth, setWindowWidth] = useState(0);
    const [windowHeight, setWindowHeight] = useState(0);
    const [error, setError] = useState<string | null>(null);
    const [domReady, setDomReady] = useState(false);
    const [retryCount, setRetryCount] = useState(0);
    const [noData, setNoData] = useState(false);
    const plotsRef = useRef<HTMLDivElement | null>(null);

    const title = getPlotTitle(plotType, columnName, xAxis);

    // Handle window resizing
    useEffect(() => {
        const updateDimensions = () => {
            setWindowWidth(window.innerWidth);
            setWindowHeight(window.innerHeight);
        };

        updateDimensions();
        window.addEventListener("resize", updateDimensions);
        return () => window.removeEventListener("resize", updateDimensions);
    }, []);

    // Check DOM readiness
    useEffect(() => {
        if (plotsRef.current && !domReady) {
            setDomReady(true);
        }
    }, [plotsRef.current, domReady]);

    // Create line plot
    const createLinePlot = useCallback(async () => {
        const stats = await getDataStats(conn, tableName, columnName);
        const dataCount = await checkDataAvailability(conn, tableName, columnName, xAxis);

        if (dataCount === 0) {
            setNoData(true);
            if (plotsRef.current) {
                plotsRef.current.innerHTML = '';
                const emptyPlot = document.createElement('div');
                emptyPlot.className = 'flex items-center justify-center w-full h-full min-h-[300px]';
                emptyPlot.innerHTML = '<div class="text-white text-xl">No data available</div>';
                plotsRef.current.appendChild(emptyPlot);
            }
            return;
        }

        const config = COLUMN_CONFIGS[columnName as keyof typeof COLUMN_CONFIGS];
        let viewName: string;
        let plot: any;

        // Handle special column configurations
        if (config?.usePercentiles &&
            Math.abs(stats.min_val) > config.thresholdValue) {

            viewName = generateViewName(tableName, columnName, "robust_");
            await createPercentileBasedView(
                conn,
                viewName,
                tableName,
                columnName,
                xAxis,
                config.percentileLow,
                config.percentileHigh
            );

            const robustStats = await getDataStats(conn, viewName, "avg_value");

            plot = vg.plot(
                ...createLinePlotElements(viewName, true),
                ...createPlotConfiguration(
                    crossFilter,
                    windowWidth,
                    width,
                    robustStats.min_val,
                    robustStats.max_val
                ),
                vg.yLabel(getYAxisLabel(columnName, config.labelSuffix)),
                vg.style(getLinePlotStyle())
            );
        } else {
            // Standard aggregated view
            viewName = generateViewName(tableName, columnName, "agg_");
            await createStandardAggregatedView(conn, viewName, tableName, columnName, xAxis);

            plot = vg.plot(
                ...createLinePlotElements(viewName, config?.enhancedVisualization),
                ...createPlotConfiguration(
                    crossFilter,
                    windowWidth,
                    width,
                    stats.min_val,
                    stats.max_val
                ),
                vg.yLabel(getYAxisLabel(columnName)),
                vg.style(getLinePlotStyle())
            );
        }

        return plot;
    }, [conn, tableName, columnName, xAxis, crossFilter, windowWidth, width]);

    // Create numerical histogram
    const createNumericalHistogram = useCallback(async () => {
        const stats = await getDataStats(conn, tableName, columnName);
        let plot: any;

        // Handle BigInt columns
        if (BIGINT_COLUMNS.includes(columnName)) {
            const viewName = generateViewName(tableName, columnName, "bigint_");
            await conn.query(`
                CREATE TEMPORARY VIEW ${viewName} AS
                SELECT 
                    *,
                    CAST(${columnName} AS DOUBLE) as ${columnName}_double
                FROM ${tableName}
                WHERE ${columnName} IS NOT NULL
            `);

            plot = vg.plot(
                vg.rectY(vg.from(viewName, {filterBy: crossFilter}), {
                    x: vg.bin(`${columnName}_double`),
                    y: vg.count(),
                    inset: 1,
                    fill: BOIILERMAKER_GOLD,
                }),
                vg.marginLeft(60),
                vg.marginBottom(55),
                vg.intervalX({as: crossFilter}),
                vg.xDomain(vg.Fixed),
                vg.width(Math.min(windowWidth * width, 800)),
                vg.height(Math.min(windowHeight * height, 300)),
                vg.xLabel(column_pretty_names.get(columnName) || columnName),
                vg.style(getHistogramStyle())
            );
        }
        // Handle small values that need scaling
        else if (needsSpecialScaling(columnName, stats.min_val, stats.max_val)) {
            const viewName = generateViewName(tableName, columnName, "hist_");
            await conn.query(`
                CREATE TEMPORARY VIEW ${viewName} AS
                SELECT 
                    *,
                    ${columnName} * 1000000 as ${columnName}_scaled
                FROM ${tableName}
                WHERE ${columnName} IS NOT NULL
            `);

            plot = vg.plot(
                vg.rectY(vg.from(viewName, {filterBy: crossFilter}), {
                    x: vg.bin(`${columnName}_scaled`),
                    y: vg.count(),
                    inset: 1,
                    fill: BOIILERMAKER_GOLD,
                }),
                vg.marginLeft(60),
                vg.marginBottom(55),
                vg.intervalX({as: crossFilter}),
                vg.xDomain(vg.Fixed),
                vg.width(Math.min(windowWidth * width, 800)),
                vg.height(Math.min(windowHeight * height, 300)),
                vg.xLabel(`${column_pretty_names.get(columnName) || columnName} (×10⁻⁶)`),
                vg.style(getHistogramStyle())
            );
        }
        // Regular histogram
        else {
            plot = vg.plot(
                vg.rectY(vg.from(tableName, {filterBy: crossFilter}), {
                    x: vg.bin(columnName),
                    y: vg.count(),
                    inset: 1,
                    fill: BOIILERMAKER_GOLD,
                }),
                vg.marginLeft(60),
                vg.marginBottom(55),
                vg.intervalX({as: crossFilter}),
                vg.xDomain(vg.Fixed),
                vg.width(Math.min(windowWidth * width, 800)),
                vg.height(Math.min(windowHeight * height, 300)),
                vg.style(getHistogramStyle())
            );
        }

        return plot;
    }, [conn, tableName, columnName, crossFilter, windowWidth, windowHeight, width, height]);

    async function createTopNCategoriesView(
        conn: any,
        tableName: string,
        columnName: string,
        topN: number = 10
    ): Promise<string> {
        const viewName = generateViewName(tableName, columnName, "topn_");

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

            return viewName;
        } catch (err) {
            console.error("Error creating top N categories view:", err);
            throw err;
        }
    }


    // Create categorical histogram
    const createCategoricalHistogram = useCallback(async () => {
        const highlight = vg.Selection.intersect();

        const availableWidth = Math.min(windowWidth * width, 800);
        const pixelsPerCategory = 100;
        const maxCategories = Math.max(5, Math.floor(availableWidth / pixelsPerCategory));
        const topN = topCategories ? Math.min(topCategories, maxCategories) : maxCategories;

        try {
            const viewName = await createTopNCategoriesView(
                conn,
                tableName,
                columnName,
                topN
            );

            const plot = vg.plot(
                vg.rectY(vg.from(viewName), {
                    x: "category",
                    y: "count",
                    inset: 4,
                    fill: BOIILERMAKER_GOLD,
                    tooltip: true
                }),
                vg.marginLeft(60),
                vg.marginBottom(180),   
                vg.marginRight(30),
                vg.width(availableWidth),
                vg.height(Math.min(windowHeight * height, 300)),
                // Simplified xAxis configuration to avoid the anchor issue
                vg.xAxis("bottom", {
                    tickRotate: -45,
                    labelPadding: 15,
                    tickSpacing: 30
                }),
                vg.xLabel(column_pretty_names.get(columnName) || columnName),
                vg.yLabel("Count"),
                vg.style({
                    ...getBaseStyle(),
                    "svg g[aria-label='x-axis tick label'] text": {
                        transform: "rotate(-45deg) !important",
                        transformOrigin: "10px 10px !important",
                        textAnchor: "end !important",
                        fontSize: "0.75rem !important",
                        fontWeight: "normal !important",
                        letterSpacing: "0.01em !important"
                    }
                })
            );

            return plot;
        } catch (err) {
            console.error("Error creating categorical histogram:", err);
            throw err;
        }
    }, [conn, tableName, columnName, topCategories, windowWidth, windowHeight, width, height]);

    // Main setup function
// Main setup function
    const setupDb = useCallback(async () => {
        if (dbLoading || !db || dataLoading || !conn) {
            return;
        }

        if (!plotsRef.current) {
            if (retryCount < MAX_RETRY_ATTEMPTS) {
                setTimeout(() => {
                    setRetryCount(prev => prev + 1);
                }, RETRY_DELAY_MS);
                return;
            } else {
                setError(`Unable to render plot: container element not available after ${retryCount} attempts`);
                return;
            }
        }

        if (retryCount > 0) {
            setRetryCount(0);
        }

        try {
            // Validate table and columns
            await validateTableAndColumn(conn, tableName, columnName, xAxis);

            // Check data availability
            const dataCount = await checkDataAvailability(conn, tableName, columnName);
            if (dataCount === 0) {
                throw new Error(`No data available for column "${columnName}"`);
            }

            // Set up the coordinator
            try {
                vg.coordinator().databaseConnector(
                    vg.wasmConnector({duckdb: db, connection: conn})
                );
            } catch (err) {
                // Coordinator might already be set up
            }

            let plot: any;

            // Create appropriate plot type
            switch (plotType) {
                case PlotType.LinePlot:
                    plot = await createLinePlot();
                    break;
                case PlotType.NumericalHistogram:
                    plot = await createNumericalHistogram();
                    break;
                case PlotType.CategoricalHistogram:
                    plot = await createCategoricalHistogram();  // Added await here
                    break;
            }

            // Mount the plot
            if (plotsRef.current && plot) {
                plotsRef.current.innerHTML = '';
                plotsRef.current.appendChild(plot);
            }
        } catch (err) {
            console.error("Error in setupDb:", err);
            setError(`Failed to create visualization: ${err instanceof Error ? err.message : 'Unknown error'}`);
        }
    }, [
        dbLoading,
        db,
        dataLoading,
        conn,
        plotType,
        tableName,
        crossFilter,
        xAxis,
        columnName,
        width,
        height,
        retryCount,
        createLinePlot,
        createNumericalHistogram,
        createCategoricalHistogram
    ]);

    // Run setup when DOM is ready
    useEffect(() => {
        if (domReady) {
            setupDb();
        }
    }, [setupDb, domReady, retryCount]);

    // Error display
    if (error) {
        return (
            <div className="flex flex-col w-full text-white bg-zinc-900 p-4 rounded-lg min-h-40">
                <h1 className="text-center text-xl text-red-400">{title}</h1>
                <div className="flex items-center justify-center flex-1 p-4">
                    <p className="text-red-400">{error}</p>
                </div>
            </div>
        );
    }

    // Main render
    return (
        <div className="flex flex-col w-full text-white">
            <div
                className="overflow-visible w-full min-h-[400px] flex items-center justify-center"
                ref={plotsRef}
                style={{
                    minWidth: '100%',
                    position: 'relative',
                    zIndex: 1
                }}
            />
        </div>
    );
};

export default React.memo(VgPlot);