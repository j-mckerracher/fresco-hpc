import { useEffect, useRef, useState } from 'react';
import * as vg from "@uwdata/vgplot";
import { useRouter } from 'next/router'; // Direct use of Next.js router
import { useDuckDB } from "@/context/DuckDBContext";

interface HistogramProps {
    readyToPlot: boolean;
}

interface BrushValue {
    value?: [Date, Date];
}

const Histogram: React.FC<HistogramProps> = ({ readyToPlot }) => {
    const { db, loading, setHistogramData } = useDuckDB();
    const plotRef = useRef<HTMLDivElement>(null);
    const [dataLoaded, setDataLoaded] = useState(false);
    const [brush, setBrush] = useState<BrushValue | null>(null);
    const [error, setError] = useState<string | null>(null);
    const connRef = useRef<any>(null);
    const router = useRouter(); // Use Next.js router directly

    const formatDate = (date: Date): string => {
        return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    };

    useEffect(() => {
        const createPlot = async () => {
            if (!db || loading || !readyToPlot || !plotRef.current) {
                console.log('Waiting for requirements:', {
                    db: !!db,
                    loading,
                    readyToPlot,
                    plotRef: !!plotRef.current
                });
                return;
            }

            try {
                // Create and store connection
                console.log('DEBUG: Creating plot connection...');
                connRef.current = await db.connect();

                // Set up ICU and timezone
                await connRef.current.query("LOAD icu");
                await connRef.current.query("SET TimeZone='America/New_York'");

                // First verify the table has data
                const tableCheck = await connRef.current.query(`
                    SELECT COUNT(*) as count FROM histogram
                `);
                const tableCount = tableCheck.toArray()[0].count;
                console.log('DEBUG: Histogram table count:', tableCount);

                if (tableCount === 0) {
                    throw new Error('No data available in histogram table');
                }

                // Create a time range check query that handles potential NULL values
                const rangeQuery = await connRef.current.query(`
                    SELECT 
                        MIN(time) as min_time,
                        MAX(time) as max_time,
                        COUNT(*) as count
                    FROM histogram_view
                    WHERE time IS NOT NULL
                `);

                const rangeResult = rangeQuery.toArray();
                console.log('DEBUG: Range query result:', rangeResult);

                if (rangeResult.length === 0 || rangeResult[0].count === 0) {
                    throw new Error('No data with valid time values found in histogram_view');
                }

                const range = rangeResult[0];
                console.log('DEBUG: Data range details:', {
                    min_time: range.min_time,
                    max_time: range.max_time,
                    count: range.count
                });

                // Generate fallback time range if needed
                let minTime, maxTime;

                if (!range.min_time || !range.max_time) {
                    console.log('DEBUG: Missing time range values, creating fallback');

                    // Try to get at least one valid time value from the table
                    const sampleQuery = await connRef.current.query(`
                        SELECT time FROM histogram WHERE time IS NOT NULL LIMIT 1
                    `);

                    const sampleResults = sampleQuery.toArray();
                    let baseTime;

                    if (sampleResults.length > 0 && sampleResults[0].time) {
                        baseTime = new Date(sampleResults[0].time);
                        console.log('DEBUG: Using sample time as base:', baseTime);
                    } else {
                        baseTime = new Date();
                        console.log('DEBUG: Using current time as base:', baseTime);
                    }

                    // Create a 24-hour range around the base time
                    const oneDay = 24 * 60 * 60 * 1000; // ms in a day
                    minTime = new Date(baseTime.getTime() - (oneDay / 2));
                    maxTime = new Date(baseTime.getTime() + (oneDay / 2));
                } else {
                    minTime = new Date(range.min_time);
                    maxTime = new Date(range.max_time);

                    // If dates are the same, create a small range
                    if (minTime.getTime() === maxTime.getTime()) {
                        const oneHour = 60 * 60 * 1000;
                        minTime = new Date(minTime.getTime() - oneHour);
                        maxTime = new Date(maxTime.getTime() + oneHour);
                    }
                }

                console.log('DEBUG: Final time range for plot:', {
                    min: minTime,
                    max: maxTime
                });

                // Set up coordinator
                console.log('DEBUG: Setting up vgplot coordinator...');
                const coordinator = vg.coordinator();
                coordinator.databaseConnector(
                    vg.wasmConnector({
                        duckdb: db,
                        connection: connRef.current,
                    })
                );

                // Create brush selection
                const brushSelection = vg.Selection.intersect();
                setBrush(brushSelection as BrushValue);

                // Create plot using the view with proper date formatting
                console.log('DEBUG: Creating plot element...');

                // @ts-ignore
                const plotElement = vg.plot(
                    vg.rectY(
                        vg.from("histogram_view"),
                        {
                            x: vg.bin("time", {
                                maxbins: 50,
                                extent: [minTime, maxTime]
                            }),
                            y: vg.count(),
                            inset: 0.5,
                            fill: "#CFB991",
                        }
                    ),
                    vg.intervalX({ as: brushSelection }),
                    vg.xScale('time'),
                    vg.xLabel('Time'),
                    vg.yLabel('Count'),
                    vg.width(Math.min(window.innerWidth * 0.8, 1200)),
                    vg.height(400),
                    vg.style({
                        backgroundColor: "transparent",
                        color: "#FFFFFF",
                        fontSize: "14px",
                        fontFamily: "system-ui",
                        ".vgplot-x-axis line, .vgplot-y-axis line": {
                            stroke: "#FFFFFF",
                        },
                        ".vgplot-x-axis text, .vgplot-y-axis text": {
                            fill: "#FFFFFF",
                        }
                    })
                ) as HTMLElement;

                // Mount plot
                console.log('DEBUG: Mounting plot...');
                plotRef.current.innerHTML = '';
                plotRef.current.appendChild(plotElement);
                setDataLoaded(true);
                console.log('DEBUG: Plot mounted successfully');
            } catch (error) {
                console.error('DEBUG: Error creating plot:', error);
                setError(error instanceof Error ? error.message : 'An unknown error occurred');
            }
        };

        createPlot();

        // Cleanup connection on unmount
        return () => {
            if (connRef.current) {
                connRef.current.close();
            }
        };
    }, [db, loading, readyToPlot]);

    const handleQueryDataset = () => {
        if (brush?.value) {
            // Format the brush values for proper display
            const startTime = formatDate(brush.value[0]);
            const endTime = formatDate(brush.value[1]);

            console.log(`DEBUG: Selected time range: ${startTime} to ${endTime}`);

            const query = `SELECT * FROM job_data_small WHERE time BETWEEN '${
                brush.value[0].toISOString()
            }' AND '${brush.value[1].toISOString()}'`;

            // Store query in localStorage for compatibility with existing code
            window.localStorage.setItem("SQLQuery", query);

            // Use Next.js router directly to navigate
            console.log('DEBUG: Navigating to data_analysis page with Next.js router');
            router.push('/data_analysis');
        } else {
            alert("No selection made");
        }
    };

    if (error) {
        return (
            <div className="text-center p-4">
                <p className="text-white text-lg">Error: {error}</p>
                <button
                    onClick={() => router.push('/')}
                    className="mt-4 px-6 py-2 bg-[#CFB991] text-black rounded-md hover:bg-[#BFA881] transition-colors"
                >
                    Return to Home
                </button>
            </div>
        );
    }

    return (
        <div className="flex flex-col items-center justify-center w-full">
            <h1 className="text-xl font-medium mb-14 text-white">
                {dataLoaded ? 'Drag across the histogram to select a slice of the dataset' : 'Loading histogram data...'}
            </h1>
            <div className="min-h-[60vh] w-full" ref={plotRef} />
            {dataLoaded && (
                <button
                    onClick={handleQueryDataset}
                    className="mt-8 px-6 py-2 bg-[#CFB991] text-black rounded-md hover:bg-[#BFA881] transition-colors"
                >
                    Query dataset
                </button>
            )}
        </div>
    );
};

export default Histogram;