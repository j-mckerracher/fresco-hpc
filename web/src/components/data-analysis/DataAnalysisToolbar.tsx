/**
 * Toolbar component for data analysis page
 * 
 * Contains download functionality and navigation controls
 */

import React, { useMemo } from 'react';
import Link from 'next/link';
import Header from '@/components/Header';

interface DataAnalysisToolbarProps {
    /** Whether download is currently in progress */
    downloading: boolean;
    /** Callback to handle CSV download */
    onDownload: () => void;
}

/**
 * Top toolbar with download and navigation controls
 */
export const DataAnalysisToolbar: React.FC<DataAnalysisToolbarProps> = React.memo(({
    downloading,
    onDownload
}) => {
    // Memoize button styling to prevent unnecessary recalculations
    const buttonClassName = useMemo(() => {
        return `px-4 py-2 rounded-md transition-colors mr-4 ${
            downloading
                ? 'bg-gray-500 cursor-not-allowed'
                : 'bg-purdue-boilermakerGold text-black hover:bg-purdue-rush'
        }`;
    }, [downloading]);

    const buttonText = useMemo(() => {
        return downloading ? 'Downloading...' : 'Download Data as CSV';
    }, [downloading]);

    return (
        <>
            <Header />
            <div className="flex justify-end p-4">
                <button
                    onClick={onDownload}
                    disabled={downloading}
                    className={buttonClassName}
                >
                    {buttonText}
                </button>
                <Link href="/query_builder">
                    <div className="px-4 py-2 bg-zinc-800 text-white rounded-md hover:bg-zinc-700 transition-colors">
                        Back to Query Builder
                    </div>
                </Link>
            </div>
        </>
    );
});

DataAnalysisToolbar.displayName = 'DataAnalysisToolbar';

export default DataAnalysisToolbar;