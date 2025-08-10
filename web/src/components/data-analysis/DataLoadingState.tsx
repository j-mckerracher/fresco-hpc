/**
 * Data loading state component for the data analysis page
 * 
 * Handles different loading states, errors, and retry functionality
 */

import React from 'react';
import dynamic from 'next/dynamic';
import { useNavigation } from '@/util/navigation';

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

interface DataLoadingStateProps {
    /** Whether data is currently loading */
    isLoading: boolean;
    /** Whether data is ready for visualization */
    dataReady: boolean;
    /** Error message if loading failed */
    loadError: string | null;
    /** Callback to retry loading with demo data */
    onRetry: () => void;
}

/**
 * Renders appropriate UI based on data loading state
 */
export const DataLoadingState: React.FC<DataLoadingStateProps> = React.memo(({
    isLoading,
    dataReady,
    loadError,
    onRetry
}) => {
    const { navigate } = useNavigation();

    // Show loading animation while data is being loaded
    if (isLoading || !dataReady) {
        console.log('Rendering loading state');
        return <LoadingAnimation />;
    }

    // Show error state with retry options
    if (loadError) {
        return (
            <div className="flex flex-col items-center justify-center p-8 text-white">
                <div className="bg-zinc-900 p-6 rounded-lg max-w-2xl text-center">
                    <h2 className="text-2xl text-red-500 mb-4">Error Loading Data</h2>
                    <p className="mb-6">{loadError}</p>
                    <div className="flex gap-4 justify-center">
                        <button
                            onClick={onRetry}
                            className="px-4 py-2 bg-purdue-boilermakerGold text-black rounded-md hover:bg-purdue-rush transition-colors"
                        >
                            Try Again with Demo Data
                        </button>
                        <button
                            onClick={() => navigate('/query_builder')}
                            className="px-4 py-2 bg-gray-700 text-white rounded-md hover:bg-gray-600 transition-colors"
                        >
                            Return to Query Builder
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    // If we reach here, data is ready and there's no error
    return null;
});

DataLoadingState.displayName = 'DataLoadingState';

export default DataLoadingState;