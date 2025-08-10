/**
 * Lazy-loaded components for improved performance
 * 
 * Implements code splitting and lazy loading for heavy components
 * to reduce initial bundle size and improve page load times.
 */

import dynamic from 'next/dynamic';
import React from 'react';

/**
 * Loading fallback component
 */
const LoadingFallback: React.FC<{ message?: string }> = ({ message = 'Loading...' }) => (
    <div className="flex items-center justify-center p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        <span className="ml-3 text-gray-600">{message}</span>
    </div>
);

/**
 * Error fallback component
 */
const ErrorFallback: React.FC<{ error?: Error; retry?: () => void }> = ({ error, retry }) => (
    <div className="p-4 border border-red-300 rounded-md bg-red-50">
        <h3 className="text-sm font-medium text-red-800 mb-2">
            Failed to load component
        </h3>
        <p className="text-red-700 text-sm mb-3">
            {error?.message || 'Component failed to load'}
        </p>
        {retry && (
            <button
                onClick={retry}
                className="px-3 py-1 bg-red-600 text-white text-sm rounded hover:bg-red-700"
            >
                Retry
            </button>
        )}
    </div>
);

/**
 * Lazy-loaded VgPlot component (largest component)
 */
export const LazyVgPlot = dynamic(() => import('@/components/vgplot'), {
    loading: () => <LoadingFallback message="Loading visualization..." />,
    ssr: false // VgPlot requires browser APIs
});

/**
 * Lazy-loaded refactored VgPlot component
 */
export const LazyVgPlotRefactored = dynamic(() => import('@/components/charts/VgPlotRefactored'), {
    loading: () => <LoadingFallback message="Loading chart..." />,
    ssr: false
});

/**
 * Lazy-loaded data analysis page
 */
export const LazyDataAnalysis = dynamic(() => import('@/pages/data_analysis'), {
    loading: () => <LoadingFallback message="Loading data analysis..." />,
    ssr: false
});

/**
 * Lazy-loaded refactored data analysis page
 */
export const LazyDataAnalysisRefactored = dynamic(() => import('@/pages/data_analysis_refactored'), {
    loading: () => <LoadingFallback message="Loading data analysis..." />,
    ssr: false
});

/**
 * Lazy-loaded query builder page
 */
export const LazyQueryBuilder = dynamic(() => import('@/pages/query_builder'), {
    loading: () => <LoadingFallback message="Loading query builder..." />,
    ssr: false
});

/**
 * Lazy-loaded multi-select component
 */
export const LazyMultiSelect = dynamic(() => import('@/components/multi-select'), {
    loading: () => <LoadingFallback message="Loading selector..." />,
    ssr: false
});

/**
 * Lazy-loaded VgMenu component
 */
export const LazyVgMenu = dynamic(() => import('@/components/vgmenu'), {
    loading: () => <LoadingFallback message="Loading menu..." />,
    ssr: false
});

/**
 * Lazy-loaded DuckDB context provider
 */
export const LazyDuckDBProvider = dynamic(() => 
    import('@/context/DuckDBContext').then(mod => ({ default: mod.DuckDBProvider })), {
    loading: () => <LoadingFallback message="Initializing database..." />,
    ssr: false
});

/**
 * Lazy-loaded chart components
 */
export const LazyLinePlotChart = dynamic(() => 
    import('@/components/charts/LinePlotChart').then(mod => ({ default: mod.LinePlotChart })), {
    loading: () => <LoadingFallback message="Loading line chart..." />,
    ssr: false
});

export const LazyNumericalHistogramChart = dynamic(() => 
    import('@/components/charts/NumericalHistogramChart').then(mod => ({ default: mod.NumericalHistogramChart })), {
    loading: () => <LoadingFallback message="Loading histogram..." />,
    ssr: false
});

export const LazyCategoricalHistogramChart = dynamic(() => 
    import('@/components/charts/CategoricalHistogramChart').then(mod => ({ default: mod.CategoricalHistogramChart })), {
    loading: () => <LoadingFallback message="Loading bar chart..." />,
    ssr: false
});

/**
 * Lazy-loaded data analysis components
 */
export const LazyDataLoadingState = dynamic(() => import('@/components/data-analysis/DataLoadingState'), {
    loading: () => <LoadingFallback message="Loading..." />
});

export const LazyDataAnalysisToolbar = dynamic(() => import('@/components/data-analysis/DataAnalysisToolbar'), {
    loading: () => <LoadingFallback message="Loading toolbar..." />
});

export const LazyColumnSelector = dynamic(() => import('@/components/data-analysis/ColumnSelector'), {
    loading: () => <LoadingFallback message="Loading column selector..." />
});

export const LazyVisualizationGrid = dynamic(() => import('@/components/data-analysis/VisualizationGrid'), {
    loading: () => <LoadingFallback message="Loading visualization grid..." />
});

/**
 * HOC for adding error boundary to lazy components
 */
export function withLazyErrorBoundary<P extends object>(
    LazyComponent: React.ComponentType<P>
): React.ComponentType<P> {
    const LazyComponentWithErrorBoundary = React.memo((props: P) => {
        const [error, setError] = React.useState<Error | null>(null);
        const [retryCount, setRetryCount] = React.useState(0);

        const handleRetry = React.useCallback(() => {
            setError(null);
            setRetryCount(prev => prev + 1);
        }, []);

        if (error) {
            return <ErrorFallback error={error} retry={handleRetry} />;
        }

        return (
            <React.Suspense fallback={<LoadingFallback />}>
                <LazyComponent {...props} key={retryCount} />
            </React.Suspense>
        );
    });
    
    LazyComponentWithErrorBoundary.displayName = `withLazyErrorBoundary(${LazyComponent.displayName || LazyComponent.name || 'Component'})`;
    
    return LazyComponentWithErrorBoundary;
}

/**
 * Preload components for better user experience
 */
export const preloadComponents = {
    vgplot: () => import('@/components/vgplot'),
    vgplotRefactored: () => import('@/components/charts/VgPlotRefactored'),
    dataAnalysis: () => import('@/pages/data_analysis'),
    dataAnalysisRefactored: () => import('@/pages/data_analysis_refactored'),
    queryBuilder: () => import('@/pages/query_builder'),
    multiSelect: () => import('@/components/multi-select'),
    vgmenu: () => import('@/components/vgmenu'),
    duckdbContext: () => import('@/context/DuckDBContext'),
};

/**
 * Utility to preload critical components
 */
export function preloadCriticalComponents(): void {
    // Preload components that are likely to be needed soon
    if (typeof window !== 'undefined') {
        // Use requestIdleCallback if available, otherwise setTimeout
        const schedulePreload = (fn: () => Promise<unknown>) => {
            if ('requestIdleCallback' in window) {
                window.requestIdleCallback(() => fn().catch(console.warn));
            } else {
                setTimeout(() => fn().catch(console.warn), 100);
            }
        };

        // Preload in order of importance
        schedulePreload(preloadComponents.duckdbContext);
        schedulePreload(preloadComponents.vgplotRefactored);
        schedulePreload(preloadComponents.dataAnalysisRefactored);
        schedulePreload(preloadComponents.queryBuilder);
    }
}

const LazyComponentsExport = {
    LazyVgPlot,
    LazyVgPlotRefactored,
    LazyDataAnalysis,
    LazyDataAnalysisRefactored,
    LazyQueryBuilder,
    LazyMultiSelect,
    LazyVgMenu,
    LazyDuckDBProvider,
    LazyLinePlotChart,
    LazyNumericalHistogramChart,
    LazyCategoricalHistogramChart,
    LazyDataLoadingState,
    LazyDataAnalysisToolbar,
    LazyColumnSelector,
    LazyVisualizationGrid,
    withLazyErrorBoundary,
    preloadComponents,
    preloadCriticalComponents
};

export default LazyComponentsExport;