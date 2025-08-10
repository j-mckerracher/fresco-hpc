/**
 * Chart styling utilities for FRESCO visualizations
 * 
 * Provides consistent styling functions for different chart types
 * with proper theming for the FRESCO application.
 */

import { CHART_COLORS } from './ChartConstants';

/**
 * Base style configuration applied to all charts
 */
export const getBaseStyle = () => ({
    color: CHART_COLORS.WHITE,
    backgroundColor: CHART_COLORS.TRANSPARENT,
    fontSize: "14px",
    ".vgplot-x-axis line, .vgplot-y-axis line": {
        stroke: CHART_COLORS.WHITE,
    },
    ".vgplot-x-axis text, .vgplot-y-axis text": {
        fill: CHART_COLORS.WHITE,
    }
});

/**
 * Style configuration for line plots
 */
export const getLinePlotStyle = () => ({
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

/**
 * Style configuration for histograms
 */
export const getHistogramStyle = () => ({
    ...getBaseStyle(),
    "font-size": "0.8rem"
});

/**
 * Style configuration for numerical histograms
 */
export const getNumericalHistogramStyle = () => ({
    ...getBaseStyle(),
    ".vgplot-marks rect": {
        strokeWidth: "0.5px",
        fillOpacity: "0.7"
    },
    "font-size": "0.8rem"
});

/**
 * Style configuration for categorical histograms with rotated labels
 */
export const getCategoricalHistogramStyle = () => ({
    ...getBaseStyle(),
    ".vgplot-marks rect": {
        strokeWidth: "0.5px",
        fillOpacity: "0.8"
    },
    "svg g[aria-label='x-axis tick label'] text": {
        transform: "rotate(-45deg) !important",
        transformOrigin: "10px 10px !important",
        textAnchor: "end !important",
        fontSize: "0.75rem !important",
        fontWeight: "normal !important",
        letterSpacing: "0.01em !important"
    }
});

/**
 * Get style configuration based on chart type
 */
export const getStyleForChartType = (chartType: 'line' | 'histogram' | 'categorical'): Record<string, unknown> => {
    switch (chartType) {
        case 'line':
            return getLinePlotStyle();
        case 'categorical':
            return getCategoricalHistogramStyle();
        case 'histogram':
        default:
            return getHistogramStyle();
    }
};