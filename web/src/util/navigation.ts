// src/util/navigation.ts
import { useRouter as useNextRouter } from 'next/router';
import {
    useNavigate as useReactRouterNavigate,
    useLocation,
    NavigateOptions
} from 'react-router-dom';

/**
 * A hook that provides navigation functionality compatible with both
 * Next.js and React Router, allowing for incremental migration
 */
export function useNavigation() {
    // Try to use React Router's useNavigate, which will throw an error if not in a Router context
    let reactNavigate;
    let reactLocation;
    let isReactRouter = false;

    try {
        reactNavigate = useReactRouterNavigate();
        reactLocation = useLocation();
        isReactRouter = true;
    } catch (e) {
        // Not in a React Router context, will use Next.js router
        console.log("React Router not available, falling back to Next.js router");
    }

    // Use Next.js router as fallback
    const nextRouter = useNextRouter();

    /**
     * Navigate to a path using either React Router or Next.js Router
     */
    const navigate = (path: string, options?: NavigateOptions) => {
        console.log(`Navigation requested to: ${path}, using ${isReactRouter ? 'React Router' : 'Next.js Router'}`);

        if (isReactRouter && reactNavigate) {
            try {
                reactNavigate(path, options);
                console.log(`Successfully navigated to ${path} using React Router`);
                return;
            } catch (error) {
                console.error("React Router navigation failed:", error);
                // Fall back to Next.js if React Router navigation fails
            }
        }

        // If we get here, either isReactRouter is false or React Router navigation failed
        try {
            console.log(`Attempting Next.js navigation to ${path}`);
            nextRouter.push(path);
            console.log(`Successfully initiated Next.js navigation to ${path}`);
        } catch (error) {
            console.error("Next.js navigation failed:", error);

            // Last resort fallback - use window.location
            console.log("Falling back to window.location for navigation");
            window.location.href = path;
        }
    };

    /**
     * Get the current path/location
     */
    const getCurrentPath = () => {
        if (isReactRouter && reactLocation) {
            return reactLocation.pathname;
        } else {
            return nextRouter.pathname;
        }
    };

    return {
        navigate,
        getCurrentPath,
        isUsingReactRouter: isReactRouter
    };
}