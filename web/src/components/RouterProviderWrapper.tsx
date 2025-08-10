// src/components/RouterProviderWrapper.tsx
import React from 'react';
import { BrowserRouter } from 'react-router-dom';

interface RouterProviderWrapperProps {
    children: React.ReactNode;
}

/**
 * A component that wraps children in a Router context if on the client side
 * This allows using React Router hooks in Next.js pages during migration
 */
const RouterProviderWrapper: React.FC<RouterProviderWrapperProps> = ({ children }) => {
    // Only render the Router on the client side to avoid Next.js SSR errors
    const [isClient, setIsClient] = React.useState(false);

    React.useEffect(() => {
        setIsClient(true);
    }, []);

    if (!isClient) {
        // Return children without Router during SSR
        return <>{children}</>;
    }

    // Wrap with BrowserRouter on client side
    return <BrowserRouter>{children}</BrowserRouter>;
};

export default RouterProviderWrapper;