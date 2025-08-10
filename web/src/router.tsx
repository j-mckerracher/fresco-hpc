// src/router.tsx
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import Layout from './components/Layout';
import HomePage from './views/HomePage';
import AboutPage from './views/AboutPage';
import TeamPage from './views/TeamPage';
import AuthPage from './views/AuthPage';
import QueryBuilderPage from './views/QueryBuilderPage';
import DataAnalysisPage from './views/DataAnalysisPage';
import { DuckDBProvider } from './context/DuckDBContext';

// Create router with all routes
const router = createBrowserRouter([
    {
        path: '/',
        element: <Layout />,
        children: [
            {
                index: true,
                element: <HomePage />,
            },
            {
                path: 'about',
                element: <AboutPage />,
            },
            {
                path: 'team',
                element: <TeamPage />,
            },
            {
                path: 'auth',
                element: <AuthPage />,
            },
            {
                path: 'query_builder',
                element: <QueryBuilderPage />,
            },
            {
                path: 'data_analysis',
                element: <DataAnalysisPage />,
            },
        ],
    },
]);

// Router provider with DuckDB context
export default function AppRouter() {
    return (
        <DuckDBProvider>
            <RouterProvider router={router} />
        </DuckDBProvider>
    );
}