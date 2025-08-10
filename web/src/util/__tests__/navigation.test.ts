import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook } from '@testing-library/react';
import { useNavigation } from '../navigation';

// Mock Next.js router
const mockNextPush = vi.fn();
const mockNextRouter = {
  push: mockNextPush,
  pathname: '/next-path',
  route: '/next-path',
  query: {},
  asPath: '/next-path',
  replace: vi.fn(),
  reload: vi.fn(),
  back: vi.fn(),
  prefetch: vi.fn(),
  beforePopState: vi.fn(),
  events: {
    on: vi.fn(),
    off: vi.fn(),
    emit: vi.fn(),
  },
};

// Mock React Router
const mockReactNavigate = vi.fn();
const mockReactLocation = {
  pathname: '/react-path',
  search: '',
  hash: '',
  state: null,
  key: 'test-key',
};

// Mock window.location
const mockWindowLocation = {
  href: '',
};

describe('useNavigation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock Next.js router
    vi.mock('next/router', () => ({
      useRouter: () => mockNextRouter,
    }));

    // Mock console methods
    global.console = {
      ...global.console,
      log: vi.fn(),
      error: vi.fn(),
    };

    // Mock window.location
    Object.defineProperty(window, 'location', {
      value: mockWindowLocation,
      writable: true,
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('with React Router available', () => {
    beforeEach(() => {
      // Mock React Router DOM to return successful hooks
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => mockReactLocation,
      }));
    });

    it('should use React Router when available', () => {
      const { result } = renderHook(() => useNavigation());
      
      expect(result.current.isUsingReactRouter).toBe(true);
      expect(result.current.getCurrentPath()).toBe('/react-path');
    });

    it('should navigate using React Router', () => {
      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/test-path');
      
      expect(mockReactNavigate).toHaveBeenCalledWith('/test-path', undefined);
      expect(mockNextPush).not.toHaveBeenCalled();
    });

    it('should pass options to React Router navigate', () => {
      const { result } = renderHook(() => useNavigation());
      const options = { replace: true, state: { test: true } };
      
      result.current.navigate('/test-path', options);
      
      expect(mockReactNavigate).toHaveBeenCalledWith('/test-path', options);
    });

    it('should fallback to Next.js router if React Router navigation fails', () => {
      mockReactNavigate.mockImplementation(() => {
        throw new Error('React Router navigation failed');
      });

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/test-path');
      
      expect(mockReactNavigate).toHaveBeenCalledWith('/test-path', undefined);
      expect(mockNextPush).toHaveBeenCalledWith('/test-path');
    });
  });

  describe('with React Router not available', () => {
    beforeEach(() => {
      // Mock React Router DOM to throw errors (not in router context)
      vi.mock('react-router-dom', () => ({
        useNavigate: () => {
          throw new Error('useNavigate() may be used only in the context of a <Router> component.');
        },
        useLocation: () => {
          throw new Error('useLocation() may be used only in the context of a <Router> component.');
        },
      }));
    });

    it('should use Next.js router when React Router is not available', () => {
      const { result } = renderHook(() => useNavigation());
      
      expect(result.current.isUsingReactRouter).toBe(false);
      expect(result.current.getCurrentPath()).toBe('/next-path');
    });

    it('should navigate using Next.js router', () => {
      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/test-path');
      
      expect(mockNextPush).toHaveBeenCalledWith('/test-path');
      expect(mockReactNavigate).not.toHaveBeenCalled();
    });

    it('should fallback to window.location if Next.js router fails', () => {
      mockNextPush.mockImplementation(() => {
        throw new Error('Next.js navigation failed');
      });

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/test-path');
      
      expect(mockNextPush).toHaveBeenCalledWith('/test-path');
      expect(mockWindowLocation.href).toBe('/test-path');
    });
  });

  describe('edge cases', () => {
    it('should handle navigation to root path', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => mockReactLocation,
      }));

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/');
      
      expect(mockReactNavigate).toHaveBeenCalledWith('/', undefined);
    });

    it('should handle navigation with query parameters', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => mockReactLocation,
      }));

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/search?q=test');
      
      expect(mockReactNavigate).toHaveBeenCalledWith('/search?q=test', undefined);
    });

    it('should handle navigation with hash fragments', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => mockReactLocation,
      }));

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/page#section');
      
      expect(mockReactNavigate).toHaveBeenCalledWith('/page#section', undefined);
    });

    it('should handle external URLs in fallback', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => {
          throw new Error('Not in router context');
        },
        useLocation: () => {
          throw new Error('Not in router context');
        },
      }));

      mockNextPush.mockImplementation(() => {
        throw new Error('Cannot navigate to external URL');
      });

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('https://external.com');
      
      expect(mockWindowLocation.href).toBe('https://external.com');
    });
  });

  describe('getCurrentPath', () => {
    it('should return React Router pathname when available', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => ({ ...mockReactLocation, pathname: '/custom-react-path' }),
      }));

      const { result } = renderHook(() => useNavigation());
      
      expect(result.current.getCurrentPath()).toBe('/custom-react-path');
    });

    it('should return Next.js pathname when React Router not available', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => {
          throw new Error('Not in router context');
        },
        useLocation: () => {
          throw new Error('Not in router context');
        },
      }));

      mockNextRouter.pathname = '/custom-next-path';

      const { result } = renderHook(() => useNavigation());
      
      expect(result.current.getCurrentPath()).toBe('/custom-next-path');
    });
  });

  describe('logging', () => {
    it('should log navigation attempts', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => mockReactLocation,
      }));

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/test-path');
      
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('Navigation requested to: /test-path')
      );
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('using React Router')
      );
    });

    it('should log fallback to Next.js router', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => {
          throw new Error('Not in router context');
        },
        useLocation: () => {
          throw new Error('Not in router context');
        },
      }));

      const { result } = renderHook(() => useNavigation());
      
      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('React Router not available, falling back to Next.js router')
      );
    });

    it('should log errors when navigation fails', () => {
      vi.mock('react-router-dom', () => ({
        useNavigate: () => mockReactNavigate,
        useLocation: () => mockReactLocation,
      }));

      mockReactNavigate.mockImplementation(() => {
        throw new Error('Navigation error');
      });

      const { result } = renderHook(() => useNavigation());
      
      result.current.navigate('/test-path');
      
      expect(console.error).toHaveBeenCalledWith(
        'React Router navigation failed:',
        expect.any(Error)
      );
    });
  });
});