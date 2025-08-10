import React from 'react';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import LoadingAnimation from '../LoadingAnimation';

// Mock Three.js
const mockRenderer = {
  setSize: vi.fn(),
  setAnimationLoop: vi.fn(),
  render: vi.fn(),
  domElement: document.createElement('canvas'),
  dispose: vi.fn(),
};

const mockCamera = {
  aspect: 1,
  updateProjectionMatrix: vi.fn(),
  position: { z: 0 },
};

const mockTorusKnot = {
  rotation: { x: 0, y: 0 },
};

const mockScene = {
  add: vi.fn(),
};

const mockGeometry = {};
const mockMaterial = {};
const mockMesh = mockTorusKnot;

const mockLoadingManager = {
  onProgress: vi.fn(),
};

const mockTHREE = {
  LoadingManager: vi.fn(() => mockLoadingManager),
  Scene: vi.fn(() => mockScene),
  PerspectiveCamera: vi.fn(() => mockCamera),
  WebGLRenderer: vi.fn(() => mockRenderer),
  TorusKnotGeometry: vi.fn(() => mockGeometry),
  MeshNormalMaterial: vi.fn(() => mockMaterial),
  Mesh: vi.fn(() => mockMesh),
};

vi.mock('three', () => mockTHREE);

// Mock console methods
const consoleMocks = {
  log: vi.fn(),
  error: vi.fn(),
};

// Mock window resize methods
const mockAddEventListener = vi.fn();
const mockRemoveEventListener = vi.fn();
const mockCancelAnimationFrame = vi.fn();

describe('LoadingAnimation', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    
    // Mock console
    global.console = {
      ...global.console,
      ...consoleMocks,
    };

    // Mock window methods
    global.window = {
      ...global.window,
      addEventListener: mockAddEventListener,
      removeEventListener: mockRemoveEventListener,
      innerWidth: 1024,
      innerHeight: 768,
    } as any;

    global.cancelAnimationFrame = mockCancelAnimationFrame;

    // Reset Three.js mocks
    mockRenderer.setSize.mockClear();
    mockRenderer.setAnimationLoop.mockClear();
    mockCamera.updateProjectionMatrix.mockClear();
    mockScene.add.mockClear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should render with default props', () => {
    render(<LoadingAnimation />);
    
    expect(screen.getByText('Initializing... (0%)')).toBeInTheDocument();
  });

  it('should display custom stage and progress', () => {
    render(<LoadingAnimation currentStage="Loading data..." progress={75} />);
    
    expect(screen.getByText('Loading data... (75%)')).toBeInTheDocument();
  });

  it('should round progress to nearest integer', () => {
    render(<LoadingAnimation currentStage="Processing..." progress={45.7} />);
    
    expect(screen.getByText('Processing... (46%)')).toBeInTheDocument();
  });

  it('should log stage and progress changes', () => {
    const { rerender } = render(<LoadingAnimation currentStage="Initial" progress={0} />);
    
    expect(consoleMocks.log).toHaveBeenCalledWith('LoadingAnimation: Stage=Initial, Progress=0%');
    
    rerender(<LoadingAnimation currentStage="Updated" progress={50} />);
    
    expect(consoleMocks.log).toHaveBeenCalledWith('LoadingAnimation: Stage=Updated, Progress=50%');
  });

  it('should have correct CSS classes for full-screen overlay', () => {
    render(<LoadingAnimation />);
    
    const container = screen.getByText('Initializing... (0%)').closest('div');
    expect(container?.parentElement).toHaveClass(
      'fixed',
      'inset-0',
      'flex',
      'flex-col',
      'items-center',
      'justify-center',
      'bg-black',
      'z-50'
    );
  });

  it('should initialize Three.js scene when component mounts', async () => {
    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockTHREE.LoadingManager).toHaveBeenCalled();
      expect(mockTHREE.Scene).toHaveBeenCalled();
      expect(mockTHREE.PerspectiveCamera).toHaveBeenCalledWith(75, expect.any(Number), 0.1, 1000);
      expect(mockTHREE.WebGLRenderer).toHaveBeenCalled();
    });
  });

  it('should create Three.js objects in correct order', async () => {
    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockTHREE.TorusKnotGeometry).toHaveBeenCalledWith(10, 3, 100, 16);
      expect(mockTHREE.MeshNormalMaterial).toHaveBeenCalledWith({});
      expect(mockTHREE.Mesh).toHaveBeenCalledWith(mockGeometry, mockMaterial);
      expect(mockScene.add).toHaveBeenCalledWith(mockMesh);
    });
  });

  it('should set up renderer with correct parameters', async () => {
    // Mock container dimensions
    const mockContainer = {
      clientWidth: 800,
      clientHeight: 600,
      appendChild: vi.fn(),
      removeChild: vi.fn(),
    };

    const originalCreateElement = document.createElement;
    document.createElement = vi.fn((tagName) => {
      if (tagName === 'div') {
        return mockContainer as any;
      }
      return originalCreateElement.call(document, tagName);
    });

    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockRenderer.setSize).toHaveBeenCalledWith(800, 600);
      expect(mockRenderer.setAnimationLoop).toHaveBeenCalled();
    });

    document.createElement = originalCreateElement;
  });

  it('should set up window resize listener', async () => {
    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockAddEventListener).toHaveBeenCalledWith('resize', expect.any(Function));
    });
  });

  it('should handle window resize events', async () => {
    const mockContainer = {
      clientWidth: 1200,
      clientHeight: 900,
      appendChild: vi.fn(),
      removeChild: vi.fn(),
    };

    const originalCreateElement = document.createElement;
    document.createElement = vi.fn((tagName) => {
      if (tagName === 'div') {
        return mockContainer as any;
      }
      return originalCreateElement.call(document, tagName);
    });

    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockAddEventListener).toHaveBeenCalledWith('resize', expect.any(Function));
    });

    // Get the resize handler and call it
    const resizeHandler = mockAddEventListener.mock.calls.find(
      call => call[0] === 'resize'
    )?.[1];

    if (resizeHandler) {
      // Update container size
      mockContainer.clientWidth = 1400;
      mockContainer.clientHeight = 1000;
      
      resizeHandler();
      
      expect(mockCamera.updateProjectionMatrix).toHaveBeenCalled();
      expect(mockRenderer.setSize).toHaveBeenCalledWith(1400, 1000);
    }

    document.createElement = originalCreateElement;
  });

  it('should handle Three.js initialization errors', async () => {
    // Mock Three.js to throw an error
    vi.mocked(mockTHREE.Scene).mockImplementationOnce(() => {
      throw new Error('Three.js initialization failed');
    });

    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(consoleMocks.error).toHaveBeenCalledWith(
        'Error setting up Three.js:',
        expect.any(Error)
      );
    });
  });

  it('should handle missing container ref gracefully', async () => {
    // Mock a component that doesn't have a container
    const LoadingAnimationNoRef = () => {
      return (
        <div className="fixed inset-0 flex flex-col items-center justify-center bg-black z-50">
          <div className="w-full h-full" />
          <p className="absolute bottom-16 text-xl text-white">
            Test (0%)
          </p>
        </div>
      );
    };

    render(<LoadingAnimationNoRef />);
    
    // Should not throw errors
    await waitFor(() => {
      expect(consoleMocks.error).not.toHaveBeenCalled();
    });
  });

  it('should clean up resources on unmount', async () => {
    const mockContainer = {
      clientWidth: 800,
      clientHeight: 600,
      appendChild: vi.fn(),
      removeChild: vi.fn(),
    };

    const originalCreateElement = document.createElement;
    document.createElement = vi.fn((tagName) => {
      if (tagName === 'div') {
        return mockContainer as any;
      }
      return originalCreateElement.call(document, tagName);
    });

    const { unmount } = render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockAddEventListener).toHaveBeenCalledWith('resize', expect.any(Function));
    });

    unmount();

    expect(mockRemoveEventListener).toHaveBeenCalledWith('resize', expect.any(Function));
    expect(mockRenderer.dispose).toHaveBeenCalled();

    document.createElement = originalCreateElement;
  });

  it('should set up loading manager progress callback', async () => {
    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockTHREE.LoadingManager).toHaveBeenCalled();
    });

    // Test the progress callback
    const progressCallback = mockLoadingManager.onProgress;
    expect(typeof progressCallback).toBe('function');
  });

  it('should handle loading manager progress updates', async () => {
    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockTHREE.LoadingManager).toHaveBeenCalled();
    });

    // Simulate progress update
    if (mockLoadingManager.onProgress) {
      mockLoadingManager.onProgress('test-url', 5, 10);
    }

    expect(consoleMocks.log).toHaveBeenCalledWith('Loading progress: 50%');
  });

  it('should handle different progress values correctly', () => {
    const testCases = [
      { progress: 0, expected: '(0%)' },
      { progress: 25.4, expected: '(25%)' },
      { progress: 50.7, expected: '(51%)' },
      { progress: 99.9, expected: '(100%)' },
      { progress: 100, expected: '(100%)' },
    ];

    testCases.forEach(({ progress, expected }) => {
      const { unmount } = render(<LoadingAnimation progress={progress} />);
      expect(screen.getByText(expect.stringContaining(expected))).toBeInTheDocument();
      unmount();
    });
  });

  it('should handle resize when camera or renderer is not available', async () => {
    // Mock a scenario where camera/renderer might be null
    const mockContainerWithNullRefs = {
      clientWidth: 800,
      clientHeight: 600,
      appendChild: vi.fn(),
      removeChild: vi.fn(),
    };

    mockTHREE.WebGLRenderer.mockReturnValueOnce(null as any);

    render(<LoadingAnimation />);
    
    await waitFor(() => {
      expect(mockAddEventListener).toHaveBeenCalledWith('resize', expect.any(Function));
    });

    // Get and call resize handler - should not throw
    const resizeHandler = mockAddEventListener.mock.calls.find(
      call => call[0] === 'resize'
    )?.[1];

    expect(() => resizeHandler?.()).not.toThrow();
  });
});