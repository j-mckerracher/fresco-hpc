import { vi, expect } from 'vitest';
import * as matchers from '@testing-library/jest-dom/matchers';

// Extend Vitest's expect with jest-dom matchers
expect.extend(matchers);

// Mock Next.js router
vi.mock('next/router', () => ({
  useRouter: () => ({
    route: '/',
    pathname: '/',
    query: {},
    asPath: '/',
    push: vi.fn(),
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
  }),
}));

// Mock DuckDB WASM
vi.mock('duckdb-wasm-kit', () => ({
  DuckDBKit: vi.fn().mockImplementation(() => ({
    connect: vi.fn().mockResolvedValue({
      query: vi.fn().mockResolvedValue([]),
      exec: vi.fn().mockResolvedValue(undefined),
      close: vi.fn().mockResolvedValue(undefined),
    }),
    disconnect: vi.fn().mockResolvedValue(undefined),
  })),
}));

// Mock VGPlot
vi.mock('@uwdata/vgplot', () => ({
  coordinator: vi.fn().mockReturnValue({
    connect: vi.fn(),
    value: vi.fn(),
  }),
  hconcat: vi.fn(),
  vconcat: vi.fn(),
  plot: vi.fn(),
  from: vi.fn(),
  rectY: vi.fn(),
  rectX: vi.fn(),
  lineY: vi.fn(),
  dot: vi.fn(),
  barY: vi.fn(),
  barX: vi.fn(),
  area: vi.fn(),
  ruleY: vi.fn(),
  ruleX: vi.fn(),
  text: vi.fn(),
  plotlyJSON: vi.fn(),
  loadParquet: vi.fn(),
}));

// Mock localStorage
Object.defineProperty(window, 'localStorage', {
  value: {
    getItem: vi.fn(),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
  },
});

// Mock ResizeObserver
global.ResizeObserver = vi.fn().mockImplementation(() => ({
  observe: vi.fn(),
  unobserve: vi.fn(),
  disconnect: vi.fn(),
}));

// Mock IntersectionObserver
global.IntersectionObserver = vi.fn().mockImplementation(() => ({
  observe: vi.fn(),
  unobserve: vi.fn(),
  disconnect: vi.fn(),
}));