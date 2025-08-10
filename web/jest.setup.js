import '@testing-library/jest-dom';

// Mock Next.js router
jest.mock('next/router', () => ({
  useRouter() {
    return {
      route: '/',
      pathname: '/',
      query: {},
      asPath: '/',
      push: jest.fn(),
      replace: jest.fn(),
      reload: jest.fn(),
      back: jest.fn(),
      prefetch: jest.fn(),
      beforePopState: jest.fn(),
      events: {
        on: jest.fn(),
        off: jest.fn(),
        emit: jest.fn(),
      },
    };
  },
}));

// Mock DuckDB WASM
jest.mock('duckdb-wasm-kit', () => ({
  DuckDBKit: jest.fn().mockImplementation(() => ({
    connect: jest.fn().mockResolvedValue({
      query: jest.fn().mockResolvedValue([]),
      exec: jest.fn().mockResolvedValue(undefined),
      close: jest.fn().mockResolvedValue(undefined),
    }),
    disconnect: jest.fn().mockResolvedValue(undefined),
  })),
}));

// Mock VGPlot
jest.mock('@uwdata/vgplot', () => ({
  coordinator: jest.fn().mockReturnValue({
    connect: jest.fn(),
    value: jest.fn(),
  }),
  hconcat: jest.fn(),
  vconcat: jest.fn(),
  plot: jest.fn(),
  from: jest.fn(),
  rectY: jest.fn(),
  rectX: jest.fn(),
  lineY: jest.fn(),
  dot: jest.fn(),
  barY: jest.fn(),
  barX: jest.fn(),
  area: jest.fn(),
  ruleY: jest.fn(),
  ruleX: jest.fn(),
  text: jest.fn(),
  plotlyJSON: jest.fn(),
  loadParquet: jest.fn(),
}));

// Mock AWS SDK
jest.mock('@aws-sdk/client-cognito-identity', () => ({
  CognitoIdentityClient: jest.fn(),
  GetIdCommand: jest.fn(),
  GetCredentialsForIdentityCommand: jest.fn(),
}));

// Mock React Router DOM
jest.mock('react-router-dom', () => ({
  ...jest.requireActual('react-router-dom'),
  useNavigate: () => jest.fn(),
  useLocation: () => ({ pathname: '/' }),
  useParams: () => ({}),
}));

// Mock file operations
Object.defineProperty(window, 'URL', {
  value: {
    createObjectURL: jest.fn(() => 'mocked-url'),
    revokeObjectURL: jest.fn(),
  },
});

// Mock localStorage
const localStorageMock = {
  getItem: jest.fn(),
  setItem: jest.fn(),
  removeItem: jest.fn(),
  clear: jest.fn(),
};
Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
});

// Mock ResizeObserver
global.ResizeObserver = jest.fn().mockImplementation(() => ({
  observe: jest.fn(),
  unobserve: jest.fn(),
  disconnect: jest.fn(),
}));

// Mock IntersectionObserver
global.IntersectionObserver = jest.fn().mockImplementation(() => ({
  observe: jest.fn(),
  unobserve: jest.fn(),
  disconnect: jest.fn(),
}));

// Mock WebAssembly
global.WebAssembly = {
  instantiate: jest.fn(),
  Module: jest.fn(),
  Instance: jest.fn(),
  Memory: jest.fn(),
  Table: jest.fn(),
  CompileError: jest.fn(),
  LinkError: jest.fn(),
  RuntimeError: jest.fn(),
};