# FRESCO Web Frontend

FRESCO is a web application for analyzing high-performance computing (HPC) job data. Built with Next.js, TypeScript, and TailwindCSS, it uses DuckDB-WASM for in-browser data processing and analysis of time-series data from HPC clusters.

## Features

- **Interactive Data Visualization**: Real-time charts and histograms using VGPlot
- **In-Browser Analytics**: DuckDB-WASM for fast, client-side data processing
- **Time Series Analysis**: HPC job data analysis with temporal filtering
- **Data Export**: CSV export functionality with customizable filters
- **Responsive Design**: Mobile-friendly interface with TailwindCSS

## Tech Stack

- **Framework**: Next.js 14 with Pages Router
- **Language**: TypeScript
- **Styling**: TailwindCSS
- **Database**: DuckDB-WASM
- **Visualization**: VGPlot (Observable Plot)
- **Cloud Integration**: AWS SDK for S3 data access
- **Testing**: Vitest + React Testing Library

## Getting Started

### Prerequisites

- Node.js 18+ 
- npm, yarn, pnpm, or bun

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd FRESCO-web-front-end

# Install dependencies
npm install
```

### Development

```bash
# Start the development server
npm run dev

# Alternative package managers
yarn dev
pnpm dev
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the application.

### Build for Production

```bash
# Build the application
npm run build

# Start production server
npm start
```

## Testing

FRESCO has comprehensive test coverage (95%+) across all components.

### Testing Framework

- **Test Runner**: [Vitest](https://vitest.dev/) - Fast, modern testing framework
- **React Testing**: [React Testing Library](https://testing-library.com/docs/react-testing-library/intro/)
- **DOM Assertions**: [@testing-library/jest-dom](https://github.com/testing-library/jest-dom)
- **Coverage**: [@vitest/coverage-v8](https://vitest.dev/guide/coverage.html)

### Running Tests

```bash
# Run all tests once
npm test

# Run tests in watch mode (re-runs on file changes)
npm run test:watch

# Run tests with coverage report
npm run test:coverage

# Run tests for CI/CD (coverage + no watch)
npm run test:ci

# Open Vitest UI for interactive testing
npm run test:ui
```

### Coverage Reports

Generate and view test coverage reports:

```bash
# Generate coverage report
npm run test:coverage
```

This creates coverage reports in multiple formats:
- **Terminal**: Text summary displayed in console
- **HTML**: Interactive report at `coverage/index.html`
- **LCOV**: Machine-readable format at `coverage/lcov.info`

#### Viewing HTML Coverage Report

```bash
# After running coverage, open the HTML report
open coverage/index.html
# or on Windows
start coverage/index.html
# or on Linux
xdg-open coverage/index.html
```

The HTML report provides:
- Line-by-line coverage visualization
- Branch coverage details
- Function coverage metrics
- Interactive file navigation

### Coverage Targets

The project maintains high coverage standards:

- **Lines**: 95%
- **Functions**: 95% 
- **Branches**: 95%
- **Statements**: 95%

### Test Structure

```
src/
├── __tests__/           # Setup and configuration tests
├── components/
│   └── __tests__/       # Component tests
├── hooks/
│   └── __tests__/       # Custom hook tests
├── context/
│   └── __tests__/       # React context tests
├── util/
│   └── __tests__/       # Utility function tests
├── utils/
│   └── __tests__/       # Business logic tests
└── types/
    └── __tests__/       # TypeScript type tests
```

### What's Tested

✅ **Core Business Logic**
- API communication and data loading
- Database operations (DuckDB-WASM)
- Data export and transformation
- Error handling and recovery

✅ **React Components**
- User interface components
- Error boundaries and fallbacks
- Loading animations and states

✅ **State Management**
- React Context providers
- Custom hooks
- Data loading orchestration

✅ **Utilities**
- Date/time processing
- Navigation abstraction
- Data persistence (localStorage)

✅ **TypeScript Types**
- Interface validation
- Enum definitions
- Type composition

### Test Configuration

#### Vitest Configuration (`vitest.config.ts`)

```typescript
export default defineConfig({
  test: {
    environment: 'jsdom',
    setupFiles: ['./vitest.setup.ts'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html', 'lcov'],
      reportsDirectory: './coverage',
      thresholds: {
        lines: 95,
        functions: 95,
        branches: 95,
        statements: 95,
      }
    }
  }
})
```

#### Mock Setup

The test setup includes comprehensive mocking for:
- **DuckDB-WASM**: Database operations
- **VGPlot**: Data visualization
- **AWS SDK**: Cloud service integration
- **Next.js Router**: Navigation
- **Browser APIs**: localStorage, File API, etc.

### Writing Tests

#### Example Component Test

```typescript
import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import MyComponent from '../MyComponent';

describe('MyComponent', () => {
  it('should render correctly', () => {
    render(<MyComponent />);
    expect(screen.getByText('Hello World')).toBeInTheDocument();
  });

  it('should handle click events', () => {
    const onClick = vi.fn();
    render(<MyComponent onClick={onClick} />);
    
    fireEvent.click(screen.getByRole('button'));
    expect(onClick).toHaveBeenCalledTimes(1);
  });
});
```

#### Example Utility Test

```typescript
import { describe, it, expect } from 'vitest';
import { myUtilityFunction } from '../myUtility';

describe('myUtilityFunction', () => {
  it('should process data correctly', () => {
    const input = { data: 'test' };
    const result = myUtilityFunction(input);
    
    expect(result).toEqual({ processed: 'test' });
  });
});
```

### Continuous Integration

Tests are automatically run in CI/CD pipelines:

```bash
# CI command
npm run test:ci
```

This command:
- Runs all tests once (no watch mode)
- Generates coverage reports
- Enforces coverage thresholds
- Exits with error code if tests fail

### Debugging Tests

#### VS Code Integration

Add to `.vscode/launch.json`:

```json
{
  "type": "node",
  "request": "launch",
  "name": "Debug Vitest Tests",
  "program": "${workspaceFolder}/node_modules/vitest/vitest.mjs",
  "args": ["run", "--reporter=verbose"],
  "console": "integratedTerminal",
  "internalConsoleOptions": "neverOpen"
}
```

#### Browser Debugging

```bash
# Run tests with browser debugging
npm run test:ui
```

Opens an interactive web interface for:
- Running individual tests
- Viewing test results
- Debugging test failures
- Exploring coverage reports

## Project Structure

```
src/
├── components/          # React components
├── pages/              # Next.js pages
├── hooks/              # Custom React hooks
├── context/            # React context providers
├── util/               # Utility functions
├── utils/              # Business logic utilities
├── types/              # TypeScript type definitions
└── styles/             # CSS and styling
```

## Development Workflow

1. **Start Development Server**: `npm run dev`
2. **Run Tests in Watch Mode**: `npm run test:watch`
3. **Make Changes**: Edit source files
4. **Verify Tests Pass**: Tests auto-run in watch mode
5. **Check Coverage**: `npm run test:coverage`
6. **Build for Production**: `npm run build`

## Learn More

- [Next.js Documentation](https://nextjs.org/docs) - Learn about Next.js features and API
- [Vitest Documentation](https://vitest.dev/) - Modern testing framework
- [React Testing Library](https://testing-library.com/) - Simple and complete testing utilities
- [DuckDB-WASM](https://github.com/duckdb/duckdb-wasm) - In-browser analytical database
- [VGPlot](https://observablehq.com/plot/) - Grammar of graphics for JavaScript

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Ensure tests pass: `npm test`
5. Verify coverage: `npm run test:coverage`
6. Submit a pull request

All contributions must maintain the 95% test coverage threshold.