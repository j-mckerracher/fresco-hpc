import { describe, it, expect, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useColumnSelection } from '../useColumnSelection';

interface ColumnOption {
  value: string;
  label: string;
  numerical: boolean;
  linePlot: boolean;
}

describe('useColumnSelection', () => {
  let mockColumnOptions: ColumnOption[];
  let mockAvailableColumns: string[];

  beforeEach(() => {
    mockColumnOptions = [
      { value: 'time', label: 'Time', numerical: false, linePlot: false },
      { value: 'value_cpuuser', label: 'CPU User %', numerical: true, linePlot: true },
      { value: 'value_memused', label: 'Memory Used', numerical: true, linePlot: true },
      { value: 'nhosts', label: 'Number of Hosts', numerical: true, linePlot: false },
      { value: 'account', label: 'Account', numerical: false, linePlot: false },
      { value: 'queue', label: 'Queue', numerical: false, linePlot: false },
      { value: 'value_gpu', label: 'GPU Usage', numerical: true, linePlot: true },
      { value: 'jid', label: 'Job ID', numerical: false, linePlot: false },
    ];

    mockAvailableColumns = [
      'time',
      'value_cpuuser',
      'value_memused', 
      'nhosts',
      'account',
      'queue',
      // Note: 'value_gpu' and 'jid' are intentionally missing to test filtering
    ];
  });

  describe('initialization', () => {
    it('should initialize with time column selected for histograms', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      expect(result.current.histogramColumns).toEqual([
        { value: 'time', label: 'Time' }
      ]);
      expect(result.current.linePlotColumns).toEqual([]);
    });

    it('should create valueToNumerical map correctly', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      expect(result.current.valueToNumerical.get('time')).toBe(false);
      expect(result.current.valueToNumerical.get('value_cpuuser')).toBe(true);
      expect(result.current.valueToNumerical.get('nhosts')).toBe(true);
      expect(result.current.valueToNumerical.get('account')).toBe(false);
    });
  });

  describe('available options filtering', () => {
    it('should filter histogram options correctly', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const expectedHistogramOptions = [
        { value: 'time', label: 'Time', numerical: false, linePlot: false },
        { value: 'nhosts', label: 'Number of Hosts', numerical: true, linePlot: false },
        { value: 'account', label: 'Account', numerical: false, linePlot: false },
        { value: 'queue', label: 'Queue', numerical: false, linePlot: false },
      ];

      expect(result.current.availableHistogramOptions).toEqual(expectedHistogramOptions);
    });

    it('should filter line plot options correctly', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const expectedLinePlotOptions = [
        { value: 'value_cpuuser', label: 'CPU User %', numerical: true, linePlot: true },
        { value: 'value_memused', label: 'Memory Used', numerical: true, linePlot: true },
        // value_gpu should be excluded because it's not in availableColumns
      ];

      expect(result.current.availableLinePlotOptions).toEqual(expectedLinePlotOptions);
    });

    it('should update filtered options when availableColumns change', () => {
      const { result, rerender } = renderHook(
        ({ availableColumns }) =>
          useColumnSelection({
            columnOptions: mockColumnOptions,
            availableColumns,
          }),
        {
          initialProps: { availableColumns: ['time', 'nhosts'] }
        }
      );

      expect(result.current.availableHistogramOptions).toHaveLength(2);
      expect(result.current.availableLinePlotOptions).toHaveLength(0);

      rerender({ availableColumns: [...mockAvailableColumns, 'value_gpu'] });

      expect(result.current.availableHistogramOptions).toHaveLength(4);
      expect(result.current.availableLinePlotOptions).toHaveLength(3);
    });
  });

  describe('histogram column selection', () => {
    it('should set valid histogram columns', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'time', label: 'Time' },
        { value: 'nhosts', label: 'Number of Hosts' },
        { value: 'account', label: 'Account' },
      ];

      act(() => {
        result.current.setHistogramColumns(newColumns);
      });

      expect(result.current.histogramColumns).toEqual(newColumns);
    });

    it('should filter out unavailable columns', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'time', label: 'Time' },
        { value: 'jid', label: 'Job ID' }, // Not in availableColumns
        { value: 'unavailable_column', label: 'Unavailable' }, // Not in columnOptions
      ];

      act(() => {
        result.current.setHistogramColumns(newColumns);
      });

      expect(result.current.histogramColumns).toEqual([
        { value: 'time', label: 'Time' }
      ]);
    });

    it('should filter out line plot columns from histogram selection', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'time', label: 'Time' },
        { value: 'value_cpuuser', label: 'CPU User %' }, // This is a line plot column
        { value: 'nhosts', label: 'Number of Hosts' },
      ];

      act(() => {
        result.current.setHistogramColumns(newColumns);
      });

      expect(result.current.histogramColumns).toEqual([
        { value: 'time', label: 'Time' },
        { value: 'nhosts', label: 'Number of Hosts' },
      ]);
    });
  });

  describe('line plot column selection', () => {
    it('should set valid line plot columns', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'value_cpuuser', label: 'CPU User %' },
        { value: 'value_memused', label: 'Memory Used' },
      ];

      act(() => {
        result.current.setLinePlotColumns(newColumns);
      });

      expect(result.current.linePlotColumns).toEqual(newColumns);
    });

    it('should filter out unavailable line plot columns', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'value_cpuuser', label: 'CPU User %' },
        { value: 'value_gpu', label: 'GPU Usage' }, // Not in availableColumns
      ];

      act(() => {
        result.current.setLinePlotColumns(newColumns);
      });

      expect(result.current.linePlotColumns).toEqual([
        { value: 'value_cpuuser', label: 'CPU User %' }
      ]);
    });

    it('should filter out non-line plot columns from line plot selection', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'value_cpuuser', label: 'CPU User %' },
        { value: 'nhosts', label: 'Number of Hosts' }, // Not a line plot column
        { value: 'time', label: 'Time' }, // Not a line plot column
      ];

      act(() => {
        result.current.setLinePlotColumns(newColumns);
      });

      expect(result.current.linePlotColumns).toEqual([
        { value: 'value_cpuuser', label: 'CPU User %' }
      ]);
    });
  });

  describe('edge cases', () => {
    it('should handle empty column options', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: [],
          availableColumns: [],
        })
      );

      expect(result.current.histogramColumns).toEqual([
        { value: 'time', label: 'Time' }
      ]);
      expect(result.current.availableHistogramOptions).toEqual([]);
      expect(result.current.availableLinePlotOptions).toEqual([]);
    });

    it('should handle empty available columns', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: [],
        })
      );

      expect(result.current.availableHistogramOptions).toEqual([]);
      expect(result.current.availableLinePlotOptions).toEqual([]);
    });

    it('should handle setting empty column arrays', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      act(() => {
        result.current.setHistogramColumns([]);
        result.current.setLinePlotColumns([]);
      });

      expect(result.current.histogramColumns).toEqual([]);
      expect(result.current.linePlotColumns).toEqual([]);
    });

    it('should handle columns with missing labels', () => {
      const { result } = renderHook(() =>
        useColumnSelection({
          columnOptions: mockColumnOptions,
          availableColumns: mockAvailableColumns,
        })
      );

      const newColumns = [
        { value: 'time', label: 'Time' },
        { value: 'nhosts', label: '' }, // Empty label
      ];

      act(() => {
        result.current.setHistogramColumns(newColumns);
      });

      expect(result.current.histogramColumns).toEqual(newColumns);
    });
  });

  describe('memoization', () => {
    it('should not recreate valueToNumerical map unnecessarily', () => {
      const { result, rerender } = renderHook(
        ({ availableColumns }) =>
          useColumnSelection({
            columnOptions: mockColumnOptions,
            availableColumns,
          }),
        {
          initialProps: { availableColumns: mockAvailableColumns }
        }
      );

      const firstMap = result.current.valueToNumerical;

      // Rerender with same columnOptions but different availableColumns
      rerender({ availableColumns: ['time', 'nhosts'] });

      const secondMap = result.current.valueToNumerical;

      // Map should be the same since columnOptions didn't change
      expect(firstMap).toBe(secondMap);
    });

    it('should recreate filtered options when dependencies change', () => {
      const { result, rerender } = renderHook(
        ({ availableColumns }) =>
          useColumnSelection({
            columnOptions: mockColumnOptions,
            availableColumns,
          }),
        {
          initialProps: { availableColumns: ['time'] }
        }
      );

      const firstHistogramOptions = result.current.availableHistogramOptions;
      expect(firstHistogramOptions).toHaveLength(1);

      rerender({ availableColumns: ['time', 'nhosts', 'account'] });

      const secondHistogramOptions = result.current.availableHistogramOptions;
      expect(secondHistogramOptions).toHaveLength(3);
      expect(firstHistogramOptions).not.toBe(secondHistogramOptions);
    });
  });
});