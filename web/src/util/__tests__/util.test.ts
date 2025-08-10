import { describe, it, expect } from 'vitest';
import stripTimezone from '../util';

describe('stripTimezone', () => {
  it('should remove timezone offset from date', () => {
    // Create a test date: 2024-01-15 12:00:00 UTC
    const testDate = new Date('2024-01-15T12:00:00Z');
    
    const result = stripTimezone(testDate);
    
    // The result should be a string without the 'Z' suffix
    expect(typeof result).toBe('string');
    expect(result).not.toContain('Z');
    
    // Should contain the date components
    expect(result).toContain('2024-01-15');
    expect(result).toContain('T');
  });

  it('should handle different timezones consistently', () => {
    // Test with same UTC time but different timezone representations
    const utcDate = new Date('2024-01-15T12:00:00Z');
    const localDate = new Date('2024-01-15T12:00:00');
    
    const utcResult = stripTimezone(utcDate);
    const localResult = stripTimezone(localDate);
    
    expect(typeof utcResult).toBe('string');
    expect(typeof localResult).toBe('string');
    expect(utcResult).not.toContain('Z');
    expect(localResult).not.toContain('Z');
  });

  it('should return ISO string format without Z', () => {
    const testDate = new Date('2024-12-25T15:30:45.123Z');
    
    const result = stripTimezone(testDate);
    
    // Should be in ISO format but without the trailing Z
    expect(result).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$/);
  });

  it('should handle edge case dates', () => {
    // Test with epoch - adjusted for timezone offset
    const epochDate = new Date(0);
    const epochResult = stripTimezone(epochDate);
    // The result should be the local time representation
    expect(epochResult).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$/);
    
    // Test with far future date
    const futureDate = new Date('2099-12-31T23:59:59Z');
    const futureResult = stripTimezone(futureDate);
    expect(futureResult).toContain('2099-12-31T');
  });

  it('should handle current date', () => {
    const now = new Date();
    const result = stripTimezone(now);
    
    expect(typeof result).toBe('string');
    expect(result).not.toContain('Z');
    expect(result.length).toBeGreaterThan(15); // Should have reasonable length for datetime string
  });

  it('should account for timezone offset correctly', () => {
    // Create a specific UTC date
    const utcDate = new Date('2024-06-15T12:00:00Z');
    
    // Calculate expected offset adjustment
    const tzoffset = utcDate.getTimezoneOffset() * 60000;
    const expected = new Date(utcDate.valueOf() - tzoffset).toISOString().slice(0, -1);
    
    const result = stripTimezone(utcDate);
    
    expect(result).toBe(expected);
  });
});