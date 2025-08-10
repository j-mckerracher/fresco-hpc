/**
 * Basic test to verify Jest setup is working correctly
 */
describe('Testing Infrastructure Setup', () => {
  it('should run tests successfully', () => {
    expect(true).toBe(true);
  });

  it('should have access to jest globals', () => {
    expect(jest).toBeDefined();
    expect(describe).toBeDefined();
    expect(it).toBeDefined();
    expect(expect).toBeDefined();
  });

  it('should support TypeScript', () => {
    const testValue: string = 'TypeScript works';
    expect(testValue).toBe('TypeScript works');
  });
});