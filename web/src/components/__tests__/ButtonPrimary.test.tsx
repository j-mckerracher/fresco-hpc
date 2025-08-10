import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import ButtonPrimary from '../ButtonPrimary';

describe('ButtonPrimary', () => {
  it('should render with label', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Test Button" onClick={mockOnClick} />);
    
    expect(screen.getByRole('button')).toHaveTextContent('Test Button');
  });

  it('should call onClick when clicked', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Click Me" onClick={mockOnClick} />);
    
    fireEvent.click(screen.getByRole('button'));
    
    expect(mockOnClick).toHaveBeenCalledTimes(1);
  });

  it('should be disabled when disabled prop is true', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Disabled Button" onClick={mockOnClick} disabled={true} />);
    
    const button = screen.getByRole('button');
    expect(button).toBeDisabled();
  });

  it('should not call onClick when disabled and clicked', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Disabled Button" onClick={mockOnClick} disabled={true} />);
    
    fireEvent.click(screen.getByRole('button'));
    
    expect(mockOnClick).not.toHaveBeenCalled();
  });

  it('should have correct CSS classes', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Styled Button" onClick={mockOnClick} />);
    
    const button = screen.getByRole('button');
    expect(button).toHaveClass(
      'bg-purdue-boilermakerGold',
      'text-black',
      'font-semibold',
      'px-4',
      'py-2',
      'rounded-full',
      'text-2xl'
    );
  });

  it('should handle undefined onClick gracefully', () => {
    render(<ButtonPrimary label="No Handler" onClick={undefined as any} />);
    
    const button = screen.getByRole('button');
    expect(button).toBeInTheDocument();
    
    // Should not throw when clicked
    expect(() => fireEvent.click(button)).not.toThrow();
  });

  it('should handle empty label', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="" onClick={mockOnClick} />);
    
    const button = screen.getByRole('button');
    expect(button).toHaveTextContent('');
    expect(button).toBeInTheDocument();
  });

  it('should default disabled to false', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Default Enabled" onClick={mockOnClick} />);
    
    const button = screen.getByRole('button');
    expect(button).not.toBeDisabled();
  });

  it('should explicitly set disabled to false', () => {
    const mockOnClick = vi.fn();
    
    render(<ButtonPrimary label="Explicitly Enabled" onClick={mockOnClick} disabled={false} />);
    
    const button = screen.getByRole('button');
    expect(button).not.toBeDisabled();
  });
});