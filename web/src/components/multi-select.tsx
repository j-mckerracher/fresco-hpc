"use client";

import React, { useState, useRef, useEffect } from "react";
import { ChevronDown, X } from "lucide-react";

type Option = {
  label: string;
  value: string;
};

type MultiSelectProps = {
  options: Option[];
  selected: Option[];
  onChange: (selected: Option[]) => void;
  placeholder?: string;
  maxItems?: number;
  className?: string;
  optionClassName?: string;
  selectedItemClassName?: string;
  dropdownClassName?: string;
  renderOption?: (option: Option) => React.ReactNode;
  renderSelectedItem?: (option: Option) => React.ReactNode;
  isSearchable?: boolean;
  noOptionsMessage?: string;
  maxSelectedMessage?: string;
};

export default function MultiSelect({
  options,
  selected = [],
  onChange,
  placeholder = "Select items...",
  maxItems,
  className = "",
  optionClassName = "",
  selectedItemClassName = "",
  dropdownClassName = "",
  renderOption,
  renderSelectedItem,
  isSearchable = true,
  noOptionsMessage = "No options found",
  maxSelectedMessage = "Maximum items selected",
}: MultiSelectProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [maxReached, setMaxReached] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        wrapperRef.current &&
        !wrapperRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [wrapperRef]);

  useEffect(() => {
    setMaxReached(!!maxItems && selected.length >= maxItems);
  }, [selected, maxItems]);

  const toggleOption = (option: Option) => {
    if (maxReached && !selected.some((item) => item.value === option.value)) {
      return;
    }
    const updatedSelected = selected.some((item) => item.value === option.value)
      ? selected.filter((item) => item.value !== option.value)
      : [...selected, option];
    onChange(updatedSelected);
  };

  const handleKeyDown = (e: React.KeyboardEvent, option: Option) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggleOption(option);
    }
  };

  const filteredOptions = options.filter((option) =>
    option.label.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className={`relative w-full ${className}`} ref={wrapperRef}>
      <div
        className="flex flex-wrap items-center gap-2 p-2 border rounded-md cursor-pointer bg-white"
        onClick={() => setIsOpen(!isOpen)}
      >
        {selected.length > 0 ? (
          selected.map((option) => (
            <span
              key={option.value}
              className={`flex items-center gap-1 px-2 py-1 text-sm bg-gray-200 rounded-full ${selectedItemClassName}`}
            >
              {renderSelectedItem ? renderSelectedItem(option) : option.label}
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  toggleOption(option);
                }}
                className="focus:outline-none"
                aria-label={`Remove ${option.label}`}
              >
                <X className="w-4 h-4" />
              </button>
            </span>
          ))
        ) : (
          <span className="text-gray-400">{placeholder}</span>
        )}
        <ChevronDown className="w-4 h-4 ml-auto" />
      </div>
      {isOpen && (
        <div
          className={`absolute z-10 w-full mt-1 bg-white border rounded-md shadow-lg ${dropdownClassName}`}
        >
          {isSearchable && (
            <input
              type="text"
              className="w-full p-2 border-b"
              placeholder="Search..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          )}
          <ul className="max-h-64 overflow-auto">
            {filteredOptions.length > 0 ? (
              filteredOptions.map((option) => (
                <li
                  key={option.value}
                  className={`p-2 cursor-pointer hover:bg-gray-100 ${
                    selected.some((item) => item.value === option.value)
                      ? "bg-blue-100"
                      : ""
                  } ${optionClassName}`}
                  onClick={() => toggleOption(option)}
                  onKeyDown={(e) => handleKeyDown(e, option)}
                  tabIndex={0}
                  role="option"
                  aria-selected={selected.some(
                    (item) => item.value === option.value
                  )}
                >
                  {renderOption ? renderOption(option) : option.label}
                </li>
              ))
            ) : (
              <li className="p-2 text-gray-500">{noOptionsMessage}</li>
            )}
          </ul>
          {maxReached && (
            <div className="p-2 text-sm text-red-500">{maxSelectedMessage}</div>
          )}
        </div>
      )}
    </div>
  );
}
