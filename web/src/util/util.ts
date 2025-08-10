const stripTimezone = (date: Date)=> {
    const tzoffset = date.getTimezoneOffset() * 60000; // offset in milliseconds
    const withoutTimezone = new Date(date.valueOf() - tzoffset)
      .toISOString()
      .slice(0, -1); // Remove trailing 'Z'
  
    return withoutTimezone;
}

export default stripTimezone;