/**
 * Database schema definitions for FRESCO
 * 
 * This module contains all database schema definitions and SQL generation
 * functions to eliminate code duplication across the application.
 */

import { HpcJobData } from '../types';

/**
 * HPC job data table schema definition
 */
export const HPC_JOB_SCHEMA = {
  TABLE_NAME: 'job_data_complete',
  TEMP_TABLE_NAME: 'job_data_small',
  COLUMNS: {
    // Time-related columns
    time: 'TIMESTAMP',
    submit_time: 'TIMESTAMP',
    start_time: 'TIMESTAMP',
    end_time: 'TIMESTAMP',
    timelimit: 'DOUBLE',
    
    // Job identification
    jid: 'VARCHAR',
    jobname: 'VARCHAR',
    username: 'VARCHAR',
    account: 'VARCHAR',
    queue: 'VARCHAR',
    exitcode: 'VARCHAR',
    
    // Resource allocation
    nhosts: 'BIGINT',
    ncores: 'BIGINT',
    host: 'VARCHAR',
    host_list: 'VARCHAR',
    unit: 'VARCHAR',
    
    // Performance metrics
    value_cpuuser: 'DOUBLE',
    value_gpu: 'DOUBLE',
    value_memused: 'DOUBLE',
    value_memused_minus_diskcache: 'DOUBLE',
    value_nfs: 'DOUBLE',
    value_block: 'DOUBLE'
  } as const
} as const;

/**
 * Generate CREATE TABLE SQL for HPC job data
 * 
 * @param tableName - Name of the table to create
 * @param ifNotExists - Whether to include IF NOT EXISTS clause
 * @returns SQL string for creating the table
 */
export function createHpcJobTableSQL(
  tableName: string = HPC_JOB_SCHEMA.TABLE_NAME,
  ifNotExists: boolean = true
): string {
  const ifNotExistsClause = ifNotExists ? 'IF NOT EXISTS ' : '';
  
  const columnDefinitions = Object.entries(HPC_JOB_SCHEMA.COLUMNS)
    .map(([columnName, columnType]) => `    ${columnName} ${columnType}`)
    .join(',\n');

  return `CREATE TABLE ${ifNotExistsClause}${tableName} (
${columnDefinitions}
);`;
}

/**
 * Generate INSERT SQL for HPC job data
 * 
 * @param tableName - Name of the target table
 * @param sourceTable - Name of the source table or SELECT statement
 * @returns SQL string for inserting data
 */
export function insertHpcJobDataSQL(
  tableName: string,
  sourceTable: string
): string {
  const columns = Object.keys(HPC_JOB_SCHEMA.COLUMNS).join(', ');
  
  return `INSERT INTO ${tableName} (${columns})
SELECT ${columns}
FROM ${sourceTable};`;
}

/**
 * Generate SELECT SQL for HPC job data with optional conditions
 * 
 * @param tableName - Name of the table to select from
 * @param columns - Columns to select (default: all)
 * @param whereClause - WHERE clause without the WHERE keyword
 * @param orderBy - ORDER BY clause without the ORDER BY keyword
 * @param limit - LIMIT clause value
 * @returns SQL string for selecting data
 */
export function selectHpcJobDataSQL(
  tableName: string,
  columns: string[] = ['*'],
  whereClause?: string,
  orderBy?: string,
  limit?: number
): string {
  let sql = `SELECT ${columns.join(', ')} FROM ${tableName}`;
  
  if (whereClause) {
    sql += ` WHERE ${whereClause}`;
  }
  
  if (orderBy) {
    sql += ` ORDER BY ${orderBy}`;
  }
  
  if (limit) {
    sql += ` LIMIT ${limit}`;
  }
  
  return sql + ';';
}

/**
 * Generate time-bound query for HPC job data
 * 
 * @param tableName - Name of the table to query
 * @param startTime - Start time (ISO string)
 * @param endTime - End time (ISO string)
 * @param additionalConditions - Additional WHERE conditions
 * @returns SQL string for time-bound query
 */
export function createTimeBoundQuery(
  tableName: string,
  startTime: string,
  endTime: string,
  additionalConditions?: string
): string {
  let whereClause = `time BETWEEN '${startTime}' AND '${endTime}'`;
  
  if (additionalConditions) {
    whereClause += ` AND ${additionalConditions}`;
  }
  
  return selectHpcJobDataSQL(tableName, ['*'], whereClause);
}

/**
 * Get all column names from the HPC job schema
 * 
 * @returns Array of column names
 */
export function getHpcJobColumns(): string[] {
  return Object.keys(HPC_JOB_SCHEMA.COLUMNS);
}

/**
 * Get column names by category
 * 
 * @param category - Category of columns to return
 * @returns Array of column names in the specified category
 */
export function getHpcJobColumnsByCategory(category: 'time' | 'job' | 'resource' | 'performance'): string[] {
  switch (category) {
    case 'time':
      return ['time', 'submit_time', 'start_time', 'end_time', 'timelimit'];
    case 'job':
      return ['jid', 'jobname', 'username', 'account', 'queue', 'exitcode'];
    case 'resource':
      return ['nhosts', 'ncores', 'host', 'host_list', 'unit'];
    case 'performance':
      return ['value_cpuuser', 'value_gpu', 'value_memused', 'value_memused_minus_diskcache', 'value_nfs', 'value_block'];
    default:
      return [];
  }
}

/**
 * Get user-friendly column names for display
 * 
 * @returns Map of column names to display names
 */
export function getColumnDisplayNames(): Record<string, string> {
  return {
    time: 'Time',
    submit_time: 'Submit Time',
    start_time: 'Start Time',
    end_time: 'End Time',
    timelimit: 'Time Limit',
    jid: 'Job ID',
    jobname: 'Job Name',
    username: 'Username',
    account: 'Account',
    queue: 'Queue',
    exitcode: 'Exit Code',
    nhosts: 'Number of Hosts',
    ncores: 'Number of Cores',
    host: 'Host',
    host_list: 'Host List',
    unit: 'Unit',
    value_cpuuser: 'CPU Usage',
    value_gpu: 'GPU Usage',
    value_memused: 'Memory Used',
    value_memused_minus_diskcache: 'Memory Used (minus disk cache)',
    value_nfs: 'NFS Usage',
    value_block: 'Block I/O'
  };
}

/**
 * Validate if a column name exists in the schema
 * 
 * @param columnName - Column name to validate
 * @returns True if column exists, false otherwise
 */
export function isValidColumn(columnName: string): boolean {
  return columnName in HPC_JOB_SCHEMA.COLUMNS;
}

/**
 * Get column type for a given column name
 * 
 * @param columnName - Column name
 * @returns Column type or undefined if column doesn't exist
 */
export function getColumnType(columnName: string): string | undefined {
  return HPC_JOB_SCHEMA.COLUMNS[columnName as keyof typeof HPC_JOB_SCHEMA.COLUMNS];
}

/**
 * Check if a column is numeric
 * 
 * @param columnName - Column name to check
 * @returns True if column is numeric, false otherwise
 */
export function isNumericColumn(columnName: string): boolean {
  const columnType = getColumnType(columnName);
  return columnType === 'DOUBLE' || columnType === 'BIGINT';
}

/**
 * Check if a column is temporal (time-related)
 * 
 * @param columnName - Column name to check
 * @returns True if column is temporal, false otherwise
 */
export function isTemporalColumn(columnName: string): boolean {
  const columnType = getColumnType(columnName);
  return columnType === 'TIMESTAMP';
}

/**
 * Check if a column is categorical (string-based)
 * 
 * @param columnName - Column name to check
 * @returns True if column is categorical, false otherwise
 */
export function isCategoricalColumn(columnName: string): boolean {
  const columnType = getColumnType(columnName);
  return columnType === 'VARCHAR';
}

/**
 * Generate demo data creation SQL
 * 
 * @param tableName - Name of the table to create demo data for
 * @param rowCount - Number of demo rows to create
 * @param startTime - Start time for demo data
 * @param endTime - End time for demo data
 * @returns SQL string for creating demo data
 */
export function generateDemoDataSQL(
  tableName: string,
  rowCount: number = 1000,
  startTime: string,
  endTime: string
): string {
  return `
INSERT INTO ${tableName} (
  time, submit_time, start_time, end_time, timelimit,
  nhosts, ncores, account, queue, host, jid, unit,
  jobname, exitcode, host_list, username,
  value_cpuuser, value_gpu, value_memused, value_memused_minus_diskcache,
  value_nfs, value_block
)
SELECT
  '${startTime}'::timestamp + (random() * ('${endTime}'::timestamp - '${startTime}'::timestamp)) as time,
  '${startTime}'::timestamp + (random() * ('${endTime}'::timestamp - '${startTime}'::timestamp)) as submit_time,
  '${startTime}'::timestamp + (random() * ('${endTime}'::timestamp - '${startTime}'::timestamp)) as start_time,
  '${startTime}'::timestamp + (random() * ('${endTime}'::timestamp - '${startTime}'::timestamp)) as end_time,
  (random() * 86400)::double as timelimit,
  (random() * 10 + 1)::bigint as nhosts,
  (random() * 32 + 1)::bigint as ncores,
  'demo-account-' || (random() * 100)::int as account,
  'demo-queue-' || (random() * 5)::int as queue,
  'demo-host-' || (random() * 50)::int as host,
  'demo-job-' || generate_series as jid,
  'demo' as unit,
  'demo-job-' || generate_series as jobname,
  CASE WHEN random() < 0.9 THEN '0' ELSE '1' END as exitcode,
  'demo-host-' || (random() * 50)::int as host_list,
  'demo-user-' || (random() * 20)::int as username,
  (random() * 100)::double as value_cpuuser,
  (random() * 100)::double as value_gpu,
  (random() * 16000)::double as value_memused,
  (random() * 12000)::double as value_memused_minus_diskcache,
  (random() * 1000)::double as value_nfs,
  (random() * 1000)::double as value_block
FROM generate_series(1, ${rowCount});
`;
}

/**
 * Common table expressions for complex queries
 */
export const COMMON_CTES = {
  /**
   * CTE for job duration calculation
   */
  JOB_DURATION: `
job_duration AS (
  SELECT *,
    EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds
  FROM {table_name}
  WHERE start_time IS NOT NULL AND end_time IS NOT NULL
)`,

  /**
   * CTE for resource utilization calculation
   */
  RESOURCE_UTILIZATION: `
resource_utilization AS (
  SELECT *,
    (value_cpuuser / NULLIF(ncores, 0)) as cpu_utilization_per_core,
    (value_memused / NULLIF(nhosts, 0)) as memory_per_host
  FROM {table_name}
  WHERE ncores > 0 AND nhosts > 0
)`,

  /**
   * CTE for time-based aggregation
   */
  TIME_BUCKETS: `
time_buckets AS (
  SELECT 
    date_trunc('hour', time) as time_bucket,
    COUNT(*) as job_count,
    AVG(value_cpuuser) as avg_cpu,
    AVG(value_memused) as avg_memory,
    AVG(value_gpu) as avg_gpu
  FROM {table_name}
  GROUP BY date_trunc('hour', time)
  ORDER BY time_bucket
)`
} as const;