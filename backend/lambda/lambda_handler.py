import json
import boto3
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
from botocore.exceptions import ClientError
from botocore.signers import CloudFrontSigner

# Configure logging
logger = logging.getLogger('QueryProcessor')
logger.setLevel(logging.INFO)


def print_available_years(json_data):
    """
    Extract and print the unique years available in the provided JSON data.

    Args:
        json_data (dict): The JSON data containing timestamps in the keys
    """
    years = set()

    for key in json_data.keys():
        # The keys appear to be in format "YYYY-MM-DD-HH"
        parts = key.split('-')
        if len(parts) >= 1 and parts[0].isdigit():
            years.add(parts[0])

    # Convert to sorted list for display
    years_list = sorted(list(years))

    if years_list:
        print("Years available in the data:")
        for year in years_list:
            print(f"- {year}")
    else:
        print("No years found in the data.")


def load_manifest() -> dict:
    """Load manifest.json from the current directory and return as dict."""
    try:
        manifest_file = "manifest_final.json"
        logger.info(f"Loading {manifest_file}")

        with open(manifest_file, 'r') as f:
            manifest = json.loads(f.read())
            # Check if 'lastUpdated' key exists before accessing it
            if "lastUpdated" in manifest:
                logger.info(f"last updated: {manifest['lastUpdated']}")
            else:
                logger.info("No 'lastUpdated' information available in the manifest")
            print_available_years(manifest["chunks"])
        return manifest
    except FileNotFoundError:
        raise FileNotFoundError("manifest.json not found in current directory")
    except json.JSONDecodeError as e:
        raise ValueError(f"Error decoding manifest.json: {str(e)}")


class RequestIdFilter(logging.Filter):
    def __init__(self):
        self.request_id = None

    def filter(self, record):
        record.request_id = self.request_id or 'NO_REQUEST_ID'
        return True


formatter = logging.Formatter('%(asctime)s - %(request_id)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
request_id_filter = RequestIdFilter()
logger.addHandler(handler)
logger.addFilter(request_id_filter)


@dataclass
class ChunkMetadata:
    path: str
    hour_key: str  # Format: YYYY-MM-DD-HH
    time_range: Dict[str, str]
    record_count: int
    size: int


@dataclass
class QueryPlan:
    chunks: List[ChunkMetadata]
    filters: List[Dict]
    projections: List[str]
    estimated_size: int
    partition_count: int


s3_client = boto3.client('s3')


class SQLParser:
    """Simple SQL parser for basic SELECT queries with WHERE clauses"""

    def __init__(self, query: str):
        self.query = query.strip()
        self.tokens = self._tokenize(query)
        logger.info(f"Tokenized query into {len(self.tokens)} tokens")

    def _tokenize(self, query: str) -> List[str]:
        """Split query into tokens, preserving quoted strings"""
        # Replace newlines and extra spaces
        query = ' '.join(query.split())

        # Split on spaces while preserving quoted strings
        tokens = []
        current_token = ''
        in_quotes = False
        quote_char = None

        for char in query:
            if char in ["'", '"']:
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif char == quote_char:
                    in_quotes = False
                current_token += char
            elif char.isspace() and not in_quotes:
                if current_token:
                    tokens.append(current_token)
                    current_token = ''
            else:
                current_token += char

        if current_token:
            tokens.append(current_token)

        return tokens

    def extract_projections(self) -> List[str]:
        """Extract column names from SELECT clause"""
        try:
            select_idx = self.tokens.index('SELECT')
            from_idx = self.tokens.index('FROM')

            projection_str = ' '.join(self.tokens[select_idx + 1:from_idx])
            projections = [col.strip() for col in projection_str.split(',')]
            logger.info(f"Extracted projections: {projections}")
            return projections

        except ValueError:
            logger.error("Invalid SQL: Missing SELECT or FROM clause")
            raise ValueError("Invalid SQL: Missing SELECT or FROM clause")

    def extract_time_range(self) -> Optional[Dict[str, str]]:
        """Extract time range from BETWEEN clause"""
        try:
            where_idx = self.tokens.index('WHERE')
            where_clause = ' '.join(self.tokens[where_idx + 1:])

            # Find BETWEEN clause for timestamp
            between_match = re.search(
                r"time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'",
                where_clause,
                re.IGNORECASE
            )

            if between_match:
                start_time, end_time = between_match.groups()
                logger.info(f"Extracted time range: {start_time} to {end_time}")
                return {'start': start_time, 'end': end_time}

            return None

        except ValueError:
            return None

    def extract_filters(self) -> List[Dict]:
        """Extract all WHERE conditions"""
        filters = []
        try:
            where_idx = self.tokens.index('WHERE')
            where_clause = ' '.join(self.tokens[where_idx + 1:])

            # Split on AND, ignoring ANDs in BETWEEN clauses
            conditions = []
            current_condition = ''
            in_between = False

            for token in where_clause.split():
                if token.upper() == 'BETWEEN':
                    in_between = True
                elif token.upper() == 'AND':
                    if in_between:
                        current_condition += ' ' + token
                        in_between = False
                    else:
                        if current_condition:
                            conditions.append(current_condition.strip())
                            current_condition = ''
                else:
                    current_condition += ' ' + token

            if current_condition:
                conditions.append(current_condition.strip())

            # Create filter objects
            for condition in conditions:
                if 'BETWEEN' not in condition.upper():
                    filters.append({'condition': condition.strip()})
                    logger.info(f"Added filter condition: {condition.strip()}")

            return filters

        except ValueError:
            return []


class QueryProcessor:
    def __init__(self):
        logger.info("Initializing QueryProcessor")
        self.s3 = boto3.client('s3')
        self.BUCKET = 'fresco-data-source'
        self.REGION = 'us-east-1'  # Add region
        self.MAX_CONCURRENT_CHUNKS = 4
        self.MAX_RESPONSE_TIME = 60
        self.CHUNK_SIZE_TARGET = 50 * 1024 * 1024
        logger.info(f"Configuration: BUCKET={self.BUCKET}, MAX_CONCURRENT_CHUNKS={self.MAX_CONCURRENT_CHUNKS}")

    def get_hour_key(self, timestamp: datetime) -> str:
        """Generate consistent hour-based key for chunks"""
        hour_key = timestamp.strftime('%Y-%m-%d-%H')
        logger.info(f"Generated hour key: {hour_key} for timestamp {timestamp}")
        return hour_key

    def get_chunk_path(self, hour_key: str) -> str:
        """Generate S3 path for a given hour chunk"""
        # Parse the hour_key (format: YYYY-MM-DD-HH)
        parts = hour_key.split('-')
        year, month, day, hour = parts

        # Construct path in the correct format: chunks/YYYY/MM/DD/HH.parquet
        path = f'chunks/{year}/{month}/{day}/{hour}.parquet'
        logger.info(f"Generated chunk path: {path}")
        return path

    def generate_public_urls(self, chunks: List[ChunkMetadata]) -> List[Dict]:
        """Generate public S3 URLs for chunk access"""
        logger.info(f"Generating public S3 URLs for {len(chunks)} chunks")
        urls = []

        try:
            for chunk in chunks:
                # Get a new presigned URL with the current credentials
                logger.info(f"File path: {chunk.path}")
                presigned_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': self.BUCKET,
                        'Key': chunk.path
                    },
                    ExpiresIn=3600,  # URL expires in 1 hour
                    HttpMethod='GET'
                )

                urls.append({
                    'url': presigned_url,
                    'hourKey': chunk.hour_key,
                    'timeRange': chunk.time_range,
                    'size': chunk.size
                })
                logger.info(f"Generated presigned URL for chunk {chunk.hour_key}")

            logger.info(f"Generated {len(urls)} URLs")
            return urls

        except Exception as e:
            logger.error(f"Error generating URLs: {str(e)}")
            raise

    def parse_sql(self, sql: str) -> Tuple[List[str], List[Dict], Dict]:
        """Parse SQL query to extract projections, filters, and time range"""
        logger.info(f"Parsing SQL query: {sql}")
        parser = SQLParser(sql)

        projections = parser.extract_projections()
        time_range = parser.extract_time_range()
        filters = parser.extract_filters()

        if not time_range:
            logger.error("Missing required time range in query")
            raise ValueError("Query must include a timestamp BETWEEN clause")

        logger.info(f"SQL parsing complete. Projections: {len(projections)}, "
                    f"Filters: {len(filters)}, Time range: {time_range}")
        return projections, filters, time_range

    def get_required_hours(self, start_time: datetime, end_time: datetime) -> Set[str]:
        """Get set of hour keys needed to cover the time range"""
        logger.info(f"Calculating required hours between {start_time} and {end_time}")
        required_hours = set()
        current = start_time.replace(minute=0, second=0, microsecond=0)

        while current <= end_time:
            required_hours.add(self.get_hour_key(current))
            current += timedelta(hours=1)

        logger.info(f"Found {len(required_hours)} required hour chunks")
        return required_hours

    # In class QueryProcessor:

    def get_relevant_chunks(self, manifest: Dict, time_range: Dict) -> List[ChunkMetadata]:
        logger.info(f"Finding relevant chunks using manifest for time range: {time_range}")
        start_time = datetime.fromisoformat(time_range['start'])
        end_time = datetime.fromisoformat(time_range['end'])

        required_hours_set = self.get_required_hours(start_time, end_time)  # Set of 'YYYY-MM-DD-HH'
        relevant_chunks: List[ChunkMetadata] = []
        total_size_bytes = 0

        # Iterate through the required hours, ensuring a sorted order for deterministic behavior if needed
        for hour_key in sorted(list(required_hours_set)):
            # Get the specific chunk's information from the manifest
            # Based on your example, manifest['chunks'][hour_key] is a dictionary
            chunk_info = manifest['chunks'].get(hour_key)

            if chunk_info:
                if 'path' not in chunk_info:
                    logger.warning(f"Manifest entry for {hour_key} is missing the 'path' key. Skipping.")
                    continue

                # Ensure other necessary fields are present, using .get() for safety with defaults
                path_from_manifest = chunk_info['path']
                time_range_from_manifest = chunk_info.get('timeRange')
                record_count_from_manifest = chunk_info.get('recordCount', 0)  # Default to 0 if missing
                size_bytes_from_manifest = chunk_info.get('sizeBytes', 0)  # Default to 0 if missing

                if not time_range_from_manifest:
                    logger.warning(
                        f"Manifest entry for {hour_key} (Path: {path_from_manifest}) is missing 'timeRange'. Skipping.")
                    # Alternatively, you could define a default time range for the whole hour,
                    # but skipping is safer if timeRange is critical.
                    # Example default:
                    # dt_hour_start = datetime.strptime(hour_key, '%Y-%m-%d-%H')
                    # time_range_from_manifest = {
                    #     'start': dt_hour_start.isoformat(),
                    #     'end': (dt_hour_start + timedelta(hours=1) - timedelta(microseconds=1)).isoformat()
                    # }
                    continue  # Skipping if timeRange is missing

                metadata = ChunkMetadata(
                    path=path_from_manifest,  # Use the path directly from the manifest
                    hour_key=hour_key,
                    time_range=time_range_from_manifest,
                    record_count=record_count_from_manifest,
                    size=size_bytes_from_manifest
                )
                relevant_chunks.append(metadata)
                total_size_bytes += size_bytes_from_manifest
                logger.info(
                    f"Added chunk from manifest: {path_from_manifest} for hour {hour_key}, size={size_bytes_from_manifest} bytes")
            else:
                logger.info(f"No manifest entry found for required hour_key: {hour_key}")

        if not relevant_chunks:
            logger.error(
                f"No data found in manifest for the specified time range: {time_range['start']} to {time_range['end']}")
            raise Exception(f"No data found for time range {time_range['start']} to {time_range['end']}")

        logger.info(
            f"Collected {len(relevant_chunks)} chunks from manifest. Total estimated size: {total_size_bytes / (1024 * 1024):.2f} MB.")
        return relevant_chunks

    def optimize_chunk_distribution(self, chunks: List[ChunkMetadata]) -> int:
        """Determine optimal number of parallel downloads based on chunk sizes"""
        total_size = sum(chunk.size for chunk in chunks)
        logger.info(f"Optimizing chunk distribution for {len(chunks)} chunks, "
                    f"total size: {total_size / 1024 / 1024:.2f}MB")

        if total_size < self.CHUNK_SIZE_TARGET:
            logger.info("Total size below target, using single partition")
            return 1

        partition_count = min(
            self.MAX_CONCURRENT_CHUNKS,
            len(chunks),
            max(1, round(total_size / self.CHUNK_SIZE_TARGET))
        )

        logger.info(f"Determined optimal partition count: {partition_count}")
        return partition_count

    def create_query_plan(self, sql: str, manifest: Dict) -> QueryPlan:
        """Create execution plan based on SQL query and manifest"""
        logger.info("Creating query plan")
        projections, filters, time_range = self.parse_sql(sql)

        chunks = self.get_relevant_chunks(manifest, time_range)
        chunks.sort(key=lambda x: x.hour_key)

        partition_count = self.optimize_chunk_distribution(chunks)
        estimated_size = sum(chunk.size for chunk in chunks)

        plan = QueryPlan(
            chunks=chunks,
            filters=filters,
            projections=projections,
            estimated_size=estimated_size,
            partition_count=partition_count
        )

        logger.info(f"Query plan created: {len(chunks)} chunks, {partition_count} partitions")
        return plan

    def generate_presigned_urls(self, chunks: List[ChunkMetadata]) -> List[Dict]:
        """Generate signed URLs for chunk access through CloudFront"""
        logger.info(f"Generating CloudFront signed URLs for {len(chunks)} chunks")
        urls = []

        # Create timezone-aware datetime
        from zoneinfo import ZoneInfo
        expiration = datetime.now(ZoneInfo("UTC")) + timedelta(minutes=15)

        # Get private key from Secrets Manager
        try:
            secret_name = "CLOUDFRONT_PRIVATE_KEY"
            region_name = "us-east-1"

            session = boto3.session.Session()
            secrets_client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )

            logger.info("Retrieving CloudFront private key from Secrets Manager")
            secret_response = secrets_client.get_secret_value(
                SecretId=secret_name
            )
            private_key = secret_response['SecretString']
            logger.info("Successfully retrieved private key from Secrets Manager")

        except ClientError as e:
            logger.error(f"Error retrieving secret: {str(e)}")
            raise

        try:
            # Create CloudFront signer using the imported CloudFrontSigner class
            signer = CloudFrontSigner(
                self.CLOUDFRONT_KEY_ID,
                lambda message: private_key.encode('ascii')
            )
            logger.info("Created CloudFront signer")

            for chunk in chunks:
                # Remove any leading https:// from domain if present
                domain = self.CLOUDFRONT_DOMAIN.replace('https://', '')

                # Construct the CloudFront URL
                cloudfront_url = f'https://{domain}/{chunk.path}'

                # Generate signed URL
                signed_url = signer.generate_presigned_url(
                    cloudfront_url,
                    date_less_than=expiration
                )

                urls.append({
                    'url': signed_url,
                    'hourKey': chunk.hour_key,
                    'timeRange': chunk.time_range,
                    'size': chunk.size
                })
                logger.info(f"Generated signed CloudFront URL for chunk {chunk.hour_key}")

            logger.info(f"Generated {len(urls)} signed URLs, expiring at {expiration}")
            return urls

        except Exception as e:
            logger.error(f"Error generating signed URLs: {str(e)}")
            raise


def lambda_handler(event, context):
    logger.info(F"Incoming event: {event}")

    try:
        logger.info("Loading manifest")
        manifest = load_manifest()
        sql_query = event['query']
        logger.info(f"Received SQL query: {sql_query}")

        # Special case handler for timestamps query
        if sql_query.strip().upper() == "SELECT TIME FROM JOB_DATA":
            timestamp_file_url = s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': 'fresco-data-source',
                    'Key': 'timestamps/job_timestamps_20241219_225746.parquet'
                },
                ExpiresIn=3600
            )

            response = {
                'statusCode': 200,
                'body': json.dumps({
                    'transferId': context.aws_request_id,
                    'metadata': {
                        'total_partitions': 1,
                        'estimated_size': 0,  # Size unknown
                        'chunk_count': 1,
                        'hour_count': 1
                    },
                    'chunks': [{
                        'url': timestamp_file_url,
                        'hourKey': '2024-12-19-22',
                        'timeRange': {
                            'start': '2024-12-19T22:57:46',
                            'end': '2024-12-19T22:57:46'
                        },
                        'size': 0  # Size unknown
                    }],
                    'queryPlan': {
                        'projections': ['time'],
                        'filters': []
                    }
                })
            }

            logger.info(f"Request {context.aws_request_id} completed successfully (timestamp special case)")
            return response

        # Regular query processing
        processor = QueryProcessor()
        plan = processor.create_query_plan(sql_query, manifest)
        chunk_urls = processor.generate_public_urls(plan.chunks)

        response = {
            'statusCode': 200,
            'body': json.dumps({
                'transferId': context.aws_request_id,
                'metadata': {
                    'total_partitions': plan.partition_count,
                    'estimated_size': plan.estimated_size,
                    'chunk_count': len(chunk_urls),
                    'hour_count': len(set(chunk.hour_key for chunk in plan.chunks))
                },
                'chunks': chunk_urls,
                'queryPlan': {
                    'projections': plan.projections,
                    'filters': plan.filters
                }
            })
        }

        logger.info(f"Request {context.aws_request_id} completed successfully")
        return response

    except Exception as e:
        logger.error(f"Error processing request: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }
