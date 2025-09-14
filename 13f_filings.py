"""
13F Filings ETL v2.0

This script extracts 13F filings from SEC EDGAR using index files and loads them into TimescaleDB and S3.
It follows SEC's fair access guidelines and uses proper request headers.

Features:
- Uses SEC EDGAR index files for comprehensive filing discovery
- Proper SEC request headers for fair access
- Rate limiting (10 requests/second max)
- Comprehensive logging and error handling
- S3 upload for raw documents
- TimescaleDB storage for structured data
- Configurable date ranges
- Detailed progress tracking

Usage:
    # Default: Load recent 13F filings
    python 13f_filings_etl_v2.py
    
    # Custom date range
    python 13f_filings_etl_v2.py --start-date 2024-01-01 --end-date 2024-01-31
    
    # Single date
    python 13f_filings_etl_v2.py --start-date 2024-01-15 --end-date 2024-01-15
"""

import os
import sys
import json
import time
import asyncio
import argparse
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import asyncpg
import aiohttp
import boto3
from botocore.exceptions import ClientError
import dotenv
from contextlib import asynccontextmanager
import xml.etree.ElementTree as ET
import re
import gzip
import io
import ssl
import certifi

# Load environment variables
dotenv.load_dotenv()

# Configuration
TSDB_USERNAME = os.getenv("TSDB_USERNAME")
TSDB_PASSWORD = os.getenv("TSDB_PASSWORD")
TSDB_HOST = os.getenv("TSDB_HOST")
TSDB_PORT = os.getenv("TSDB_PORT")
TSDB_DATABASE = os.getenv("TSDB_DATABASE")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Database connection setup
TIMESCALE_DB_URL = f"postgres://{TSDB_USERNAME}:{TSDB_PASSWORD}@{TSDB_HOST}:{TSDB_PORT}/{TSDB_DATABASE}"

# SEC Configuration
SEC_BASE_URL = "https://www.sec.gov"
SEC_ARCHIVES_URL = "https://www.sec.gov/Archives/edgar"
SEC_ARCHIVES_URL_DATA = "https://www.sec.gov/Archives"

# Rate limiting (SEC allows 10 requests/second)
MAX_REQUESTS_PER_SECOND = 10
RATE_LIMIT_DELAY = 1.0 / MAX_REQUESTS_PER_SECOND

# Request headers as per SEC documentation - using browser-like headers
SEC_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Host': 'www.sec.gov',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1'
}

# ETL Configuration
BATCH_SIZE = 100
MAX_RETRIES = 3
RETRY_DELAY = 1.0

@dataclass
class ETLMetrics:
    """Track ETL execution metrics"""
    records_processed: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_failed: int = 0
    api_calls_made: int = 0
    rate_limit_hits: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None

class CloudWatchLogger:
    """Logger for the ETL process"""
    
    def __init__(self):
        self.logger = logging.getLogger("13f-etl-v2")
        
        # In AWS Lambda, the root logger is already configured.
        # We can't use logging.basicConfig(), but we can set the level.
        # This ensures our INFO level logs are captured.
        logging.getLogger().setLevel(logging.INFO)
        
        # We will rely on Lambda's default behavior of capturing stdout/stderr
        # and sending it to CloudWatch logs for the function.
        # The custom boto3 client logic is removed as it's not necessary.

    def info(self, message: str):
        """Log info message"""
        self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message"""
        self.logger.warning(message)
    
    def error(self, message: str):
        """Log error message"""
        self.logger.error(message)
    
    def critical(self, message: str):
        """Log critical message"""
        self.logger.critical(message)
    
    def debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)


class SECIndexClient:
    """Client for accessing SEC EDGAR index files"""
    
    def __init__(self, logger: CloudWatchLogger):
        self.logger = logger
        self.session = None
        self.request_count = 0
        self.last_request_time = 0
    
    async def __aenter__(self):
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        self.session = aiohttp.ClientSession(headers=SEC_HEADERS, connector=connector)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def _rate_limit(self):
        """Ensure we don't exceed SEC's rate limit"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - time_since_last
            self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f} seconds")
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
        self.logger.debug(f"Request count: {self.request_count}")
    
    async def get_index_files(self, start_date: date, end_date: date) -> List[str]:
        """Get list of index files to process for the date range"""
        try:
            self.logger.info(f"Getting index files for date range: {start_date} to {end_date}")
            
            index_files = []
            current_date = start_date
            
            while current_date <= end_date:
                # Daily index files
                daily_index_url = f"{SEC_ARCHIVES_URL}/daily-index/{current_date.strftime('%Y')}/QTR{((current_date.month-1)//3)+1}/master.{current_date.strftime('%Y%m%d')}.idx"
                #self.logger.info(f"Daily index URL: {daily_index_url}")
                
                # Check if daily index exists
                try:
                    self.logger.info(f"Making request to: {daily_index_url}")
                    await self._rate_limit()
                    async with self.session.get(daily_index_url) as response:
                        self.logger.info(f"Response status: {response.status}")
                        if response.status == 200:
                            index_files.append(daily_index_url)
                            self.logger.info(f"Found daily index: {daily_index_url}")
                        else:
                            self.logger.info(f"Daily index returned HTTP {response.status} for {current_date}")
                except Exception as e:
                    self.logger.info(f"Daily index request failed for {current_date}: {e}")
                
                # Skip quarterly index files - only process daily files
                # quarter = ((current_date.month-1)//3)+1
                # quarterly_index_url = f"{SEC_ARCHIVES_URL}/full-index/{current_date.year}/QTR{quarter}/master.idx"
                # 
                # if quarterly_index_url not in index_files:
                #     try:
                #         await self._rate_limit()
                #         async with self.session.get(quarterly_index_url) as response:
                #             if response.status == 200:
                #                 index_files.append(quarterly_index_url)
                #                 self.logger.info(f"Found quarterly index: {quarterly_index_url}")
                #             else:
                #                 self.logger.debug(f"Quarterly index returned HTTP {response.status} for Q{quarter} {current_date.year}")
                #     except Exception as e:
                #         self.logger.debug(f"Quarterly index not found for Q{quarter} {current_date.year}: {e}")
                
                #self.logger.info(f"Current date: {current_date}")
                current_date += timedelta(days=1)
            
            self.logger.info(f"Found {len(index_files)} index files to process")
            return list(set(index_files))  # Remove duplicates
            
        except Exception as e:
            self.logger.error(f"Error getting index files: {str(e)}")
            return []
    
    async def parse_index_file(self, index_url: str) -> List[Dict[str, Any]]:
        """Parse an SEC index file to extract 13F filings"""
        try:
            self.logger.info(f"Parsing index file: {index_url}")
            self.logger.info(f"Index file format: CIK|Company Name|Form Type|Date|Filename (pipe-delimited)")
            
            await self._rate_limit()
            async with self.session.get(index_url) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to fetch index file {index_url}: HTTP {response.status}")
                    return []
                
                content = await response.text()
                
                # Parse the index file
                filings = []
                lines = content.split('\n')
                #print number of lines in the file and the file name in one log line
                self.logger.info(f"Number of lines in {index_url}: {len(lines)}")

                
                for line in lines:
                    if line.strip() and not line.startswith('---') and not line.startswith('CIK'):
                                                # Parse index line format: CIK|Company Name|Form Type|Date|Filename
                        # The file is pipe-delimited, not space-delimited
                        parts = line.strip().split('|')
                        
                        #self.logger.info(f"Parts: {parts}")
                        
                        if len(parts) >= 5:
                            cik = parts[0].strip()
                            company_name = parts[1].strip()
                            form_type = parts[2].strip()
                            date_filed = parts[3].strip()
                            filename = parts[4].strip()
                            

                            #print form type
                            #self.logger.info(f"Form type: {form_type}")

                            # Filter for 13F filings
                            if form_type in ['13F-HR', '13F-HR/A', '13F-NT', '13F-NT/A']:
                                try:
                                    # Convert YYYY-MM-DD format to date
                                    filing_date = datetime.strptime(date_filed, '%Y%m%d').date()
                                    filings.append({
                                        'cik': cik,
                                        'company_name': company_name,
                                        'form_type': form_type,
                                        'filing_date': filing_date,
                                        'filename': filename,
                                        'index_url': index_url
                                    })
                                except ValueError as e:
                                    self.logger.warning(f"Invalid date format in index: {date_filed}")
                                    continue
                
                self.logger.info(f"Found {len(filings)} 13F filings in index file")
                return filings
                
        except Exception as e:
            self.logger.error(f"Error parsing index file {index_url}: {str(e)}")
            return []
    
    async def get_filing_document(self, filename: str) -> Optional[Dict[str, Any]]:
        """Fetch the actual filing document from SEC EDGAR"""
        try:
            document_url = f"{SEC_ARCHIVES_URL_DATA}/{filename}"
            self.logger.debug(f"Fetching document: {document_url}")
            
            await self._rate_limit()
            async with self.session.get(document_url) as response:
                if response.status == 200:
                    content = await response.text()
                    
                    return {
                        'content': content,
                        'content_length': len(content),
                        'url': document_url,
                        'success': True
                    }
                else:
                    self.logger.warning(f"Failed to fetch document {document_url}: HTTP {response.status}")
                    return None
                    
        except Exception as e:
            self.logger.error(f"Error fetching document {filename}: {str(e)}")
            return None

class S3Manager:
    """Manages S3 operations for storing 13F data"""
    
    def __init__(self, bucket_name: str, region: str, logger: CloudWatchLogger):
        self.bucket_name = bucket_name
        self.region = region
        self.logger = logger
        self.s3_client = boto3.client(
            's3',
            region_name=region
        )
    
    def upload_13f_document(self, content: str, filing_date: date, cik: str, form_type: str) -> bool:
        """Upload 13F document to S3"""
        try:
            # Create structured key
            year = filing_date.year
            month = filing_date.month
            key = f"13f_filings/{year}/{month:02d}/{cik}_{filing_date}_{form_type}.txt"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=content,
                ContentType='text/plain'
            )
            
            self.logger.info(f"Successfully uploaded 13F document to S3: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload to S3: {str(e)}")
            return False
    
    def upload_etl_metrics(self, metrics: ETLMetrics, run_date: str) -> bool:
        """Upload ETL metrics to S3"""
        try:
            key = f"etl_metrics/13f-v2/{run_date}/metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            metrics_data = {
                "run_date": run_date,
                "start_time": metrics.start_time.isoformat() if metrics.start_time else None,
                "end_time": metrics.end_time.isoformat() if metrics.end_time else None,
                "records_processed": metrics.records_processed,
                "records_inserted": metrics.records_inserted,
                "records_updated": metrics.records_updated,
                "records_failed": metrics.records_failed,
                "api_calls_made": metrics.api_calls_made,
                "rate_limit_hits": metrics.rate_limit_hits,
                "upload_timestamp": datetime.now().isoformat()
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json.dumps(metrics_data, indent=2),
                ContentType='application/json'
            )
            
            self.logger.info(f"Successfully uploaded ETL metrics to S3: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload ETL metrics to S3: {str(e)}")
            return False

class DatabaseManager:
    """Manages database connections and operations for 13F data"""
    
    def __init__(self, db_url: str, logger: CloudWatchLogger):
        self.db_url = db_url
        self.logger = logger
        self.pool = None
    
    async def __aenter__(self):
        self.pool = await asyncpg.create_pool(
            self.db_url,
            statement_cache_size=0
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.pool:
            await self.pool.close()
    
    async def create_etl_run(self, run_date: date) -> int:
        """Create a new ETL run record and return its ID"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO etl_runs_13f (run_date, start_time, status)
                VALUES ($1, $2, 'running')
                RETURNING id
            """, run_date, datetime.now())
            return row['id']
    
    async def update_etl_run(self, run_id: int, **kwargs):
        """Update ETL run record"""
        async with self.pool.acquire() as conn:
            set_clauses = []
            values = [run_id]
            param_count = 1
            
            for key, value in kwargs.items():
                param_count += 1
                set_clauses.append(f"{key} = ${param_count}")
                values.append(value)
            
            query = f"""
                UPDATE etl_runs_13f 
                SET {', '.join(set_clauses)}
                WHERE id = $1
            """
            await conn.execute(query, *values)
    
    async def insert_13f_filing(self, filing_data: Dict[str, Any], document_data: Optional[Dict[str, Any]] = None) -> bool:
        """Insert or update a 13F filing record"""
        try:
            async with self.pool.acquire() as conn:
                # Extract data
                cik = filing_data['cik']
                company_name = filing_data['company_name']
                form_type = filing_data['form_type']
                filing_date = filing_data['filing_date']
                filename = filing_data['filename']
                
                # Document metadata
                document_metadata = {}
                if document_data:
                    document_metadata = {
                        'content_length': document_data.get('content_length', 0),
                        'url': document_data.get('url', ''),
                        'success': document_data.get('success', False)
                    }
                
                # Insert into filings_13f table
                result = await conn.fetchrow("""
                    INSERT INTO filings_13f (
                        cik, company_name, form_type, filing_date, 
                        accession_number, source_filing_url,
                        holdings_data, total_value, total_shares, source_api
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (cik, filing_date, form_type)
                    DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        source_filing_url = EXCLUDED.source_filing_url,
                        holdings_data = EXCLUDED.holdings_data,
                        source_api = EXCLUDED.source_api,
                        updated_at = NOW()
                    RETURNING id
                """, 
                cik,
                company_name,
                form_type,
                filing_date,
                filename,  # Use filename as accession number
                document_data.get('url') if document_data else None,
                json.dumps(document_metadata),
                0,  # total_value - will be calculated later
                0,  # total_shares - will be calculated later
                'SEC_EDGAR_INDEX'
                )
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to insert 13F filing: {str(e)}")
            return False
    
    async def log_failure(self, etl_run_id: int, filing_data: Dict[str, Any], error_message: str):
        """Log a failed record"""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO etl_failures_13f 
                (etl_run_id, cik, company_name, form_type, filing_date, error_message, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, 
            etl_run_id,
            filing_data.get('cik'),
            filing_data.get('company_name'),
            filing_data.get('form_type'),
            filing_data.get('filing_date'),
            error_message,
            json.dumps(filing_data)
            )

class ThirteenFETLV2:
    """Main ETL class for processing 13F filings using SEC index files"""
    
    def __init__(self, start_date: Optional[date] = None, end_date: Optional[date] = None, 
                 enable_s3: bool = True):
        self.start_date = start_date or (datetime.now() - timedelta(days=30)).date()
        self.end_date = end_date or (datetime.now()).date()
        self.enable_s3 = enable_s3
        self.logger = CloudWatchLogger()
        self.metrics = ETLMetrics()
        
        # Validate configuration
        if not all([TSDB_USERNAME, TSDB_PASSWORD, TSDB_HOST, TSDB_PORT, TSDB_DATABASE]):
            raise ValueError("All TimescaleDB environment variables are required")
        
        # Initialize S3 manager if enabled thi
        self.s3_manager = None
        if self.enable_s3:
            if not S3_BUCKET_NAME:
                self.logger.warning("S3_BUCKET_NAME not set, S3 uploads will be disabled")
                self.enable_s3 = False
            else:
                self.s3_manager = S3Manager(S3_BUCKET_NAME, AWS_REGION, self.logger)
    
    async def run(self):
        """Execute the ETL pipeline"""
        self.metrics.start_time = datetime.now()
        self.logger.info(f"Starting 13F filings ETL v2 for {self.start_date} to {self.end_date}")
        
        try:
            async with DatabaseManager(TIMESCALE_DB_URL, self.logger) as db:
                # Create ETL run record
                etl_run_id = await db.create_etl_run(self.start_date)
                self.logger.info(f"Created ETL run with ID: {etl_run_id}")
                
                # Initialize SEC index client
                async with SECIndexClient(self.logger) as sec_client:
                    await self._process_13f_filings(sec_client, db, etl_run_id)
                
                # Update ETL run as completed
                await db.update_etl_run(
                    etl_run_id,
                    status='completed',
                    end_time=datetime.now(),
                    records_processed=self.metrics.records_processed,
                    records_inserted=self.metrics.records_inserted,
                    records_updated=self.metrics.records_updated,
                    records_failed=self.metrics.records_failed,
                    api_calls_made=self.metrics.api_calls_made,
                    api_rate_limit_hits=self.metrics.rate_limit_hits
                )
                
                self.metrics.end_time = datetime.now()
                duration = (self.metrics.end_time - self.metrics.start_time).total_seconds()
                
                # Upload metrics to S3 if enabled
                if self.enable_s3 and self.s3_manager:
                    run_date_str = self.start_date.strftime('%Y-%m-%d')
                    self.s3_manager.upload_etl_metrics(self.metrics, run_date_str)
                
                self.logger.info(f"ETL completed successfully in {duration:.2f} seconds")
                self.logger.info(f"Metrics: {self.metrics.records_processed} processed, "
                               f"{self.metrics.records_inserted} inserted, "
                               f"{self.metrics.records_failed} failed")
                
        except Exception as e:
            self.logger.error(f"ETL failed: {str(e)}")
            if 'etl_run_id' in locals():
                await db.update_etl_run(etl_run_id, status='failed', error_message=str(e))
            raise
    
    async def _process_13f_filings(self, sec_client: SECIndexClient, db: DatabaseManager, etl_run_id: int):
        """Process 13F filings for the date range"""
        
        self.logger.info(f"Processing 13F filings from {self.start_date} to {self.end_date}")
        
        # Get index files to process
        index_files = await sec_client.get_index_files(self.start_date, self.end_date)
        
        if not index_files:
            self.logger.warning("No index files found for the specified date range")
            return
        
        # Process each index file
        for index_url in index_files:
            try:
                self.logger.info(f"Processing index file: {index_url}")
                
                # Parse index file to get 13F filings
                filings = await sec_client.parse_index_file(index_url)
                
                if not filings:
                    self.logger.info(f"No 13F filings found in {index_url}")
                    continue
                
                # Process each filing
                for filing in filings:
                    self.metrics.records_processed += 1
                    
                    try:
                        # Fetch the actual document
                        document_data = await sec_client.get_filing_document(filing['filename'])
                        
                        # Upload to S3 if enabled
                        if self.enable_s3 and self.s3_manager and document_data:
                            self.s3_manager.upload_13f_document(
                                document_data['content'],
                                filing['filing_date'],
                                filing['cik'],
                                filing['form_type']
                            )
                        
                        # Insert into database
                        success = await db.insert_13f_filing(filing, document_data)
                        
                        if success:
                            self.metrics.records_inserted += 1
                            self.logger.info(f"Successfully processed 13F filing for {filing['company_name']} "
                                           f"(CIK: {filing['cik']}, Form: {filing['form_type']})")
                        else:
                            self.metrics.records_failed += 1
                            await db.log_failure(etl_run_id, filing, "Database insertion failed")
                            
                    except Exception as e:
                        self.metrics.records_failed += 1
                        error_msg = f"Processing error: {str(e)}"
                        self.logger.error(f"{error_msg} for filing: {filing.get('cik', 'unknown')}")
                        await db.log_failure(etl_run_id, filing, error_msg)
                    
                    # Rate limiting between filings
                    await asyncio.sleep(RATE_LIMIT_DELAY)
                
            except Exception as e:
                self.logger.error(f"Error processing index file {index_url}: {str(e)}")
                continue

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='13F Filings ETL v2.0')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--no-s3', action='store_true', help='Disable S3 uploads')
    
    return parser.parse_args()

async def main(start_date_override: Optional[date] = None, end_date_override: Optional[date] = None, enable_s3_override: bool = True):
    """Main entry point"""
    
    start_date, end_date, enable_s3 = None, None, enable_s3_override
    
    if start_date_override and end_date_override:
        start_date = start_date_override
        end_date = end_date_override
    else:
        args = parse_arguments()
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        enable_s3 = not args.no_s3

    # Default to yesterday if no dates are provided
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).date()
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).date()

    # Create and run ETL
    etl = ThirteenFETLV2(start_date, end_date, enable_s3)
    await etl.run()

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    This function is the entry point for the Lambda execution.
    It runs the ETL for the previous day.
    """
    # By default, process filings for the previous day
    yesterday = datetime.now() - timedelta(days=1)
    
    # Run the main async function
    return asyncio.run(main(start_date_override=yesterday.date(), end_date_override=yesterday.date()))


if __name__ == "__main__":
    asyncio.run(main()) 