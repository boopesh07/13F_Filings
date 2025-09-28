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
from lxml import etree
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from pandas import read_html

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

class AppLogger:
    """Logger for the ETL process, with file output for local runs."""
    
    def __init__(self):
        self.logger = logging.getLogger("13f-etl-v2")
        self.logger.setLevel(logging.INFO)
        
        # Prevent duplicate logs by clearing existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Formatter for all handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Console handler (for local TTY and CloudWatch via stdout)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        self.logger.addHandler(stream_handler)
        
        # File handler for local execution (not in Lambda)
        if os.getenv('AWS_LAMBDA_FUNCTION_NAME') is None:
            log_dir = 'logs'
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            log_file = f"{log_dir}/13f_etl_run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.info(f"Logging to console and file: {log_file}")

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
    
    def __init__(self, logger: AppLogger, metrics: ETLMetrics):
        self.logger = logger
        self.metrics = metrics
        self.session = None
        self.request_count = 0
        self.last_request_time = 0
        self._rate_limit_lock = asyncio.Lock()
    
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
        async with self._rate_limit_lock:
            current_time = time.time()
            time_since_last = current_time - self.last_request_time
            
            if time_since_last < RATE_LIMIT_DELAY:
                sleep_time = RATE_LIMIT_DELAY - time_since_last
                self.logger.debug(f"Rate limiting: sleeping for {sleep_time:.3f} seconds")
                await asyncio.sleep(sleep_time)
            
            self.last_request_time = time.time()
            self.request_count += 1
            self.logger.debug(f"Request count: {self.request_count}")
    
    async def _get_with_retries(self, url: str) -> Optional[aiohttp.ClientResponse]:
        """Make a GET request with proactive rate limiting and reactive retries."""
        await self._rate_limit()
        
        last_exception = None
        for attempt in range(MAX_RETRIES):
            try:
                response = await self.session.get(url)

                if response.status == 429:
                    self.metrics.rate_limit_hits += 1
                    self.logger.warning(f"Rate limit hit (429) for {url}. Retrying ({attempt + 1}/{MAX_RETRIES})...")
                    await response.release()
                    delay = RETRY_DELAY * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                
                return response
                
            except aiohttp.ClientError as e:
                last_exception = e
                self.logger.warning(f"Request for {url} failed with ClientError: {e}. Retrying ({attempt + 1}/{MAX_RETRIES})...")
                delay = RETRY_DELAY * (2 ** attempt)
                await asyncio.sleep(delay)
                
        self.logger.error(f"All retries failed for {url}. Last exception: {last_exception}")
        return None

    async def get_index_files(self, start_date: date, end_date: date) -> List[str]:
        """Get list of index files to process for the date range"""
        try:
            self.logger.info(f"Getting index files for date range: {start_date} to {end_date}")
            
            index_files = []
            current_date = start_date
            
            while current_date <= end_date:
                # Daily index files
                daily_index_url = f"{SEC_ARCHIVES_URL}/daily-index/{current_date.strftime('%Y')}/QTR{((current_date.month-1)//3)+1}/master.{current_date.strftime('%Y%m%d')}.idx"
                
                # Check if daily index exists
                try:
                    self.logger.info(f"Making request to: {daily_index_url}")
                    response = await self._get_with_retries(daily_index_url)
                    
                    if response:
                        async with response:
                            self.logger.info(f"Response status: {response.status}")
                            if response.status == 200:
                                index_files.append(daily_index_url)
                                self.logger.info(f"Found daily index: {daily_index_url}")
                            else:
                                self.logger.info(f"Daily index returned HTTP {response.status} for {current_date}")
                except Exception as e:
                    self.logger.info(f"Daily index request failed for {current_date}: {e}")
                
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
            
            response = await self._get_with_retries(index_url)
            if not response:
                return []

            async with response:
                if response.status != 200:
                    self.logger.error(f"Failed to fetch index file {index_url}: HTTP {response.status}")
                    return []
                
                content = await response.text()
                
                filings = []
                lines = content.split('\n')
                self.logger.info(f"Number of lines in {index_url}: {len(lines)}")

                
                for line in lines:
                    if line.strip() and not line.startswith('---') and not line.startswith('CIK'):
                        parts = line.strip().split('|')
                        
                        if len(parts) >= 5:
                            cik = parts[0].strip()
                            company_name = parts[1].strip()
                            form_type = parts[2].strip()
                            date_filed = parts[3].strip()
                            filename = parts[4].strip()

                            if form_type in ['13F-HR', '13F-HR/A', '13F-NT', '13F-NT/A']:
                                try:
                                    filing_date = datetime.strptime(date_filed, '%Y%m%d').date()
                                    filings.append({
                                        'cik': cik,
                                        'company_name': company_name,
                                        'form_type': form_type,
                                        'filing_date': filing_date,
                                        'filename': filename,
                                        'index_url': index_url
                                    })
                                except ValueError:
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
            
            response = await self._get_with_retries(document_url)
            if not response:
                return None

            async with response:
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

    async def get_filing_xml_urls(self, filename: str) -> Dict[str, str]:
        """Parse the filing's index page to find the XML file URLs."""
        try:
            parts = filename.split('/')
            cik = parts[2]
            accession_num_txt = parts[3]
            accession_num = accession_num_txt.replace('.txt', '')

            filing_base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_num.replace('-', '')}"
            filing_index_url = f"{filing_base_url}/{accession_num}-index.html"
            
            self.logger.info(f"Accessing filing index: {filing_index_url}")
            
            urls = {}
            response = await self._get_with_retries(filing_index_url)
            if not response:
                return {}

            async with response:
                if response.status != 200:
                    self.logger.error(f"Failed to fetch filing index {filing_index_url}: HTTP {response.status}")
                    return {}
                
                content = await response.text()
                
                xml_links = re.findall(r'<a href="([^"]+\.xml)"', content)
                self.logger.debug(f"Found relative XML links: {xml_links}")

                info_table_url_part = None
                primary_doc_url_part = None
                
                for link in xml_links:
                    link_lower = link.lower()
                    if 'infotable.xml' in link_lower or 'form13finfotable.xml' in link_lower:
                        info_table_url_part = link
                    elif 'primary_doc.xml' in link_lower:
                        primary_doc_url_part = link

                if not primary_doc_url_part and xml_links:
                    primary_doc_url_part = xml_links[0]
                
                if primary_doc_url_part:
                    urls['primary_doc_xml'] = f"{filing_base_url}/{primary_doc_url_part.split('/')[-1]}"
                if info_table_url_part:
                    urls['info_table_xml'] = f"{filing_base_url}/{info_table_url_part.split('/')[-1]}"

        except Exception as e:
            self.logger.error(f"Error finding XML URLs for {filename}: {e}")
            return {}
        
        self.logger.info(f"Found XML URLs: {urls}")
        return urls

    async def get_xml_document(self, url: str) -> Optional[bytes]:
        """Fetch the content of an XML document."""
        try:
            self.logger.debug(f"Fetching XML document: {url}")
            response = await self._get_with_retries(url)
            if not response:
                return None
            
            async with response:
                if response.status == 200:
                    return await response.read()
                else:
                    self.logger.warning(f"Failed to fetch XML document {url}: HTTP {response.status}")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching XML document {url}: {e}")
            return None

class S3Manager:
    """Manages S3 operations for storing 13F data"""
    
    def __init__(self, bucket_name: str, region: str, logger: AppLogger):
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
            year = filing_date.year
            month = filing_date.month
            key = f"13f_filings/{year}/{month:02d}/{cik}_{filing_date}_{form_type}.txt"
            
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
    
    def upload_parquet_to_s3(self, df: pd.DataFrame, filing_date: date, cik: str, form_type: str) -> bool:
        """Upload structured 13F data as a Parquet file to S3."""
        try:
            year = filing_date.year
            month = filing_date.month
            day = filing_date.day
            
            key = f"parsed_13f_filings/year={year}/month={month:02d}/day={day:02d}/{cik}_{filing_date}_{form_type.replace('/', '_')}.parquet"
            
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False)
            out_buffer.seek(0)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=out_buffer.read(),
                ContentType='application/octet-stream'
            )
            
            self.logger.info(f"Successfully uploaded Parquet file to S3: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upload Parquet file to S3: {str(e)}")
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

class FallbackParser:
    """Parses TXT/HTML fallback documents when XML is not available."""
    def __init__(self, logger: AppLogger):
        self.logger = logger

    def parse(self, content: str) -> (Optional[Dict[str, Any]], List[Dict[str, Any]]):
        """Tries to parse metadata and holdings from a TXT/HTML document."""
        self.logger.warning("Attempting to parse using TXT/HTML fallback.")
        metadata = self._parse_metadata(content)
        holdings = self._parse_holdings_table(content)
        return metadata, holdings

    def _parse_metadata(self, content: str) -> Dict[str, Any]:
        """Extracts summary metadata from the text content."""
        metadata = {}
        report_calendar_match = re.search(r"CONFORMED PERIOD OF REPORT:\s*(\d{8})", content)
        if report_calendar_match:
            metadata['period_of_report'] = report_calendar_match.group(1)

        table_entry_total_match = re.search(r"Report Summary(?:.|\n)*?Form 13F Information Table Entry Total:\s*(\d+)", content, re.IGNORECASE)
        if table_entry_total_match:
            metadata['table_entry_total'] = table_entry_total_match.group(1)

        table_value_total_match = re.search(r"Report Summary(?:.|\n)*?Form 13F Information Table Value Total:\s*([\d,]+)", content, re.IGNORECASE)
        if table_value_total_match:
            metadata['table_value_total'] = table_value_total_match.group(1).replace(',', '')
        
        return metadata

    def _parse_holdings_table(self, content: str) -> List[Dict[str, Any]]:
        """Extracts holdings from tables within the text/html content."""
        try:
            tables = read_html(io.StringIO(content))
            
            for df in tables:
                cols = {str(c).lower(): c for c in df.columns}
                if 'cusip' in cols and 'value' in cols and 'sshprnamt' in cols:
                    df.rename(columns={
                        cols['name of issuer']: 'name_of_issuer',
                        cols['title of class']: 'title_of_class',
                        cols['cusip']: 'cusip',
                        cols['value']: 'value',
                        cols['sshprnamt']: 'ssh_prnamt',
                    }, inplace=True)
                    
                    required_cols = ['name_of_issuer', 'title_of_class', 'cusip', 'value', 'ssh_prnamt']
                    for col in required_cols:
                        if col not in df.columns:
                            break
                    else:
                        self.logger.info(f"Successfully parsed holdings table from fallback with {len(df)} rows.")
                        return df.to_dict('records')

        except Exception as e:
            self.logger.warning(f"Could not parse table using pandas.read_html: {e}")

        self.logger.warning("No holdings table found in fallback document.")
        return []

class XMLParser:
    """Parses 13F XML documents."""

    def __init__(self, logger: AppLogger):
        self.logger = logger
        self.namespaces = {
            'ns': 'http://www.sec.gov/edgar/document/thirteenf/informationtable',
            'edgar': 'http://www.sec.gov/edgar/thirteenffiler'
        }

    def _safe_get_text(self, element, path):
        node = element.find(path, self.namespaces)
        return node.text.strip() if node is not None and node.text is not None else None

    def parse_primary_doc(self, xml_content: bytes) -> Dict[str, Any]:
        """Parses the primary_doc.xml for filing summary metadata."""
        try:
            root = etree.fromstring(xml_content)
            
            summary_page = root.find('edgar:summaryPage', self.namespaces)
            
            return {
                'report_type': self._safe_get_text(root, 'edgar:headerData/edgar:submissionType'),
                'period_of_report': self._safe_get_text(root.find('edgar:formData', self.namespaces), 'edgar:coverPage/edgar:reportCalendarOrQuarter'),
                'filer_cik': self._safe_get_text(root.find('edgar:headerData/edgar:filerInfo/edgar:filer', self.namespaces), 'edgar:credentials/edgar:cik'),
                'filer_name': self._safe_get_text(root.find('edgar:formData/edgar:coverPage/edgar:filingManager', self.namespaces), 'edgar:name'),
                'table_entry_total': self._safe_get_text(summary_page, 'edgar:tableEntryTotal') if summary_page is not None else '0',
                'table_value_total': self._safe_get_text(summary_page, 'edgar:tableValueTotal') if summary_page is not None else '0',
            }
        except Exception as e:
            self.logger.error(f"Error parsing primary_doc.xml: {e}")
            return {}

    def parse_information_table(self, xml_content: bytes) -> List[Dict[str, Any]]:
        """Parses the informationTable.xml for all holdings."""
        try:
            root = etree.fromstring(xml_content)
            holdings = []
            for info_table in root.findall('ns:infoTable', self.namespaces):
                holding = {
                    'name_of_issuer': self._safe_get_text(info_table, 'ns:nameOfIssuer'),
                    'title_of_class': self._safe_get_text(info_table, 'ns:titleOfClass'),
                    'cusip': self._safe_get_text(info_table, 'ns:cusip'),
                    'value': self._safe_get_text(info_table, 'ns:value'),
                    'ssh_prnamt': self._safe_get_text(info_table, 'ns:shrsOrPrnAmt/ns:sshPrnamt'),
                    'ssh_prnamt_type': self._safe_get_text(info_table, 'ns:shrsOrPrnAmt/ns:sshPrnamtType'),
                    'put_call': self._safe_get_text(info_table, 'ns:putCall'),
                    'investment_discretion': self._safe_get_text(info_table, 'ns:investmentDiscretion'),
                    'other_manager': self._safe_get_text(info_table, 'ns:otherManager'),
                    'voting_authority_sole': self._safe_get_text(info_table, 'ns:votingAuthority/ns:Sole'),
                    'voting_authority_shared': self._safe_get_text(info_table, 'ns:votingAuthority/ns:Shared'),
                    'voting_authority_none': self._safe_get_text(info_table, 'ns:votingAuthority/ns:None'),
                }
                holdings.append(holding)
            return holdings
        except Exception as e:
            self.logger.error(f"Error parsing informationTable.xml: {e}")
            return []

class DataNormalizer:
    """Normalizes and flattens the parsed 13F data."""

    def __init__(self, logger: AppLogger):
        self.logger = logger

    def normalize_and_flatten(self, filing_metadata: Dict, holdings_data: List[Dict], filing_info: Dict) -> pd.DataFrame:
        """Combines and flattens filing data, and normalizes values."""
        if not holdings_data:
            self.logger.warning(f"No holdings data to process for filing: {filing_info.get('cik')}")
            return pd.DataFrame()

        df = pd.DataFrame(holdings_data)
        df['raw_value'] = pd.to_numeric(df['value'], errors='coerce')
        
        summary_total_value = pd.to_numeric(filing_metadata.get('table_value_total'), errors='coerce')
        summary_entry_total = pd.to_numeric(filing_metadata.get('table_entry_total'), errors='coerce')

        holdings_sum = df['raw_value'].sum()
        value_scale = 'UNKNOWN'
        if pd.notna(summary_total_value) and holdings_sum > 0:
            if summary_total_value == 0:
                self.logger.warning(f"Summary total value is reported as 0 for {filing_info.get('cik')}; cannot perform scale detection.")
                value_scale = 'UNKNOWN'
                df['value_usd'] = df['raw_value'] * 1000 
            elif abs(holdings_sum - summary_total_value) / summary_total_value < 0.1: 
                value_scale = 'THOUSANDS'
                df['value_usd'] = df['raw_value'] * 1000
            elif abs((holdings_sum / 1000) - summary_total_value) / summary_total_value < 0.1:
                value_scale = 'DOLLARS'
                df['value_usd'] = df['raw_value']
                self.logger.warning(f"Detected value scale as DOLLARS for {filing_info.get('cik')}")
            else:
                df['value_usd'] = df['raw_value'] * 1000
        else:
            df['value_usd'] = df['raw_value'] * 1000 

        df['value_scale'] = value_scale

        validation_status = 'PASS'
        warnings = []

        if pd.notna(summary_entry_total) and abs(len(df) - summary_entry_total) > 2: 
            validation_status = 'WARN'
            warnings.append(f"Row count mismatch: parsed {len(df)}, summary {summary_entry_total}")

        if value_scale != 'UNKNOWN' and pd.notna(summary_total_value):
            if summary_total_value > 0:
                normalized_sum = df['value_usd'].sum()
                summary_sum_usd = summary_total_value * 1000
                if abs(normalized_sum - summary_sum_usd) / summary_sum_usd > 0.1: 
                    validation_status = 'WARN'
                    warnings.append(f"Value sum mismatch: parsed ${normalized_sum:,.0f}, summary ${summary_sum_usd:,.0f}")
            else:
                if df['value_usd'].sum() > 0:
                    validation_status = 'WARN'
                    warnings.append(f"Value sum mismatch: parsed holdings have value but summary total is 0.")

        if validation_status == 'WARN':
            self.logger.warning(f"Validation warnings for {filing_info.get('cik')}: {'; '.join(warnings)}")

        df['validation_status'] = validation_status
        df['validation_warnings'] = '; '.join(warnings) if warnings else None

        for key, value in filing_metadata.items():
            df[f"filing_{key}"] = value
        
        df['accession_number'] = filing_info['filename'].split('/')[-1].replace('.txt', '')
        df['cik'] = filing_info['cik']
        df['company_name'] = filing_info['company_name']
        df['form_type'] = filing_info['form_type']
        df['filing_date'] = filing_info['filing_date']

        df['cusip'] = df['cusip'].str.strip().str.upper()
        df['ssh_prnamt'] = pd.to_numeric(df['ssh_prnamt'], errors='coerce')
        df['voting_authority_sole'] = pd.to_numeric(df['voting_authority_sole'], errors='coerce').fillna(0).astype(int)
        df['voting_authority_shared'] = pd.to_numeric(df['voting_authority_shared'], errors='coerce').fillna(0).astype(int)
        df['voting_authority_none'] = pd.to_numeric(df['voting_authority_none'], errors='coerce').fillna(0).astype(int)

        return df

class ThirteenFETLV2:
    """Main ETL class for processing 13F filings using SEC index files"""
    
    def __init__(self, start_date: Optional[date] = None, end_date: Optional[date] = None, 
                 enable_s3: bool = True, concurrency: int = 10):
        self.start_date = start_date or (datetime.now() - timedelta(days=30)).date()
        self.end_date = end_date or (datetime.now()).date()
        self.enable_s3 = enable_s3
        self.logger = AppLogger()
        self.metrics = ETLMetrics()
        self.xml_parser = XMLParser(self.logger)
        self.fallback_parser = FallbackParser(self.logger)
        self.normalizer = DataNormalizer(self.logger)
        self.semaphore = asyncio.Semaphore(concurrency)
        
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
            async with SECIndexClient(self.logger, self.metrics) as sec_client:
                await self._process_13f_filings(sec_client)
            
            self.metrics.end_time = datetime.now()
            duration = (self.metrics.end_time - self.metrics.start_time).total_seconds()
            
            if self.enable_s3 and self.s3_manager:
                run_date_str = self.start_date.strftime('%Y-%m-%d')
                self.s3_manager.upload_etl_metrics(self.metrics, run_date_str)
            
            self.logger.info(f"ETL completed successfully in {duration:.2f} seconds")
            self.logger.info(f"Metrics: {self.metrics.records_processed} processed, "
                           f"{self.metrics.records_inserted} inserted, "
                           f"{self.metrics.records_failed} failed")
            
        except Exception as e:
            self.logger.error(f"ETL failed: {str(e)}")
            raise
    
    async def _process_13f_filings(self, sec_client: SECIndexClient):
        """Process 13F filings for the date range"""
        
        self.logger.info(f"Processing 13F filings from {self.start_date} to {self.end_date}")
        
        index_files = await sec_client.get_index_files(self.start_date, self.end_date)
        if not index_files:
            self.logger.warning("No index files found for the specified date range")
            return
        
        all_filings = []
        for index_url in index_files:
            filings = await sec_client.parse_index_file(index_url)
            all_filings.extend(filings)

        tasks = [self._process_one_filing(filing, sec_client) for filing in all_filings]
        await asyncio.gather(*tasks)

    async def _process_one_filing(self, filing: Dict[str, Any], sec_client: SECIndexClient):
        """Process a single 13F filing."""
        async with self.semaphore:
            self.metrics.records_processed += 1
            try:
                filing_metadata, holdings_data = {}, []

                xml_urls = await sec_client.get_filing_xml_urls(filing['filename'])
                
                primary_doc_xml = None
                if 'primary_doc_xml' in xml_urls:
                    primary_doc_xml = await sec_client.get_xml_document(xml_urls['primary_doc_xml'])
                    if primary_doc_xml:
                        filing_metadata = self.xml_parser.parse_primary_doc(primary_doc_xml)
                    else:
                        self.logger.warning(f"Could not retrieve primary_doc.xml for {filing['cik']}")

                info_table_xml = None
                if 'info_table_xml' in xml_urls:
                    info_table_xml = await sec_client.get_xml_document(xml_urls['info_table_xml'])
                    if info_table_xml:
                        holdings_data = self.xml_parser.parse_information_table(info_table_xml)
                    else:
                        self.logger.warning(f"Could not retrieve info_table.xml for {filing['cik']}")

                if not holdings_data:
                    self.logger.warning(f"No holdings from XML for {filing['cik']}. Attempting fallback.")
                    document_data = await sec_client.get_filing_document(filing['filename'])
                    if document_data:
                        fallback_meta, fallback_holdings = self.fallback_parser.parse(document_data['content'])
                        if fallback_holdings:
                            holdings_data = fallback_holdings
                            filing_metadata = {**fallback_meta, **filing_metadata}

                if holdings_data:
                    normalized_df = self.normalizer.normalize_and_flatten(filing_metadata, holdings_data, filing)
                    
                    if not normalized_df.empty and self.enable_s3 and self.s3_manager:
                        self.s3_manager.upload_parquet_to_s3(normalized_df, filing['filing_date'], filing['cik'], filing['form_type'])
                        self.logger.info(f"Successfully processed and uploaded structured data for {filing['company_name']}")
                        self.metrics.records_inserted += len(normalized_df)
                else:
                    self.logger.warning(f"No holdings data found in any format for {filing['cik']}")
                
            except Exception as e:
                self.metrics.records_failed += 1
                self.logger.error(f"Processing error for filing {filing.get('cik', 'unknown')}: {e}")

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

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).date()
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).date()

    etl = ThirteenFETLV2(start_date, end_date, enable_s3)
    await etl.run()

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    This function is the entry point for the Lambda execution.
    It runs the ETL for the previous day.
    """
    yesterday = datetime.now() - timedelta(days=1)
    
    return asyncio.run(main(start_date_override=yesterday.date(), end_date_override=yesterday.date()))


if __name__ == "__main__":
    asyncio.run(main())
