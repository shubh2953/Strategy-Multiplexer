import time
import gspread
import datetime
import pytz
import smtplib
import sqlite3
import threading
import random
import os
import sys
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from oauth2client.service_account import ServiceAccountCredentials

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("strategy_monitor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("strategy_monitor")

# Database Handler class
class DatabaseHandler:
    def __init__(self, db_file='trading_data.db'):
        self.db_file = db_file
        self.conn = None
        self.create_connection()
        self.lock = threading.Lock()
        
    def create_connection(self):
        try:
            self.conn = sqlite3.connect(
                self.db_file, 
                check_same_thread=False,
                timeout=30.0  # Add 30 second timeout
            )
            # Enable WAL mode for better concurrency
            self.conn.execute('PRAGMA journal_mode=WAL;')
            # Set busy timeout
            self.conn.execute('PRAGMA busy_timeout=30000;')  # 30 seconds
            self.conn.commit()
            logger.info(f"Connected to SQLite database {self.db_file}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to database: {e}")

    def create_strategy_config_table(self):
        """Create a table to store strategy configurations"""
        with self.lock:
            strategy_config_table = """
            CREATE TABLE IF NOT EXISTS strategy_config (
                strategy_idx INTEGER PRIMARY KEY,
                strategy_name TEXT,
                sheet_url TEXT,
                initial_cash REAL DEFAULT 100000,
                initial_nav REAL DEFAULT 100000,
                active INTEGER DEFAULT 1,
                creation_date TEXT
            );
            """
            
            cursor = self.conn.cursor()
            cursor.execute(strategy_config_table)
            self.conn.commit()
            logger.info("Created strategy_config table if it didn't exist")
            
    def create_extended_strategy_config_table(self):
        """Create a table to store extended strategy information"""
        with self.lock:
            extended_info_table = """
            CREATE TABLE IF NOT EXISTS strategy_extended_info (
                strategy_idx INTEGER PRIMARY KEY,
                owner_name TEXT,
                owner_email TEXT,
                description TEXT,
                FOREIGN KEY (strategy_idx) REFERENCES strategy_config(strategy_idx)
            );
            """
            
            cursor = self.conn.cursor()
            cursor.execute(extended_info_table)
            self.conn.commit()
            logger.info("Created strategy_extended_info table if it didn't exist")

    def create_strategy_cash_table(self):
        """Create a table to store strategy cash values"""
        with self.lock:
            strategy_cash_table = """
            CREATE TABLE IF NOT EXISTS strategy_cash (
                strategy_idx INTEGER,
                date TEXT,
                cash REAL,
                PRIMARY KEY (strategy_idx, date),
                FOREIGN KEY (strategy_idx) REFERENCES strategy_config(strategy_idx)
            );
            """
            
            cursor = self.conn.cursor()
            cursor.execute(strategy_cash_table)
            self.conn.commit()
            logger.info("Created strategy_cash table if it didn't exist")

    def create_strategy_nav_tracking_table(self):
        """Create table to track current NAV values"""
        with self.lock:
            nav_tracking_table = """
            CREATE TABLE IF NOT EXISTS strategy_nav_tracking (
                strategy_idx INTEGER,
                date TEXT,
                current_nav REAL,
                PRIMARY KEY (strategy_idx, date),
                FOREIGN KEY (strategy_idx) REFERENCES strategy_config(strategy_idx)
            );
            """
            cursor = self.conn.cursor()
            cursor.execute(nav_tracking_table)
            self.conn.commit()
            logger.info("Created strategy_nav_tracking table if it didn't exist")

    

    def deactivate_strategy(self, strategy_idx):
        """Permanently deactivate a strategy (cannot be reactivated)"""
        with self.lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute("""
                    UPDATE strategy_config 
                    SET active = -1 
                    WHERE strategy_idx = ?
                """, (strategy_idx,))
                self.conn.commit()
                logger.info(f"Permanently deactivated strategy {strategy_idx}")
                return True
            except Exception as e:
                logger.error(f"Error deactivating strategy: {str(e)}")
                return False

    # def get_strategy_status_changes(self):
    #     """Get strategies that have been marked inactive but not yet processed"""
    #     with self.lock:
    #         cursor = self.conn.cursor()
    #         cursor.execute("""
    #             SELECT strategy_idx, strategy_name, initial_nav 
    #             FROM strategy_config 
    #             WHERE active = 0
    #         """)
    #         return cursor.fetchall()

    def get_strategy_status_changes(self):
        """Get strategies that have been marked inactive but not yet processed"""
        with self.lock:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT sc.strategy_idx, sc.strategy_name, sc.initial_nav, sc.sheet_url, sei.owner_email
                FROM strategy_config sc
                JOIN strategy_extended_info sei ON sc.strategy_idx = sei.strategy_idx
                WHERE sc.active = 0
            """)
            return cursor.fetchall()

            
    def store_strategy_cash(self, strategy_idx, date, cash):
        """Store cash value for a strategy on a given date"""
        with self.lock:
            # Standardize date format
            date = self.standardize_date_format(date)
            
            sql = """
            INSERT OR REPLACE INTO strategy_cash (strategy_idx, date, cash)
            VALUES (?, ?, ?);
            """
            
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, (strategy_idx, date, cash))
                self.conn.commit()
                return True
            except sqlite3.Error as e:
                logger.error(f"Error storing strategy cash: {e}")
                return False
                
    def get_previous_day_cash(self, strategy_idx):
        """Get the most recent cash value for a strategy"""
        with self.lock:
            sql = """
            SELECT cash FROM strategy_cash 
            WHERE strategy_idx = ? 
            ORDER BY date DESC 
            LIMIT 1;
            """
            
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, (strategy_idx,))
                result = cursor.fetchone()
                return result[0] if result else None
            except sqlite3.Error as e:
                logger.error(f"Error getting previous day cash: {e}")
                return None
                
    def standardize_date_format(self, date_str):
        """Convert date string to YYYY-MM-DD format"""
        if not date_str or not isinstance(date_str, str):
            return date_str
            
        # If already in YYYY-MM-DD format, return as is
        import re
        if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
            return date_str
            
        # Try parsing with datetime
        try:
            from dateutil import parser
            parsed_date = parser.parse(date_str)
            return parsed_date.strftime("%Y-%m-%d")
        except:
            logger.warning(f"Could not convert date format: {date_str}")
            return date_str

# Enhanced rate limiting for Google Sheets API
class RateLimitedClient:
    def __init__(self, credentials_file, max_calls_per_minute=60):
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        self.client = gspread.authorize(creds)
        self.max_calls_per_minute = max_calls_per_minute
        self.call_timestamps = []
        self.lock = threading.Lock()

    def _wait_if_needed(self):
        """Wait if we're approaching the rate limit"""
        with self.lock:
            now = time.time()
            # Remove timestamps older than 60 seconds
            self.call_timestamps = [ts for ts in self.call_timestamps if now - ts < 60]
            # If we're at the limit, wait until we can make another call
            if len(self.call_timestamps) >= self.max_calls_per_minute:
                oldest = min(self.call_timestamps)
                sleep_time = 61 - (now - oldest)
                if sleep_time > 0:
                    logger.info(f"Rate limit approaching. Waiting {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                # After waiting, update now and clean timestamps again
                now = time.time()
                self.call_timestamps = [ts for ts in self.call_timestamps if now - ts < 60]
            # Add current timestamp
            self.call_timestamps.append(now)
            # Add jitter to avoid synchronization problems
            jitter = random.uniform(0.1, 0.5)
            time.sleep(jitter)

    def open_by_url(self, url):
        """Open a spreadsheet by URL with rate limiting"""
        self._wait_if_needed()
        return self.client.open_by_url(url)

    def open(self, title):
        """Open a spreadsheet by title with rate limiting"""
        self._wait_if_needed()
        return self.client.open(title)

def retry_with_backoff(func, *args, max_retries=5, initial_delay=1, **kwargs):
    """Retry a function with exponential backoff."""
    retries = 0
    delay = initial_delay
    
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if ("[429]" in str(e) or "Quota exceeded" in str(e)) and retries < max_retries:
                logger.warning(f"Rate limit exceeded. Retrying in {delay} seconds...")
                time.sleep(delay)
                retries += 1
                delay *= 2  # Exponential backoff
            else:
                raise e
    
    raise Exception(f"Failed after {max_retries} retries")

def create_strategy_nav_tracking_table(db_handler):
    """Create table to track current NAV values"""
    with db_handler.lock:
        nav_tracking_table = """
        CREATE TABLE IF NOT EXISTS strategy_nav_tracking (
            strategy_idx INTEGER,
            date TEXT,
            current_nav REAL,
            PRIMARY KEY (strategy_idx, date),
            FOREIGN KEY (strategy_idx) REFERENCES strategy_config(strategy_idx)
        );
        """
        cursor = db_handler.conn.cursor()
        cursor.execute(nav_tracking_table)
        db_handler.conn.commit()
        logger.info("Created strategy_nav_tracking table if it didn't exist")




# def check_available_nav_capacity(db_handler):
#     """Check how much of the 1,000,000 total NAV capacity is still available"""
#     # Get all active strategies
#     with db_handler.lock:
#         cursor = db_handler.conn.cursor()
#         cursor.execute("""
#             SELECT SUM(initial_nav) FROM strategy_config WHERE active = 1
#         """)
#         result = cursor.fetchone()
        
#         total_allocated = result[0] if result[0] is not None else 0
        
#         # Configuration values
#         total_nav = 1000000
#         margin_limit = 1.5
        
#         # Calculate available capacity using margin formula
#         available_capacity = margin_limit * total_nav - total_allocated
        
#         return max(0, available_capacity)

def check_available_nav_capacity(db_handler):
    """Check how much of the total NAV capacity is still available"""
    with db_handler.lock:
        cursor = db_handler.conn.cursor()
        cursor.execute("""
            SELECT SUM(initial_nav) FROM strategy_config WHERE active = 1
        """)
        result = cursor.fetchone()
        
        total_allocated = result[0] if result[0] is not None else 0
        
        # Configuration values
        total_nav = 1000000
        margin_limit = 1.5
        
        # Calculate available capacity using margin formula
        available_capacity = margin_limit * total_nav - total_allocated
        
        return max(0, available_capacity)


# def create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email):
#     """Create a new Google Sheet for the strategy with sample data and formatting"""
#     try:
#         sheet_title = f"{strategy_name} - {owner_name} - Strategy Input"
#         spreadsheet = rate_limited_client.client.create(sheet_title)
#         sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit"
        
#         # Initialize main input sheet
#         worksheet = spreadsheet.sheet1
#         worksheet.update_title("Strategy Input")
        
#         # Add and format headers
#         headers = ["Date", "Ticker", "Target Position"]
#         worksheet.append_row(headers)
        
#         # Format header row (bold, underlined, with borders)
#         worksheet.format('A1:C1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Add sample data row
#         today = datetime.datetime.now().strftime("%Y-%m-%d")
#         sample_row = [today, "AAPL", 0]
#         worksheet.append_row(sample_row)
        
#         # Create supporting sheets with formatted headers
#         detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=14)
#         detail_headers = [
#             'Date', 'Ticker', 'Net Units', 'Total Abs Units', 'Trade Price',
#             'Adj_Close', 'Trade Type', 'Pre Trade Position', 'Delta Shares',
#             'Post Trade Position', 'Commission', 'Interest', 'NAV', 'Cash'
#         ]
#         detail_worksheet.append_row(detail_headers)
#         detail_worksheet.format('A1:N1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Daily NAV sheet
#         nav_sheet = spreadsheet.add_worksheet(title="Daily NAV", rows=1000, cols=3)
#         nav_sheet.append_row(["Date", "Daily NAV"])
#         nav_sheet.format('A1:B1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Balance sheet
#         balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=8)
#         balance_headers = ["Date", "Timestamp", "Ticker", "Position", "Trade Price", "MKT Value", "Cash", "NAV"]
#         balance_sheet.append_row(balance_headers)
#         balance_sheet.format('A1:H1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Share with service account and owner
#         spreadsheet.share('ibkr-service@gpurclone.iam.gserviceaccount.com', perm_type='user', role='writer')
#         try:
#             spreadsheet.share(owner_email, perm_type='user', role='writer')
#             logger.info(f"Shared sheet with owner: {owner_email}")
#         except Exception as e:
#             logger.warning(f"Could not share with owner {owner_email}: {str(e)}")
        
#         logger.info(f"Created new strategy sheet with sample data: {sheet_url}")
#         return sheet_url
#     except Exception as e:
#         logger.error(f"Error creating strategy input sheet: {str(e)}")
#         return None

# def create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email):
#     """Create a new Google Sheet for the strategy with sample data and formatting"""
#     try:
#         sheet_title = f"{strategy_name} - {owner_name} - Strategy Input"
#         spreadsheet = rate_limited_client.client.create(sheet_title)
#         sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit"
        
#         # Initialize main input sheet
#         worksheet = spreadsheet.sheet1
#         worksheet.update_title("Strategy Input")
        
#         # Add and format headers
#         headers = ["Date", "Ticker", "Target Position"]
#         worksheet.append_row(headers)
        
#         # Format header row (bold, underlined, with borders)
#         worksheet.format('A1:C1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Add sample data row
#         today = datetime.datetime.now().strftime("%Y-%m-%d")
#         sample_row = [today, "AAPL", 0]
#         worksheet.append_row(sample_row)
        
#         # Create supporting sheets with formatted headers
#         detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=14)
#         detail_headers = [
#             'Date', 'Ticker', 'Net Units', 'Total Abs Units', 'Trade Price',
#             'Adj_Close', 'Trade Type', 'Pre Trade Position', 'Delta Shares',
#             'Post Trade Position', 'Commission', 'Interest', 'NAV', 'Cash'
#         ]
#         detail_worksheet.append_row(detail_headers)
#         detail_worksheet.format('A1:N1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Daily NAV sheet
#         nav_sheet = spreadsheet.add_worksheet(title="Daily NAV", rows=1000, cols=3)
#         nav_sheet.append_row(["Date", "Daily NAV"])
#         nav_sheet.format('A1:B1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Balance sheet
#         balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=8)
#         balance_headers = ["Date", "Timestamp", "Ticker", "Position", "Trade Price", "MKT Value", "Cash", "NAV"]
#         balance_sheet.append_row(balance_headers)
#         balance_sheet.format('A1:H1', {
#             'textFormat': {'bold': True, 'underline': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         # Share with service account and owner
#         spreadsheet.share('ibkr-service@gpurclone.iam.gserviceaccount.com', perm_type='user', role='writer')
#         try:
#             spreadsheet.share(owner_email, perm_type='user', role='writer')
#             logger.info(f"Shared sheet with owner: {owner_email}")
#         except Exception as e:
#             logger.warning(f"Could not share with owner {owner_email}: {str(e)}")
        
#         logger.info(f"Created new strategy sheet with sample data: {sheet_url}")
#         return sheet_url
#     except Exception as e:
#         logger.error(f"Error creating strategy input sheet: {str(e)}")
#         return None

def create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email):
    """Create a new Google Sheet for the strategy with sample data and formatting"""
    try:
        sheet_title = f"{strategy_name} - {owner_name} - Strategy Input"
        spreadsheet = rate_limited_client.client.create(sheet_title)
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit"
        
        # Initialize main input sheet
        worksheet = spreadsheet.sheet1
        worksheet.update_title("Strategy Input")
        
        # Add and format headers
        headers = ["Date", "Ticker", "Target Position"]
        worksheet.append_row(headers)
        
        # Format header row (bold, underlined, with borders)
        worksheet.format('A1:C1', {
            'textFormat': {'bold': True, 'underline': True},
            'borders': {
                'top': {'style': 'SOLID', 'width': 1},
                'bottom': {'style': 'SOLID', 'width': 1},
                'left': {'style': 'SOLID', 'width': 1},
                'right': {'style': 'SOLID', 'width': 1}
            }
        })
        
        # Add sample data row
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        sample_row = [today, "AAPL", 0]
        worksheet.append_row(sample_row)
        
        # Create supporting sheets with formatted headers
        detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=14)
        detail_headers = [
            'Date', 'Ticker', 'Net Units', 'Total Abs Units', 'Trade Price',
            'Adj_Close', 'Trade Type', 'Pre Trade Position', 'Delta Shares',
            'Post Trade Position', 'Commission', 'Interest', 'NAV', 'Cash'
        ]
        detail_worksheet.append_row(detail_headers)
        detail_worksheet.format('A1:N1', {
            'textFormat': {'bold': True, 'underline': True},
            'borders': {
                'top': {'style': 'SOLID', 'width': 1},
                'bottom': {'style': 'SOLID', 'width': 1},
                'left': {'style': 'SOLID', 'width': 1},
                'right': {'style': 'SOLID', 'width': 1}
            }
        })
        
        # Daily NAV sheet
        nav_sheet = spreadsheet.add_worksheet(title="Daily NAV", rows=1000, cols=3)
        nav_sheet.append_row(["Date", "Daily NAV"])
        nav_sheet.format('A1:B1', {
            'textFormat': {'bold': True, 'underline': True},
            'borders': {
                'top': {'style': 'SOLID', 'width': 1},
                'bottom': {'style': 'SOLID', 'width': 1},
                'left': {'style': 'SOLID', 'width': 1},
                'right': {'style': 'SOLID', 'width': 1}
            }
        })
        
        # Balance sheet
        balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=8)
        balance_headers = ["Date", "Timestamp", "Ticker", "Position", "Trade Price", "MKT Value", "Cash", "NAV"]
        balance_sheet.append_row(balance_headers)
        balance_sheet.format('A1:H1', {
            'textFormat': {'bold': True, 'underline': True},
            'borders': {
                'top': {'style': 'SOLID', 'width': 1},
                'bottom': {'style': 'SOLID', 'width': 1},
                'left': {'style': 'SOLID', 'width': 1},
                'right': {'style': 'SOLID', 'width': 1}
            }
        })
        
        # Share with service account and owner
        spreadsheet.share('ibkr-service@gpurclone.iam.gserviceaccount.com', perm_type='user', role='writer')
        try:
            spreadsheet.share(owner_email, perm_type='user', role='writer')
            logger.info(f"Shared sheet with owner: {owner_email}")
        except Exception as e:
            logger.warning(f"Could not share with owner {owner_email}: {str(e)}")
        
        logger.info(f"Created new strategy sheet with sample data: {sheet_url}")
        return sheet_url
    except Exception as e:
        logger.error(f"Error creating strategy input sheet: {str(e)}")
        return None


    
def deactivate_strategy(db_handler, strategy_idx):
    """Permanently deactivate a strategy (cannot be reactivated)"""
    with db_handler.lock:
        try:
            cursor = db_handler.conn.cursor()
            cursor.execute("""
                UPDATE strategy_config 
                SET active = -1 
                WHERE strategy_idx = ?
            """, (strategy_idx,))
            db_handler.conn.commit()
            logger.info(f"Permanently deactivated strategy {strategy_idx}")
            return True
        except Exception as e:
            logger.error(f"Error deactivating strategy: {str(e)}")
            return False

def send_strategy_email(owner_email, strategy_name, owner_name, initial_nav, sheet_url):
    """Send an email with strategy details and files"""
    try:
        # Set up the email
        message = MIMEMultipart()
        message['From'] = 'shubham.gupta@fischerjordan.com'  # Use actual email
        message['To'] = owner_email
        message['Subject'] = f"New Strategy Setup: {strategy_name}"
        
        # Email body
        body = f"""
        <html>
        <body>
        <h2>New Strategy Setup</h2>
        
        <p>Your new trading strategy has been set up successfully.</p>
        
        <h3>Strategy Details:</h3>
        <ul>
            <li><strong>Strategy Name:</strong> {strategy_name}</li>
            <li><strong>Owner:</strong> {owner_name}</li>
            <li><strong>Initial NAV:</strong> ${initial_nav:,.2f}</li>
        </ul>
        
        <p><a href="{sheet_url}">Click here to access your strategy input sheet</a></p>
        
        <h3>Instructions:</h3>
        <p>Please find attached the files 'credentials_IBKR.json' that contains google sheets 
        connection credentials and a 'connect_to_google_sheets.py' file that contains 
        some sample code and instructions on how to connect to the google sheets and make changes.</p>
        
        </body>
        </html>
        """
        
        # Attach HTML content
        message.attach(MIMEText(body, 'html'))
        
        # Check if files exist before attaching
        if os.path.exists('credentials_IBKR.json'):
            with open('credentials_IBKR.json', 'rb') as file:
                attachment = MIMEBase('application', 'octet-stream')
                attachment.set_payload(file.read())
                encoders.encode_base64(attachment)
                attachment.add_header('Content-Disposition', 'attachment', filename='credentials_IBKR.json')
                message.attach(attachment)
        else:
            logger.warning("credentials_IBKR.json file not found")
        
        if os.path.exists('connect_to_google_sheets.py'):
            with open('connect_to_google_sheets.py', 'rb') as file:
                attachment = MIMEBase('application', 'octet-stream')
                attachment.set_payload(file.read())
                encoders.encode_base64(attachment)
                attachment.add_header('Content-Disposition', 'attachment', filename='connect_to_google_sheets.py')
                message.attach(attachment)
        else:
            logger.warning("connect_to_google_sheets.py file not found")
        
        # Send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login('shubham.gupta@fischerjordan.com', 'nwjstonzzrdxgdmt')
            server.send_message(message)
        
        logger.info(f"Sent strategy setup email to {owner_email}")
        return True
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return False

# def add_strategy(db_handler, strategy_name, sheet_url, initial_cash=100000, initial_nav=100000, 
#                  owner_name='', owner_email='', description=''):
#     """Add a new strategy to the system with owner information"""
#     max_retries = 3
#     retry_delay = 1
    
#     for attempt in range(max_retries):
#         try:
#             with db_handler.lock:
#                 # Get next available strategy index
#                 cursor = db_handler.conn.cursor()
#                 cursor.execute("SELECT MAX(strategy_idx) FROM strategy_config")
#                 result = cursor.fetchone()
                
#                 next_idx = 0
#                 if result[0] is not None:
#                     next_idx = result[0] + 1
                
#                 # Current date in Eastern timezone
#                 eastern = pytz.timezone('US/Eastern')
#                 current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
                
#                 # Start transaction
#                 cursor.execute("BEGIN IMMEDIATE;")
                
#                 try:
#                     # Insert strategy into config table
#                     cursor.execute("""
#                         INSERT INTO strategy_config 
#                         (strategy_idx, strategy_name, sheet_url, initial_cash, initial_nav, creation_date) 
#                         VALUES (?, ?, ?, ?, ?, ?)
#                     """, (next_idx, strategy_name, sheet_url, initial_cash, initial_nav, current_date))
                    
#                     # Insert extended information
#                     cursor.execute("""
#                         INSERT INTO strategy_extended_info
#                         (strategy_idx, owner_name, owner_email, description)
#                         VALUES (?, ?, ?, ?)
#                     """, (next_idx, owner_name, owner_email, description))
                    
#                     # Initialize cash for the strategy
#                     cursor.execute("""
#                         INSERT OR REPLACE INTO strategy_cash (strategy_idx, date, cash)
#                         VALUES (?, ?, ?);
#                     """, (next_idx, current_date, initial_cash))
                    
#                     # Initialize NAV tracking
#                     cursor.execute("""
#                         INSERT OR REPLACE INTO strategy_nav_tracking (strategy_idx, date, current_nav)
#                         VALUES (?, ?, ?);
#                     """, (next_idx, current_date, initial_nav))
                    
#                     # Commit transaction
#                     db_handler.conn.commit()
#                     logger.info(f"Added new strategy: {strategy_name} (ID: {next_idx})")
#                     return next_idx
                    
#                 except Exception as e:
#                     # Rollback on error
#                     db_handler.conn.rollback()
#                     raise e
                    
#         except sqlite3.OperationalError as e:
#             if "database is locked" in str(e).lower() and attempt < max_retries - 1:
#                 logger.warning(f"Database locked on attempt {attempt + 1}, retrying in {retry_delay} seconds...")
#                 time.sleep(retry_delay)
#                 retry_delay *= 2  # Exponential backoff
#                 continue
#             else:
#                 logger.error(f"Error adding strategy after {attempt + 1} attempts: {str(e)}")
#                 return -1
#         except Exception as e:
#             logger.error(f"Error adding strategy: {str(e)}")
#             return -1
    
#     logger.error(f"Failed to add strategy after {max_retries} attempts")
#     return -1

def add_strategy(db_handler, strategy_name, sheet_url, initial_cash=100000, initial_nav=100000, 
                 owner_name='', owner_email='', description=''):
    """Add a new strategy to the system with owner information"""
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            with db_handler.lock:
                # Get next available strategy index
                cursor = db_handler.conn.cursor()
                cursor.execute("SELECT MAX(strategy_idx) FROM strategy_config")
                result = cursor.fetchone()
                
                next_idx = 0
                if result[0] is not None:
                    next_idx = result[0] + 1
                
                # Current date in Eastern timezone
                eastern = pytz.timezone('US/Eastern')
                current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
                
                # Start transaction
                cursor.execute("BEGIN IMMEDIATE;")
                
                try:
                    # Insert strategy into config table
                    cursor.execute("""
                        INSERT INTO strategy_config 
                        (strategy_idx, strategy_name, sheet_url, initial_cash, initial_nav, creation_date) 
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (next_idx, strategy_name, sheet_url, initial_cash, initial_nav, current_date))
                    
                    # Insert extended information
                    cursor.execute("""
                        INSERT INTO strategy_extended_info
                        (strategy_idx, owner_name, owner_email, description)
                        VALUES (?, ?, ?, ?)
                    """, (next_idx, owner_name, owner_email, description))
                    
                    # Initialize cash for the strategy
                    cursor.execute("""
                        INSERT OR REPLACE INTO strategy_cash (strategy_idx, date, cash)
                        VALUES (?, ?, ?);
                    """, (next_idx, current_date, initial_cash))
                    
                    # Initialize NAV tracking
                    cursor.execute("""
                        INSERT OR REPLACE INTO strategy_nav_tracking (strategy_idx, date, current_nav)
                        VALUES (?, ?, ?);
                    """, (next_idx, current_date, initial_nav))
                    
                    # Commit transaction
                    db_handler.conn.commit()
                    logger.info(f"Added new strategy: {strategy_name} (ID: {next_idx})")
                    return next_idx
                    
                except Exception as e:
                    # Rollback on error
                    db_handler.conn.rollback()
                    raise e
                    
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                logger.warning(f"Database locked on attempt {attempt + 1}, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
                continue
            else:
                logger.error(f"Error adding strategy after {attempt + 1} attempts: {str(e)}")
                return -1
        except Exception as e:
            logger.error(f"Error adding strategy: {str(e)}")
            return -1
    
    logger.error(f"Failed to add strategy after {max_retries} attempts")
    return -1


# def get_strategy_current_nav(db_handler, strategy_name):
#     """Get current NAV for a strategy"""
#     with db_handler.lock:
#         cursor = db_handler.conn.cursor()
#         cursor.execute("""
#             SELECT snt.current_nav 
#             FROM strategy_nav_tracking snt
#             JOIN strategy_config sc ON snt.strategy_idx = sc.strategy_idx
#             WHERE sc.strategy_name = ?
#             ORDER BY snt.date DESC
#             LIMIT 1
#         """, (strategy_name,))
#         result = cursor.fetchone()
#         return result[0] if result else 0

def get_strategy_current_nav(db_handler, strategy_name):
    """Get current NAV for a strategy"""
    with db_handler.lock:
        cursor = db_handler.conn.cursor()
        cursor.execute("""
            SELECT snt.current_nav 
            FROM strategy_nav_tracking snt
            JOIN strategy_config sc ON snt.strategy_idx = sc.strategy_idx
            WHERE sc.strategy_name = ?
            ORDER BY snt.date DESC
            LIMIT 1
        """, (strategy_name,))
        result = cursor.fetchone()
        return result[0] if result else 0
    
def get_strategy_status_changes(db_handler):
    """Get strategies that have been marked inactive but not yet processed"""
    with db_handler.lock:
        cursor = db_handler.conn.cursor()
        cursor.execute("""
            SELECT sc.strategy_idx, sc.strategy_name, sc.initial_nav, sc.sheet_url, sei.owner_email
            FROM strategy_config sc
            JOIN strategy_extended_info sei ON sc.strategy_idx = sei.strategy_idx
            WHERE sc.active = 0
        """)
        return cursor.fetchall()

def process_status_changes(rate_limited_client, db_handler, setup_sheet):
    """Process strategies that have been changed from active to inactive"""
    try:
        # Get strategies marked for deactivation
        strategies_to_deactivate = get_strategy_status_changes(db_handler)
        
        for strategy_idx, strategy_name, initial_nav, sheet_url, owner_email in strategies_to_deactivate:
            logger.info(f"Processing deactivation for strategy: {strategy_name}")
            
            # Change sheet permissions to view-only instead of liquidating
            permission_changed = change_sheet_permissions_to_view_only(
                rate_limited_client, sheet_url, owner_email
            )
            
            if permission_changed:
                logger.info(f"Successfully changed permissions to view-only for {strategy_name}")
                
                # Send notification email about the change
                send_deactivation_notification_email(
                    owner_email, strategy_name, sheet_url
                )
            else:
                logger.error(f"Failed to change permissions for {strategy_name}")
            
            # Mark as permanently deactivated in database
            deactivate_strategy(db_handler, strategy_idx)
            
            # Return cash to general pool (update available capacity)
            logger.info(f"Returned ${initial_nav:,.2f} to general pool from strategy {strategy_name}")
            
    except Exception as e:
        logger.error(f"Error processing status changes: {str(e)}")

# def send_deactivation_notification_email(owner_email, strategy_name, sheet_url):
#     """Send notification email when strategy is deactivated"""
#     try:
#         message = MIMEMultipart()
#         message['From'] = 'shubham.gupta@fischerjordan.com'
#         message['To'] = owner_email
#         message['Subject'] = f"Strategy Deactivated: {strategy_name}"
        
#         body = f"""
#         <html>
#         <body>
#         <h2>Strategy Deactivation Notice</h2>
        
#         <p>Your trading strategy has been deactivated.</p>
        
#         <h3>Strategy Details:</h3>
#         <ul>
#             <li><strong>Strategy Name:</strong> {strategy_name}</li>
#             <li><strong>Status:</strong> Deactivated</li>
#         </ul>
        
#         <p>Your access to the strategy sheet has been changed to view-only. You can still view the historical data but cannot make new trades.</p>
        
#         <p><a href="{sheet_url}">Click here to view your strategy sheet</a></p>
        
#         <p><strong>Note:</strong> Once deactivated, strategies cannot be reactivated.</p>
        
#         </body>
#         </html>
#         """
        
#         message.attach(MIMEText(body, 'html'))
        
#         with smtplib.SMTP('smtp.gmail.com', 587) as server:
#             server.starttls()
#             server.login('shubham.gupta@fischerjordan.com', 'nwjstonzzrdxgdmt')
#             server.send_message(message)
        
#         logger.info(f"Sent deactivation notification email to {owner_email}")
#         return True
#     except Exception as e:
#         logger.error(f"Error sending deactivation email: {str(e)}")
#         return False

def send_deactivation_notification_email(owner_email, strategy_name, sheet_url):
    """Send notification email when strategy is deactivated"""
    try:
        message = MIMEMultipart()
        message['From'] = 'shubham.gupta@fischerjordan.com'
        message['To'] = owner_email
        message['Subject'] = f"Strategy Deactivated: {strategy_name}"
        
        body = f"""
        <html>
        <body>
        <h2>Strategy Deactivation Notice</h2>
        
        <p>Your trading strategy has been deactivated.</p>
        
        <h3>Strategy Details:</h3>
        <ul>
            <li><strong>Strategy Name:</strong> {strategy_name}</li>
            <li><strong>Status:</strong> Deactivated</li>
        </ul>
        
        <p>Your access to the strategy sheet has been changed to view-only. You can still view the historical data but cannot make new trades.</p>
        
        <p><a href="{sheet_url}">Click here to view your strategy sheet</a></p>
        
        <p><strong>Note:</strong> Once deactivated, strategies cannot be reactivated.</p>
        
        </body>
        </html>
        """
        
        message.attach(MIMEText(body, 'html'))
        
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login('shubham.gupta@fischerjordan.com', 'nwjstonzzrdxgdmt')
            server.send_message(message)
        
        logger.info(f"Sent deactivation notification email to {owner_email}")
        return True
    except Exception as e:
        logger.error(f"Error sending deactivation email: {str(e)}")
        return False



# def update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler):
#     """Update summary rows in the strategy setup sheet"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Calculate summary values
#         with db_handler.lock:
#             cursor = db_handler.conn.cursor()
            
#             # Active NAV (sum of initial NAVs of active strategies)
#             cursor.execute("SELECT SUM(initial_nav) FROM strategy_config WHERE active = 1")
#             active_nav_result = cursor.fetchone()
#             active_nav = active_nav_result[0] if active_nav_result[0] is not None else 0
            
#             # Configuration values
#             total_nav = 1000000
#             margin_limit = 1.5  # You can make this configurable
#             strategy_limit = 200000  # Maximum NAV for individual strategy
            
#             # Available NAV calculation
#             available_nav = margin_limit * total_nav - active_nav
#             available_nav = max(0, available_nav)  # Ensure non-negative
        
#         # Find empty area (column J) to place summary
#         summary_start_row = 2
        
#         # Update summary rows in column J and K
#         summary_data = [
#             ["Active NAV", active_nav],
#             ["Available NAV", available_nav], 
#             ["Total NAV", total_nav],
#             ["Margin Limit", margin_limit],
#             ["Strategy Limit", strategy_limit]
#         ]
        
#         # Clear previous summary and update
#         setup_sheet.batch_update([
#             {
#                 'range': f'J{summary_start_row}:K{summary_start_row + len(summary_data) - 1}',
#                 'values': summary_data
#             }
#         ])
        
#         # Format summary section
#         setup_sheet.format(f'J{summary_start_row}:K{summary_start_row + len(summary_data) - 1}', {
#             'textFormat': {'bold': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         logger.info("Updated strategy setup summary")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error updating summary: {str(e)}")
#         return False

# def update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler):
#     """Update summary rows in the strategy setup sheet"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Calculate summary values
#         with db_handler.lock:
#             cursor = db_handler.conn.cursor()
            
#             # Active NAV (sum of initial NAVs of active strategies)
#             cursor.execute("SELECT SUM(initial_nav) FROM strategy_config WHERE active = 1")
#             active_nav_result = cursor.fetchone()
#             active_nav = active_nav_result[0] if active_nav_result[0] is not None else 0
            
#             # Configuration values
#             total_nav = 1000000
#             margin_limit = 1.5
#             strategy_limit = 200000
            
#             # Available NAV calculation
#             available_nav = margin_limit * total_nav - active_nav
#             available_nav = max(0, available_nav)
        
#         # Find empty area (column J) to place summary starting at row 2
#         summary_start_row = 2
        
#         # Update summary rows in column J and K
#         summary_data = [
#             ["Active NAV", active_nav],
#             ["Available NAV", available_nav], 
#             ["Total NAV", total_nav],
#             ["Margin Limit", margin_limit],
#             ["Strategy Limit", strategy_limit]
#         ]
        
#         # Clear previous summary and update
#         setup_sheet.batch_update([
#             {
#                 'range': f'J{summary_start_row}:K{summary_start_row + len(summary_data) - 1}',
#                 'values': summary_data
#             }
#         ])
        
#         # Format summary section
#         setup_sheet.format(f'J{summary_start_row}:K{summary_start_row + len(summary_data) - 1}', {
#             'textFormat': {'bold': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         logger.info("Updated strategy setup summary")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error updating summary: {str(e)}")
#         return False

# def update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler):
#     """Update summary rows in the strategy setup sheet"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Calculate summary values
#         with db_handler.lock:
#             cursor = db_handler.conn.cursor()
            
#             # Active NAV (sum of initial NAVs of active strategies)
#             cursor.execute("SELECT SUM(initial_nav) FROM strategy_config WHERE active = 1")
#             active_nav_result = cursor.fetchone()
#             active_nav = active_nav_result[0] if active_nav_result[0] is not None else 0
            
#             # Configuration values
#             total_nav = 1000000
#             margin_limit = 1.5
#             strategy_limit = 200000
            
#             # Available NAV calculation
#             available_nav = margin_limit * total_nav - active_nav
#             available_nav = max(0, available_nav)
        
#         # Place summary in columns I and J starting from row 2
#         summary_start_row = 2
        
#         # Update summary rows in columns I and J
#         summary_data = [
#             ["Active NAV", active_nav],
#             ["Available NAV", available_nav], 
#             ["Total NAV", total_nav],
#             ["Margin Limit", margin_limit],
#             ["Strategy Limit", strategy_limit]
#         ]
        
#         # Clear previous summary and update
#         setup_sheet.batch_update([
#             {
#                 'range': f'I{summary_start_row}:J{summary_start_row + len(summary_data) - 1}',
#                 'values': summary_data
#             }
#         ])
        
#         # Format summary section
#         setup_sheet.format(f'I{summary_start_row}:J{summary_start_row + len(summary_data) - 1}', {
#             'textFormat': {'bold': True},
#             'borders': {
#                 'top': {'style': 'SOLID', 'width': 1},
#                 'bottom': {'style': 'SOLID', 'width': 1},
#                 'left': {'style': 'SOLID', 'width': 1},
#                 'right': {'style': 'SOLID', 'width': 1}
#             }
#         })
        
#         logger.info("Updated strategy setup summary")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error updating summary: {str(e)}")
#         return False

def update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler):
    """Update summary rows in the strategy setup sheet"""
    try:
        spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
        setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
        # Calculate summary values
        with db_handler.lock:
            cursor = db_handler.conn.cursor()
            
            # Active NAV (sum of initial NAVs of active strategies)
            cursor.execute("SELECT SUM(initial_nav) FROM strategy_config WHERE active = 1")
            active_nav_result = cursor.fetchone()
            active_nav = active_nav_result[0] if active_nav_result[0] is not None else 0
            
            # Configuration values
            total_nav = 1000000
            margin_limit = 1.5
            strategy_limit = 200000
            
            # Available NAV calculation
            available_nav = margin_limit * total_nav - active_nav
            available_nav = max(0, available_nav)
        
        # Place summary in columns J and K starting from row 2
        summary_start_row = 2
        
        # Update summary rows in columns J and K
        summary_data = [
            ["Active NAV", active_nav],
            ["Available NAV", available_nav], 
            ["Total NAV", total_nav],
            ["Margin Limit", margin_limit],
            ["Strategy Limit", strategy_limit]
        ]
        
        # Clear previous summary and update - changed from I:J to J:K
        setup_sheet.batch_update([
            {
                'range': f'J{summary_start_row}:K{summary_start_row + len(summary_data) - 1}',
                'values': summary_data
            }
        ])
        
        # Format summary section - changed from I:J to J:K
        setup_sheet.format(f'J{summary_start_row}:K{summary_start_row + len(summary_data) - 1}', {
            'textFormat': {'bold': True},
            'borders': {
                'top': {'style': 'SOLID', 'width': 1},
                'bottom': {'style': 'SOLID', 'width': 1},
                'left': {'style': 'SOLID', 'width': 1},
                'right': {'style': 'SOLID', 'width': 1}
            }
        })
        
        logger.info("Updated strategy setup summary")
        return True
        
    except Exception as e:
        logger.error(f"Error updating summary: {str(e)}")
        return False




    
# def change_sheet_permissions_to_view_only(rate_limited_client, sheet_url, owner_email):
#     """Change owner's access from editor to view-only on the strategy sheet"""
#     try:
#         # Extract spreadsheet ID from URL
#         spreadsheet_id = sheet_url.split('/d/')[1].split('/')[0]
#         spreadsheet = rate_limited_client.client.open_by_key(spreadsheet_id)
        
#         # Remove current permissions for the owner
#         try:
#             spreadsheet.share(owner_email, perm_type='user', role='reader', notify=False)
#             logger.info(f"Changed {owner_email} access to view-only for sheet: {sheet_url}")
#             return True
#         except Exception as e:
#             logger.error(f"Error changing permissions for {owner_email}: {str(e)}")
#             return False
            
#     except Exception as e:
#         logger.error(f"Error processing sheet permissions change: {str(e)}")
#         return False

# def change_sheet_permissions_to_view_only(rate_limited_client, sheet_url, owner_email):
#     """Change owner's access from editor to view-only on the strategy sheet"""
#     try:
#         # Extract spreadsheet ID from URL
#         spreadsheet_id = sheet_url.split('/d/')[1].split('/')[0]
#         spreadsheet = rate_limited_client.client.open_by_key(spreadsheet_id)
        
#         # Change permissions to view-only
#         try:
#             spreadsheet.share(owner_email, perm_type='user', role='reader', notify=False)
#             logger.info(f"Changed {owner_email} access to view-only for sheet: {sheet_url}")
#             return True
#         except Exception as e:
#             logger.error(f"Error changing permissions for {owner_email}: {str(e)}")
#             return False
            
#     except Exception as e:
#         logger.error(f"Error processing sheet permissions change: {str(e)}")
#         return False

#v2
# def change_sheet_permissions_to_view_only(rate_limited_client, sheet_url, owner_email):
#     """Change owner's access from editor to view-only on the strategy sheet"""
#     try:
#         # Extract spreadsheet ID from URL
#         spreadsheet_id = sheet_url.split('/d/')[1].split('/')[0]
#         spreadsheet = rate_limited_client.client.open_by_key(spreadsheet_id)
        
#         try:
#             # Get current permissions
#             permissions = spreadsheet.list_permissions()
            
#             # Find and remove existing permission for this user
#             for permission in permissions:
#                 if permission.get('emailAddress') == owner_email:
#                     current_role = permission.get('role')
#                     permission_id = permission.get('id')
                    
#                     logger.info(f"Found existing permission for {owner_email} with role: {current_role}")
                    
#                     # Only proceed if they currently have writer/editor access
#                     if current_role in ['writer', 'editor']:
#                         # Remove the existing permission
#                         spreadsheet.remove_permissions(permission_id)
#                         logger.info(f"Removed {current_role} permission for {owner_email}")
                        
#                         # Add reader permission
#                         spreadsheet.share(owner_email, perm_type='user', role='reader', notify=False)
#                         logger.info(f"Added reader permission for {owner_email}")
#                         return True
#                     elif current_role == 'reader':
#                         logger.info(f"{owner_email} already has reader access")
#                         return True
#                     else:
#                         logger.warning(f"Unknown role {current_role} for {owner_email}")
#                         return False
            
#             # If no existing permission found, just add reader permission
#             logger.info(f"No existing permission found for {owner_email}, adding reader access")
#             spreadsheet.share(owner_email, perm_type='user', role='reader', notify=False)
#             return True
            
#         except Exception as e:
#             logger.error(f"Error managing permissions for {owner_email}: {str(e)}")
#             return False
            
#     except Exception as e:
#         logger.error(f"Error processing sheet permissions change: {str(e)}")
#         return False

# def change_sheet_permissions_to_view_only(rate_limited_client, sheet_url, owner_email):
#     """Change owner's access from editor to view-only on the strategy sheet"""
#     try:
#         # Extract spreadsheet ID from URL
#         spreadsheet_id = sheet_url.split('/d/')[1].split('/')[0]
#         spreadsheet = rate_limited_client.client.open_by_key(spreadsheet_id)
        
#         # First, get all permissions to find the user's existing permission
#         try:
#             permissions = spreadsheet.list_permissions()
#             user_permission_id = None
            
#             # Find the permission ID for this user
#             for permission in permissions:
#                 if permission.get('emailAddress') == owner_email:
#                     user_permission_id = permission.get('id')
#                     break
            
#             # Remove existing permission if found
#             if user_permission_id:
#                 spreadsheet.remove_permissions(user_permission_id)
#                 logger.info(f"Removed existing permission for {owner_email}")
            
#             # Add new reader permission
#             spreadsheet.share(owner_email, perm_type='user', role='reader', notify=False)
#             logger.info(f"Changed {owner_email} access to view-only for sheet: {sheet_url}")
#             return True
            
#         except Exception as e:
#             logger.error(f"Error changing permissions for {owner_email}: {str(e)}")
#             return False
            
#     except Exception as e:
#         logger.error(f"Error processing sheet permissions change: {str(e)}")
#         return False

def analyze_user_permissions(rate_limited_client, sheet_url, owner_email):
    """Analyze all ways a user might have access to debug permission issues"""
    try:
        spreadsheet_id = sheet_url.split('/d/')[1].split('/')[0]
        spreadsheet = rate_limited_client.client.open_by_key(spreadsheet_id)
        
        permissions = spreadsheet.list_permissions()
        
        logger.info(f"=== Permission Analysis for {owner_email} ===")
        
        # Direct permissions
        direct_permissions = [p for p in permissions if p.get('emailAddress') == owner_email]
        logger.info(f"Direct permissions: {len(direct_permissions)}")
        for perm in direct_permissions:
            logger.info(f"  - Role: {perm.get('role')}, ID: {perm.get('id')}")
        
        # Domain permissions
        if '@' in owner_email:
            domain = owner_email.split('@')[1]
            domain_permissions = [p for p in permissions if p.get('domain') == domain]
            logger.info(f"Domain permissions for {domain}: {len(domain_permissions)}")
            for perm in domain_permissions:
                logger.info(f"  - Role: {perm.get('role')}, Type: {perm.get('type')}")
        
        # Anyone permissions
        anyone_permissions = [p for p in permissions if p.get('type') == 'anyone']
        logger.info(f"Anyone permissions: {len(anyone_permissions)}")
        for perm in anyone_permissions:
            logger.info(f"  - Role: {perm.get('role')}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error analyzing permissions: {str(e)}")
        return False


def change_sheet_permissions_to_view_only(rate_limited_client, sheet_url, owner_email):
    """Change owner's access from editor to view-only with verification"""
    try:
        spreadsheet_id = sheet_url.split('/d/')[1].split('/')[0]
        spreadsheet = rate_limited_client.client.open_by_key(spreadsheet_id)
        
        # First, analyze current permissions
        analyze_user_permissions(rate_limited_client, sheet_url, owner_email)
        
        # Remove all direct permissions for the user
        try:
            removed_any = False
            permissions = spreadsheet.list_permissions()
            
            for permission in permissions:
                if permission.get('emailAddress') == owner_email:
                    permission_id = permission.get('id')
                    role = permission.get('role')
                    
                    try:
                        spreadsheet.client.remove_permission(spreadsheet_id, permission_id)
                        logger.info(f"Removed {role} permission (ID: {permission_id}) for {owner_email}")
                        removed_any = True
                    except Exception as e:
                        logger.error(f"Failed to remove permission {permission_id}: {str(e)}")
            
            # Wait for propagation
            if removed_any:
                time.sleep(3)
            
            # Add reader permission
            spreadsheet.share(owner_email, perm_type='user', role='reader', notify=False)
            logger.info(f"Added reader permission for {owner_email}")
            
            # Wait and verify
            time.sleep(2)
            analyze_user_permissions(rate_limited_client, sheet_url, owner_email)
            
            return True
            
        except Exception as e:
            logger.error(f"Error changing permissions: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing sheet permissions change: {str(e)}")
        return False





# def update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler):
#     """Update the current NAV column for all strategies"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Add Current NAV header if it doesn't exist
#         headers = setup_sheet.row_values(1)
#         current_nav_col = None
        
#         # Check if Current NAV column exists
#         if "Current NAV" in headers:
#             current_nav_col = headers.index("Current NAV") + 1
#         else:
#             # Find next available column
#             current_nav_col = len(headers) + 1
#             setup_sheet.update_cell(1, current_nav_col, "Current NAV")
#             setup_sheet.format(f'{chr(64 + current_nav_col)}1', {
#                 'textFormat': {'bold': True, 'underline': True},
#                 'borders': {
#                     'top': {'style': 'SOLID', 'width': 1},
#                     'bottom': {'style': 'SOLID', 'width': 1},
#                     'left': {'style': 'SOLID', 'width': 1},
#                     'right': {'style': 'SOLID', 'width': 1}
#                 }
#             })
        
#         # Update current NAV values for each strategy
#         all_data = setup_sheet.get_all_records()
#         for i, row in enumerate(all_data, start=2):
#             strategy_name = row.get('Strategy Name', '').strip()
#             if strategy_name:
#                 # Get current NAV from database or calculation
#                 current_nav = get_strategy_current_nav(db_handler, strategy_name)
#                 setup_sheet.update_cell(i, current_nav_col, current_nav)
        
#         logger.info("Updated current NAV column")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error updating current NAV column: {str(e)}")
#         return False

# def update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler):
#     """Update the current NAV column for all strategies"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Add Current NAV header if it doesn't exist
#         headers = setup_sheet.row_values(1)
#         current_nav_col = None
        
#         # Check if Current NAV column exists
#         if "Current NAV" in headers:
#             current_nav_col = headers.index("Current NAV") + 1
#         else:
#             # Find next available column
#             current_nav_col = len(headers) + 1
#             setup_sheet.update_cell(1, current_nav_col, "Current NAV")
#             setup_sheet.format(f'{chr(64 + current_nav_col)}1', {
#                 'textFormat': {'bold': True, 'underline': True},
#                 'borders': {
#                     'top': {'style': 'SOLID', 'width': 1},
#                     'bottom': {'style': 'SOLID', 'width': 1},
#                     'left': {'style': 'SOLID', 'width': 1},
#                     'right': {'style': 'SOLID', 'width': 1}
#                 }
#             })
        
#         # Update current NAV values for each strategy
#         all_data = setup_sheet.get_all_records()
#         for i, row in enumerate(all_data, start=2):
#             strategy_name = row.get('Strategy Name', '').strip()
#             if strategy_name:
#                 # Get current NAV from database
#                 current_nav = get_strategy_current_nav(db_handler, strategy_name)
#                 setup_sheet.update_cell(i, current_nav_col, current_nav)
        
#         logger.info("Updated current NAV column")
#         return True
        
#     except Exception as e:
#         logger.error(f"Error updating current NAV column: {str(e)}")
#         return False

def update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler):
    """Update the current NAV column for all strategies"""
    try:
        spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
        setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
        # Get current headers
        headers = setup_sheet.row_values(1)
        current_nav_col = None
        
        # Check if Current NAV column exists
        if "Current NAV" in headers:
            current_nav_col = headers.index("Current NAV") + 1
        else:
            logger.warning("Current NAV column not found in headers")
            return False
        
        # Get all values to avoid header uniqueness issues
        try:
            all_values = setup_sheet.get_all_values()
            if len(all_values) < 2:
                return True
                
            # Update current NAV values for each strategy
            for i in range(1, len(all_values)):  # Skip header row
                row = all_values[i]
                if len(row) > 0:  # Check if row has data
                    strategy_name = row[0].strip() if len(row) > 0 else ""
                    if strategy_name:
                        # Get current NAV from database
                        current_nav = get_strategy_current_nav(db_handler, strategy_name)
                        setup_sheet.update_cell(i + 1, current_nav_col, current_nav)
        except Exception as e:
            logger.error(f"Error updating current NAV values: {str(e)}")
            return False
        
        logger.info("Updated current NAV column")
        return True
        
    except Exception as e:
        logger.error(f"Error updating current NAV column: {str(e)}")
        return False
    

# def process_status_changes(rate_limited_client, db_handler, setup_sheet):
#     """Process strategies that have been changed from active to inactive"""
#     try:
#         # Get strategies marked for deactivation
#         strategies_to_deactivate = db_handler.get_strategy_status_changes()
        
#         for strategy_idx, strategy_name, initial_nav in strategies_to_deactivate:
#             logger.info(f"Processing deactivation for strategy: {strategy_name}")
            
#             # Mark as permanently deactivated
#             db_handler.deactivate_strategy(strategy_idx)
            
#             # Here you would add liquidation logic for next trading day
#             # liquidate_strategy_positions(strategy_idx)
            
#             # Return cash to general pool (update available capacity)
#             logger.info(f"Returned ${initial_nav:,.2f} to general pool from strategy {strategy_name}")
            
#     except Exception as e:
#         logger.error(f"Error processing status changes: {str(e)}")

# def init_strategy_setup_sheet(rate_limited_client, setup_sheet_url):
#     """Initialize the strategy setup sheet with the required fields"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
        
#         # Check if Setup sheet exists, create if not
#         try:
#             setup_sheet = spreadsheet.worksheet("Strategy Setup")
#             logger.info("Found existing Strategy Setup sheet")
#         except:
#             setup_sheet = spreadsheet.add_worksheet(title="Strategy Setup", rows=100, cols=10)
#             logger.info("Created new Strategy Setup sheet")
        
#         # Check if the sheet has headers (check if first row has data)
#         try:
#             first_row = setup_sheet.row_values(1)
#             has_headers = any(cell.strip() for cell in first_row) if first_row else False
#         except:
#             has_headers = False
        
#         # Add headers if they don't exist
#         if not has_headers:
#             logger.info("Adding headers to Strategy Setup sheet")
#             headers = [
#                 "Strategy Name", 
#                 "Owner Name",
#                 "Owner Email", 
#                 "Description",
#                 "Initial NAV", 
#                 "Status",
#                 "Strategy Input Sheet URL",
#                 "Current NAV"  # Add Current NAV column
#             ]
            
#             # Clear the sheet first to ensure clean state
#             setup_sheet.clear()
            
#             # Add headers
#             setup_sheet.append_row(headers)
            
#             # Format headers
#             setup_sheet.format('A1:H1', {
#                 'textFormat': {'bold': True, 'underline': True},
#                 'borders': {
#                     'top': {'style': 'SOLID', 'width': 1},
#                     'bottom': {'style': 'SOLID', 'width': 1},
#                     'left': {'style': 'SOLID', 'width': 1},
#                     'right': {'style': 'SOLID', 'width': 1}
#                 }
#             })
            
#             # Add data validation for Status column (Active/Inactive)
#             try:
#                 validation_rule = {
#                     "condition": {
#                         "type": "ONE_OF_LIST",
#                         "values": [{"userEnteredValue": "Active"}, {"userEnteredValue": "Inactive"}]
#                     },
#                     "showCustomUi": True,
#                     "strict": True
#                 }
                
#                 # Apply validation to Status column
#                 body = {
#                     "requests": [
#                         {
#                             "setDataValidation": {
#                                 "range": {
#                                     "sheetId": setup_sheet.id,
#                                     "startRowIndex": 1,
#                                     "endRowIndex": 100,
#                                     "startColumnIndex": 5,
#                                     "endColumnIndex": 6
#                                 },
#                                 "rule": validation_rule
#                             }
#                         }
#                     ]
#                 }
#                 spreadsheet.batch_update(body)
#                 logger.info("Applied data validation to Status column")
#             except Exception as validation_error:
#                 logger.warning(f"Could not apply data validation: {validation_error}")
            
#             logger.info("Successfully initialized strategy setup sheet with headers and validation")
#         else:
#             logger.info("Strategy Setup sheet already has headers")
            
#         return setup_sheet
#     except Exception as e:
#         logger.error(f"Error initializing strategy setup sheet: {str(e)}")
#         return None

# def init_strategy_setup_sheet(rate_limited_client, setup_sheet_url):
#     """Initialize the strategy setup sheet with the required fields"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
        
#         # Check if Setup sheet exists, create if not
#         try:
#             setup_sheet = spreadsheet.worksheet("Strategy Setup")
#             logger.info("Found existing Strategy Setup sheet")
#         except:
#             setup_sheet = spreadsheet.add_worksheet(title="Strategy Setup", rows=100, cols=10)
#             logger.info("Created new Strategy Setup sheet")
        
#         # Check if the sheet has headers
#         try:
#             first_row = setup_sheet.row_values(1)
#             has_headers = any(cell.strip() for cell in first_row) if first_row else False
#         except:
#             has_headers = False
        
#         # Add headers if they don't exist
#         if not has_headers:
#             logger.info("Adding headers to Strategy Setup sheet")
#             headers = [
#                 "Strategy Name", 
#                 "Owner Name",
#                 "Owner Email", 
#                 "Description",
#                 "Initial NAV", 
#                 "Status",
#                 "Strategy Input Sheet URL",
#                 "Current NAV"
#             ]
            
#             # Clear the sheet first
#             setup_sheet.clear()
            
#             # Add headers
#             setup_sheet.append_row(headers)
            
#             # Format headers
#             setup_sheet.format('A1:H1', {
#                 'textFormat': {'bold': True, 'underline': True},
#                 'borders': {
#                     'top': {'style': 'SOLID', 'width': 1},
#                     'bottom': {'style': 'SOLID', 'width': 1},
#                     'left': {'style': 'SOLID', 'width': 1},
#                     'right': {'style': 'SOLID', 'width': 1}
#                 }
#             })
            
#             # Add data validation for Status column
#             try:
#                 validation_rule = {
#                     "condition": {
#                         "type": "ONE_OF_LIST",
#                         "values": [{"userEnteredValue": "Active"}, {"userEnteredValue": "Inactive"}]
#                     },
#                     "showCustomUi": True,
#                     "strict": True
#                 }
                
#                 body = {
#                     "requests": [
#                         {
#                             "setDataValidation": {
#                                 "range": {
#                                     "sheetId": setup_sheet.id,
#                                     "startRowIndex": 1,
#                                     "endRowIndex": 100,
#                                     "startColumnIndex": 5,
#                                     "endColumnIndex": 6
#                                 },
#                                 "rule": validation_rule
#                             }
#                         }
#                     ]
#                 }
#                 spreadsheet.batch_update(body)
#                 logger.info("Applied data validation to Status column")
#             except Exception as validation_error:
#                 logger.warning(f"Could not apply data validation: {validation_error}")
            
#             logger.info("Successfully initialized strategy setup sheet")
#         else:
#             logger.info("Strategy Setup sheet already has headers")
            
#         return setup_sheet
#     except Exception as e:
#         logger.error(f"Error initializing strategy setup sheet: {str(e)}")
#         return None

def init_strategy_setup_sheet(rate_limited_client, setup_sheet_url):
    """Initialize the strategy setup sheet with the required fields"""
    try:
        spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
        
        # Check if Setup sheet exists, create if not
        try:
            setup_sheet = spreadsheet.worksheet("Strategy Setup")
            logger.info("Found existing Strategy Setup sheet")
        except:
            setup_sheet = spreadsheet.add_worksheet(title="Strategy Setup", rows=100, cols=12)
            logger.info("Created new Strategy Setup sheet")
        
        # Check if the sheet has headers (check if first row has data)
        try:
            first_row = setup_sheet.row_values(1)
            has_headers = any(cell.strip() for cell in first_row) if first_row else False
        except:
            has_headers = False
        
        # Add headers if they don't exist
        if not has_headers:
            logger.info("Adding headers to Strategy Setup sheet")
            # headers = [
            #     "Strategy Name",        # A
            #     "Owner Name",          # B
            #     "Owner Email",         # C
            #     "Description",         # D
            #     "Initial NAV",         # E
            #     "Status",              # F
            #     "Strategy Input Sheet URL",  # G
            #     "Current NAV",         # H
            #     "Summary Label",       # I (for summary section)
            #     "Summary Value"        # J (for summary values)
            # ]
            headers = [
                "Strategy Name",        # A
                "Owner Name",          # B
                "Owner Email",         # C
                "Description",         # D
                "Initial NAV",         # E
                "Status",              # F
                "Strategy Input Sheet URL",  # G
                "Current NAV",         # H
                "",                    # I (blank column)
                "Summary Label",       # J (for summary section)
                "Summary Value"        # K (for summary values)
            ]

            
            # Clear the sheet first to ensure clean state
            setup_sheet.clear()
            
            # Add headers
            setup_sheet.append_row(headers)
            
            # Format headers
            # setup_sheet.format('A1:J1', {
            #     'textFormat': {'bold': True, 'underline': True},
            #     'borders': {
            #         'top': {'style': 'SOLID', 'width': 1},
            #         'bottom': {'style': 'SOLID', 'width': 1},
            #         'left': {'style': 'SOLID', 'width': 1},
            #         'right': {'style': 'SOLID', 'width': 1}
            #     }
            # })
            setup_sheet.format('A1:K1', {
                'textFormat': {'bold': True, 'underline': True},
                'borders': {
                    'top': {'style': 'SOLID', 'width': 1},
                    'bottom': {'style': 'SOLID', 'width': 1},
                    'left': {'style': 'SOLID', 'width': 1},
                    'right': {'style': 'SOLID', 'width': 1}
                }
            })
            
            # Add data validation for Status column (Active/Inactive)
            try:
                validation_rule = {
                    "condition": {
                        "type": "ONE_OF_LIST",
                        "values": [{"userEnteredValue": "Active"}, {"userEnteredValue": "Inactive"}]
                    },
                    "showCustomUi": True,
                    "strict": True
                }
                
                # Apply validation to Status column (F)
                body = {
                    "requests": [
                        {
                            "setDataValidation": {
                                "range": {
                                    "sheetId": setup_sheet.id,
                                    "startRowIndex": 1,
                                    "endRowIndex": 100,
                                    "startColumnIndex": 5,  # Column F (0-indexed)
                                    "endColumnIndex": 6
                                },
                                "rule": validation_rule
                            }
                        }
                    ]
                }
                spreadsheet.batch_update(body)
                logger.info("Applied data validation to Status column")
            except Exception as validation_error:
                logger.warning(f"Could not apply data validation: {validation_error}")
            
            logger.info("Successfully initialized strategy setup sheet with headers and validation")
        else:
            logger.info("Strategy Setup sheet already has headers")
            
        return setup_sheet
    except Exception as e:
        logger.error(f"Error initializing strategy setup sheet: {str(e)}")
        return None



# def process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url):
#     """Process new strategy setup requests with enhanced validation"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Check for status changes (active to inactive)
#         process_status_changes(rate_limited_client, db_handler, setup_sheet)
        
#         # Get all rows
#         all_data = setup_sheet.get_all_records()
#         logger.info(f"Found {len(all_data)} rows in strategy setup sheet")
        
#         # Process each row
#         for i, row in enumerate(all_data, start=2):
#             status = row.get('Status', '').strip()
#             existing_url = row.get('Strategy Input Sheet URL', '').strip()
            
#             # Process only rows that are Active but don't have a URL yet
#             if status == 'Active' and not existing_url:
#                 strategy_name = row.get('Strategy Name', '').strip()
#                 owner_name = row.get('Owner Name', '').strip()
#                 owner_email = row.get('Owner Email', '').strip()
#                 description = row.get('Description', '').strip()
                
#                 logger.info(f"Processing new active strategy: {strategy_name} by {owner_name}")
                
#                 # Get and validate initial NAV
#                 try:
#                     initial_nav = float(row.get('Initial NAV', 0))
#                     if initial_nav <= 0:
#                         logger.error(f"Initial NAV must be positive for {strategy_name}")
#                         setup_sheet.update_cell(i, 6, "Failed")
#                         setup_sheet.update_cell(i, 7, "Initial NAV must be positive")
#                         continue
#                 except (ValueError, TypeError):
#                     logger.error(f"Invalid Initial NAV value for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Invalid Initial NAV value")
#                     continue
                
#                 # Validate required fields
#                 if not strategy_name or not owner_name or not owner_email:
#                     logger.error(f"Missing required information for strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Missing required information")
#                     continue
                
#                 # Check strategy limit
#                 strategy_limit = 200000  # Make this configurable if needed
#                 if initial_nav > strategy_limit:
#                     logger.error(f"Initial NAV exceeds strategy limit for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, f"Initial NAV exceeds strategy limit (${strategy_limit:,.2f})")
#                     continue
                
#                 # Check available NAV capacity
#                 available_capacity = check_available_nav_capacity(db_handler)
#                 if initial_nav > available_capacity:
#                     logger.error(f"Initial NAV exceeds available capacity for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, f"Initial NAV exceeds available capacity (${available_capacity:,.2f})")
#                     continue
                
#                 # Create strategy sheet and add to database
#                 sheet_url = create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email)
                
#                 if not sheet_url:
#                     logger.error(f"Failed to create sheet for strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Error creating strategy sheet")
#                     continue
                
#                 strategy_idx = add_strategy(
#                     db_handler, strategy_name, sheet_url, 
#                     initial_cash=initial_nav, initial_nav=initial_nav,
#                     owner_name=owner_name, owner_email=owner_email, description=description
#                 )
                
#                 if strategy_idx >= 0:
#                     setup_sheet.update_cell(i, 7, sheet_url)
#                     send_strategy_email(owner_email, strategy_name, owner_name, initial_nav, sheet_url)
#                     logger.info(f"Successfully processed strategy: {strategy_name} (ID: {strategy_idx})")
#                 else:
#                     logger.error(f"Database error when adding strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Database error")
                
#                 time.sleep(1.2)
        
#         # Update current NAV column
#         update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler)
        
#         # Update summary rows after processing
#         update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler)
        
#         return True
#     except Exception as e:
#         logger.error(f"Error processing strategy setup requests: {str(e)}")
#         return False

# def process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url):
#     """Process new strategy setup requests with enhanced validation"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Check for status changes (active to inactive)
#         process_status_changes(rate_limited_client, db_handler, setup_sheet)
        
#         # Get all rows
#         all_data = setup_sheet.get_all_records()
#         logger.info(f"Found {len(all_data)} rows in strategy setup sheet")
        
#         # Process each row
#         for i, row in enumerate(all_data, start=2):
#             status = row.get('Status', '').strip()
#             existing_url = row.get('Strategy Input Sheet URL', '').strip()
            
#             # Process only rows that are Active but don't have a URL yet
#             if status == 'Active' and not existing_url:
#                 strategy_name = row.get('Strategy Name', '').strip()
#                 owner_name = row.get('Owner Name', '').strip()
#                 owner_email = row.get('Owner Email', '').strip()
#                 description = row.get('Description', '').strip()
                
#                 logger.info(f"Processing new active strategy: {strategy_name} by {owner_name}")
                
#                 # Get and validate initial NAV
#                 try:
#                     initial_nav = float(row.get('Initial NAV', 0))
#                     if initial_nav <= 0:
#                         logger.error(f"Initial NAV must be positive for {strategy_name}")
#                         setup_sheet.update_cell(i, 6, "Failed")
#                         setup_sheet.update_cell(i, 7, "Initial NAV must be positive")
#                         continue
#                 except (ValueError, TypeError):
#                     logger.error(f"Invalid Initial NAV value for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Invalid Initial NAV value")
#                     continue
                
#                 # Validate required fields
#                 if not strategy_name or not owner_name or not owner_email:
#                     logger.error(f"Missing required information for strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Missing required information")
#                     continue
                
#                 # Check strategy limit
#                 strategy_limit = 200000
#                 if initial_nav > strategy_limit:
#                     logger.error(f"Initial NAV exceeds strategy limit for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, f"Initial NAV exceeds strategy limit (${strategy_limit:,.2f})")
#                     continue
                
#                 # Check available NAV capacity
#                 available_capacity = check_available_nav_capacity(db_handler)
#                 if initial_nav > available_capacity:
#                     logger.error(f"Initial NAV exceeds available capacity for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, f"Initial NAV exceeds available capacity (${available_capacity:,.2f})")
#                     continue
                
#                 # Create strategy sheet and add to database
#                 sheet_url = create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email)
                
#                 if not sheet_url:
#                     logger.error(f"Failed to create sheet for strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Error creating strategy sheet")
#                     continue
                
#                 strategy_idx = add_strategy(
#                     db_handler, strategy_name, sheet_url, 
#                     initial_cash=initial_nav, initial_nav=initial_nav,
#                     owner_name=owner_name, owner_email=owner_email, description=description
#                 )
                
#                 if strategy_idx >= 0:
#                     setup_sheet.update_cell(i, 7, sheet_url)
#                     send_strategy_email(owner_email, strategy_name, owner_name, initial_nav, sheet_url)
#                     logger.info(f"Successfully processed strategy: {strategy_name} (ID: {strategy_idx})")
#                 else:
#                     logger.error(f"Database error when adding strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Database error")
                
#                 time.sleep(1.2)
        
#         # Update current NAV column
#         update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler)
        
#         # Update summary rows after processing
#         update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler)
        
#         return True
#     except Exception as e:
#         logger.error(f"Error processing strategy setup requests: {str(e)}")
#         return False

# def process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url):
#     """Process new strategy setup requests with enhanced validation and error handling"""
#     try:
#         spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
#         setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
#         # Check for status changes (active to inactive)
#         process_status_changes(rate_limited_client, db_handler, setup_sheet)
        
#         # Get all rows using expected_headers to avoid duplicate header issues
#         expected_headers = [
#             "Strategy Name", "Owner Name", "Owner Email", "Description",
#             "Initial NAV", "Status", "Strategy Input Sheet URL", "Current NAV"
#         ]
        
#         try:
#             # Try with expected_headers first
#             all_data = setup_sheet.get_all_records(expected_headers=expected_headers)
#         except Exception as e:
#             logger.warning(f"get_all_records with expected_headers failed: {str(e)}")
#             try:
#                 # Fall back to getting all values and processing manually
#                 all_values = setup_sheet.get_all_values()
#                 if len(all_values) < 2:
#                     logger.info("No data rows found in strategy setup sheet")
#                     return True
                
#                 headers = all_values[0]
#                 data_rows = all_values[1:]
                
#                 # Create records manually, only using the columns we need
#                 all_data = []
#                 for row in data_rows:
#                     # Pad row with empty strings if it's shorter than headers
#                     padded_row = row + [''] * (len(headers) - len(row))
                    
#                     record = {}
#                     for i, header in enumerate(headers):
#                         if i < len(padded_row):
#                             record[header] = padded_row[i]
#                         else:
#                             record[header] = ''
                    
#                     # Only include rows that have data in the first column (Strategy Name)
#                     if record.get('Strategy Name', '').strip():
#                         all_data.append(record)
                        
#             except Exception as e2:
#                 logger.error(f"Manual data extraction also failed: {str(e2)}")
#                 return False
        
#         logger.info(f"Found {len(all_data)} rows in strategy setup sheet")
        
#         # Process each row
#         for i, row in enumerate(all_data, start=2):
#             status = row.get('Status', '').strip()
#             existing_url = row.get('Strategy Input Sheet URL', '').strip()
            
#             # Process only rows that are Active but don't have a URL yet
#             if status == 'Active' and not existing_url:
#                 strategy_name = row.get('Strategy Name', '').strip()
#                 owner_name = row.get('Owner Name', '').strip()
#                 owner_email = row.get('Owner Email', '').strip()
#                 description = row.get('Description', '').strip()
                
#                 logger.info(f"Processing new active strategy: {strategy_name} by {owner_name}")
                
#                 # Get and validate initial NAV
#                 try:
#                     initial_nav = float(row.get('Initial NAV', 0))
#                     if initial_nav <= 0:
#                         logger.error(f"Initial NAV must be positive for {strategy_name}")
#                         setup_sheet.update_cell(i, 6, "Failed")
#                         setup_sheet.update_cell(i, 7, "Initial NAV must be positive")
#                         continue
#                 except (ValueError, TypeError):
#                     logger.error(f"Invalid Initial NAV value for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Invalid Initial NAV value")
#                     continue
                
#                 # Validate required fields
#                 if not strategy_name or not owner_name or not owner_email:
#                     logger.error(f"Missing required information for strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Missing required information")
#                     continue
                
#                 # Check strategy limit
#                 strategy_limit = 200000
#                 if initial_nav > strategy_limit:
#                     logger.error(f"Initial NAV exceeds strategy limit for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, f"Initial NAV exceeds strategy limit (${strategy_limit:,.2f})")
#                     continue
                
#                 # Check available NAV capacity
#                 available_capacity = check_available_nav_capacity(db_handler)
#                 if initial_nav > available_capacity:
#                     logger.error(f"Initial NAV exceeds available capacity for {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, f"Initial NAV exceeds available capacity (${available_capacity:,.2f})")
#                     continue
                
#                 # Create strategy sheet and add to database
#                 sheet_url = create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email)
                
#                 if not sheet_url:
#                     logger.error(f"Failed to create sheet for strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Error creating strategy sheet")
#                     continue
                
#                 strategy_idx = add_strategy(
#                     db_handler, strategy_name, sheet_url, 
#                     initial_cash=initial_nav, initial_nav=initial_nav,
#                     owner_name=owner_name, owner_email=owner_email, description=description
#                 )
                
#                 if strategy_idx >= 0:
#                     setup_sheet.update_cell(i, 7, sheet_url)
#                     send_strategy_email(owner_email, strategy_name, owner_name, initial_nav, sheet_url)
#                     logger.info(f"Successfully processed strategy: {strategy_name} (ID: {strategy_idx})")
#                 else:
#                     logger.error(f"Database error when adding strategy: {strategy_name}")
#                     setup_sheet.update_cell(i, 6, "Failed")
#                     setup_sheet.update_cell(i, 7, "Database error")
                
#                 time.sleep(1.2)
        
#         # Update current NAV column
#         update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler)
        
#         # Update summary rows after processing
#         update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler)
        
#         return True
#     except Exception as e:
#         logger.error(f"Error processing strategy setup requests: {str(e)}")
#         import traceback
#         logger.error(f"Full traceback: {traceback.format_exc()}")
#         return False

def process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url):
    """Process new strategy setup requests with enhanced validation and error handling"""
    try:
        spreadsheet = rate_limited_client.open_by_url(setup_sheet_url)
        setup_sheet = spreadsheet.worksheet("Strategy Setup")
        
        # Get all rows using expected_headers to avoid duplicate header issues
        expected_headers = [
            "Strategy Name", "Owner Name", "Owner Email", "Description",
            "Initial NAV", "Status", "Strategy Input Sheet URL", "Current NAV"
        ]
        
        try:
            # Try with expected_headers first
            all_data = setup_sheet.get_all_records(expected_headers=expected_headers)
        except Exception as e:
            logger.warning(f"get_all_records with expected_headers failed: {str(e)}")
            try:
                # Fall back to getting all values and processing manually
                all_values = setup_sheet.get_all_values()
                if len(all_values) < 2:
                    logger.info("No data rows found in strategy setup sheet")
                    return True
                
                headers = all_values[0]
                data_rows = all_values[1:]
                
                # Create records manually, only using the columns we need
                all_data = []
                for row in data_rows:
                    # Pad row with empty strings if it's shorter than headers
                    padded_row = row + [''] * (len(headers) - len(row))
                    
                    record = {}
                    for i, header in enumerate(headers):
                        if i < len(padded_row):
                            record[header] = padded_row[i]
                        else:
                            record[header] = ''
                    
                    # Only include rows that have data in the first column (Strategy Name)
                    if record.get('Strategy Name', '').strip():
                        all_data.append(record)
                        
            except Exception as e2:
                logger.error(f"Manual data extraction also failed: {str(e2)}")
                return False
        
        logger.info(f"Found {len(all_data)} rows in strategy setup sheet")
        
        # Process each row
        for i, row in enumerate(all_data, start=2):
            status = row.get('Status', '').strip()
            existing_url = row.get('Strategy Input Sheet URL', '').strip()
            strategy_name = row.get('Strategy Name', '').strip()
            
            # NEW CODE: Check for status changes on existing strategies
            if existing_url and strategy_name:  # This is an existing strategy
                # Get current database status
                with db_handler.lock:
                    cursor = db_handler.conn.cursor()
                    cursor.execute("""
                        SELECT active FROM strategy_config 
                        WHERE strategy_name = ?
                    """, (strategy_name,))
                    result = cursor.fetchone()
                    
                    if result:
                        db_active_status = result[0]
                        
                        # If sheet shows Inactive but database shows Active (1)
                        if status == 'Inactive' and db_active_status == 1:
                            logger.info(f"Detected status change to Inactive for strategy: {strategy_name}")
                            
                            # Mark as inactive (0) in database for processing
                            cursor.execute("""
                                UPDATE strategy_config 
                                SET active = 0 
                                WHERE strategy_name = ?
                            """, (strategy_name,))
                            db_handler.conn.commit()
                            
                            logger.info(f"Marked strategy {strategy_name} as inactive in database")
                        
                        # If sheet shows Active but database shows Inactive (-1), don't reactivate
                        elif status == 'Active' and db_active_status == -1:
                            logger.warning(f"Cannot reactivate permanently deactivated strategy: {strategy_name}")
            
            # EXISTING CODE: Process new active strategies
            elif status == 'Active' and not existing_url:
                owner_name = row.get('Owner Name', '').strip()
                owner_email = row.get('Owner Email', '').strip()
                description = row.get('Description', '').strip()
                
                logger.info(f"Processing new active strategy: {strategy_name} by {owner_name}")
                
                # Get and validate initial NAV
                try:
                    initial_nav = float(row.get('Initial NAV', 0))
                    if initial_nav <= 0:
                        logger.error(f"Initial NAV must be positive for {strategy_name}")
                        setup_sheet.update_cell(i, 6, "Failed")
                        setup_sheet.update_cell(i, 7, "Initial NAV must be positive")
                        continue
                except (ValueError, TypeError):
                    logger.error(f"Invalid Initial NAV value for {strategy_name}")
                    setup_sheet.update_cell(i, 6, "Failed")
                    setup_sheet.update_cell(i, 7, "Invalid Initial NAV value")
                    continue
                
                # Validate required fields
                if not strategy_name or not owner_name or not owner_email:
                    logger.error(f"Missing required information for strategy: {strategy_name}")
                    setup_sheet.update_cell(i, 6, "Failed")
                    setup_sheet.update_cell(i, 7, "Missing required information")
                    continue
                
                # Check strategy limit
                strategy_limit = 200000
                if initial_nav > strategy_limit:
                    logger.error(f"Initial NAV exceeds strategy limit for {strategy_name}")
                    setup_sheet.update_cell(i, 6, "Failed")
                    setup_sheet.update_cell(i, 7, f"Initial NAV exceeds strategy limit (${strategy_limit:,.2f})")
                    continue
                
                # Check available NAV capacity
                available_capacity = check_available_nav_capacity(db_handler)
                if initial_nav > available_capacity:
                    logger.error(f"Initial NAV exceeds available capacity for {strategy_name}")
                    setup_sheet.update_cell(i, 6, "Failed")
                    setup_sheet.update_cell(i, 7, f"Initial NAV exceeds available capacity (${available_capacity:,.2f})")
                    continue
                
                # Create strategy sheet and add to database
                sheet_url = create_strategy_input_sheet(rate_limited_client, strategy_name, owner_name, owner_email)
                
                if not sheet_url:
                    logger.error(f"Failed to create sheet for strategy: {strategy_name}")
                    setup_sheet.update_cell(i, 6, "Failed")
                    setup_sheet.update_cell(i, 7, "Error creating strategy sheet")
                    continue
                
                strategy_idx = add_strategy(
                    db_handler, strategy_name, sheet_url, 
                    initial_cash=initial_nav, initial_nav=initial_nav,
                    owner_name=owner_name, owner_email=owner_email, description=description
                )
                
                if strategy_idx >= 0:
                    setup_sheet.update_cell(i, 7, sheet_url)
                    send_strategy_email(owner_email, strategy_name, owner_name, initial_nav, sheet_url)
                    logger.info(f"Successfully processed strategy: {strategy_name} (ID: {strategy_idx})")
                else:
                    logger.error(f"Database error when adding strategy: {strategy_name}")
                    setup_sheet.update_cell(i, 6, "Failed")
                    setup_sheet.update_cell(i, 7, "Database error")
                
                time.sleep(1.2)
        
        # Process status changes (this will now find the strategies we just marked as inactive)
        process_status_changes(rate_limited_client, db_handler, setup_sheet)
        
        # Update current NAV column
        update_current_nav_column(rate_limited_client, setup_sheet_url, db_handler)
        
        # Update summary rows after processing
        update_strategy_setup_summary(rate_limited_client, setup_sheet_url, db_handler)
        
        return True
    except Exception as e:
        logger.error(f"Error processing strategy setup requests: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return False


    


def check_once():
    """Run a single check for new strategies"""
    logger.info("Starting strategy monitoring check")
    
    # Initialize database
    db_handler = DatabaseHandler('trading_data.db')
    
    # Create tables if they don't exist
    db_handler.create_strategy_config_table()
    db_handler.create_extended_strategy_config_table()
    db_handler.create_strategy_cash_table()
    db_handler.create_strategy_nav_tracking_table()
    
    # Connect to Google Sheets with rate limiting
    credentials_file = "credentials_IBKR.json"
    rate_limited_client = RateLimitedClient(credentials_file)
    
    # Setup sheet URL for managing strategies
    setup_sheet_url = "https://docs.google.com/spreadsheets/d/1lkDpXQUYqJ7mck35Qw148D2oTD8jecXNl6WqmfcoC7k/edit?gid=0#gid=0"
    
    # Initialize the strategy setup sheet
    init_strategy_setup_sheet(rate_limited_client, setup_sheet_url)
    
    # Process any new strategies
    process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url)
    
    logger.info("Completed strategy monitoring check")

# def run_continuous_monitoring():
#     """Run continuous monitoring of the strategy setup sheet"""
#     logger.info("Starting continuous monitoring of strategy setup sheet")
    
#     # Initialize database
#     db_handler = DatabaseHandler('trading_data.db')
    
#     # Create all required tables
#     db_handler.create_strategy_config_table()
#     db_handler.create_extended_strategy_config_table()
#     db_handler.create_strategy_cash_table()
#     db_handler.create_strategy_nav_tracking_table()
    
#     # Connect to Google Sheets with rate limiting
#     credentials_file = "credentials_IBKR.json"
#     rate_limited_client = RateLimitedClient(credentials_file)
    
#     # Setup sheet URL for managing strategies
#     setup_sheet_url = "https://docs.google.com/spreadsheets/d/1SbL7yOb56pcU2S0bJPmdiA3vZot6w7DsLSS4r59LNy0/edit?gid=0#gid=0"
    
#     # Initialize the strategy setup sheet
#     init_strategy_setup_sheet(rate_limited_client, setup_sheet_url)
    
#     # Monitor for changes continuously
#     while True:
#         try:
#             # Process any new strategies
#             process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url)
#             logger.info(f"Checked for new strategies at {datetime.datetime.now()}")
            
#             # Wait for next check
#             time.sleep(60)  # Check every minute
            
#         except Exception as e:
#             logger.error(f"Error in monitoring loop: {str(e)}")
#             time.sleep(60)  # Continue checking even after errors

def run_continuous_monitoring():
    """Run continuous monitoring of the strategy setup sheet"""
    logger.info("Starting continuous monitoring of strategy setup sheet")
    
    # Initialize database
    db_handler = DatabaseHandler('trading_data.db')
    
    # Create all required tables
    db_handler.create_strategy_config_table()
    db_handler.create_extended_strategy_config_table()
    db_handler.create_strategy_cash_table()
    create_strategy_nav_tracking_table(db_handler)
    
    # Connect to Google Sheets with rate limiting
    credentials_file = "credentials_IBKR.json"
    rate_limited_client = RateLimitedClient(credentials_file)
    
    # Setup sheet URL
    setup_sheet_url = "https://docs.google.com/spreadsheets/d/1lkDpXQUYqJ7mck35Qw148D2oTD8jecXNl6WqmfcoC7k/edit?gid=0#gid=0"
    
    # Initialize the strategy setup sheet
    init_strategy_setup_sheet(rate_limited_client, setup_sheet_url)
    
    # Monitor for changes continuously
    while True:
        try:
            # Process any new strategies
            process_strategy_setup_requests(rate_limited_client, db_handler, setup_sheet_url)
            logger.info(f"Checked for new strategies at {datetime.datetime.now()}")
            
            # Wait for next check
            time.sleep(60)  # Check every minute
            
        except Exception as e:
            logger.error(f"Error in monitoring loop: {str(e)}")
            time.sleep(60)  # Continue checking even after errors


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor strategy setup sheet for changes.')
    parser.add_argument('--once', action='store_true', help='Run a single check instead of continuous monitoring')
    parser.add_argument('--setup-sheet', type=str, help='URL of the strategy setup sheet')
    
    args = parser.parse_args()
    
    if args.once:
        # Run a single check
        check_once()
    else:
        # Run continuous monitoring
        run_continuous_monitoring()
