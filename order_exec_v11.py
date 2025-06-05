import time
import json
import datetime
import gspread
import threading
import pytz  # For timezone handling
import sqlite3
from sqlite3 import Error
from oauth2client.service_account import ServiceAccountCredentials
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import TickerId, OrderId
import re
from dateutil import parser
import yfinance as yf
#from datetime import datetime, timedelta
import datetime
import threading
import random

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
                    print(f"Rate limit approaching. Waiting {sleep_time:.2f} seconds...")
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

# class DatabaseHandler:
#     def __init__(self, db_file='trading_data.db'):
#         self.db_file = db_file
#         self.conn = None
#         self.create_connection()
#         self.create_tables()

#     def export_to_sheet(self, detail_worksheet):
#         """Export data from SQLite to Google Sheet"""
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute("SELECT * FROM trades ORDER BY id DESC")
#             rows = cursor.fetchall()
            
#             # Skip the ID and timestamp columns when exporting
#             formatted_rows = []
#             for row in rows:
#                 # Extract only the columns needed for Google Sheets (excluding id and timestamp)
#                 formatted_row = row[1:14] # Indices 1-13 contain the trade data
#                 formatted_rows.append(formatted_row)
                
#             # Update Google Sheet with data from SQLite
#             if formatted_rows:
#                 # Add rows in batches to avoid API limits
#                 batch_size = 10  # Reduced batch size from 100 to 10
#                 for i in range(0, len(formatted_rows), batch_size):
#                     batch = formatted_rows[i:i+batch_size]
#                     for row_data in batch:
#                         detail_worksheet.append_row(row_data)
#                     time.sleep(1.2)  # Wait ~1.2 seconds between batches
                    
#                 print(f"Exported {len(formatted_rows)} rows to Google Sheet")
#             else:
#                 print("No data to export")
                
#             return True
#         except Error as e:
#             print(f"Error exporting to Google Sheet: {e}")
#             return False
        
#     def create_connection(self):
#         try:
#             self.conn = sqlite3.connect(self.db_file)
#             print(f"Connected to SQLite database {self.db_file}")
#         except Error as e:
#             print(f"Error connecting to database: {e}")
            
#     def create_tables(self):
#         try:
#             cursor = self.conn.cursor()
            
#             # Create trades table
#             trades_table = """
#             CREATE TABLE IF NOT EXISTS trades (
#                 id INTEGER PRIMARY KEY AUTOINCREMENT,
#                 date TEXT,
#                 ticker TEXT,
#                 net_units REAL,
#                 total_abs_units REAL,
#                 trade_price REAL,
#                 adj_close REAL,
#                 trade_type TEXT,
#                 pre_trade_position REAL,
#                 delta_shares REAL,
#                 post_trade_position REAL,
#                 commission REAL,
#                 interest REAL,
#                 nav REAL,
#                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP
#             );
#             """
            
#             # Create strategy_cash table
#             strategy_cash_table = """
#             CREATE TABLE IF NOT EXISTS strategy_cash (
#                 strategy_idx INTEGER,
#                 date TEXT,
#                 cash REAL,
#                 PRIMARY KEY (strategy_idx, date)
#             );
#             """
            
#             cursor.execute(trades_table)
#             cursor.execute(strategy_cash_table)
#             self.conn.commit()
#             print("Tables created successfully")
#         except Error as e:
#             print(f"Error creating tables: {e}")
            
    
#     def insert_trade(self, trade_data):
#         # Standardize the date format if it's the first element in trade_data
#         if trade_data and len(trade_data) > 0:
#             trade_data[0] = standardize_date_format(trade_data[0])
            
#         sql = """
#         INSERT INTO trades (
#             date, ticker, net_units, total_abs_units, trade_price, 
#             adj_close, trade_type, pre_trade_position, delta_shares, 
#             post_trade_position, commission, interest, nav
#         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
#         """
        
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute(sql, trade_data)
#             self.conn.commit()
#             return cursor.lastrowid
#         except Error as e:
#             print(f"Error inserting trade data: {e}")
#             return None
            
#     def get_recent_trades(self, limit=100):
#         sql = "SELECT * FROM trades ORDER BY id DESC LIMIT ?"
        
#         try:
#             cursor = self.conn.cursor()
#             cursor.execute(sql, (limit,))
#             return cursor.fetchall()
#         except Error as e:
#             print(f"Error fetching trades: {e}")
#             return []
class DatabaseHandler:
    def __init__(self, db_file='trading_data.db'):
        self.db_file = db_file
        self.conn = None
        self.create_connection()
        self.create_tables()

    def create_connection(self):
        try:
            self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
            self.lock = threading.Lock()  # Add a lock for thread safety
            print(f"Connected to SQLite database {self.db_file}")
        except Error as e:
            print(f"Error connecting to database: {e}")
            
    def create_tables(self):
        with self.lock:
            try:
                cursor = self.conn.cursor()
                
                # Create trades table
                trades_table = """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT,
                    ticker TEXT,
                    net_units REAL,
                    total_abs_units REAL,
                    trade_price REAL,
                    adj_close REAL,
                    trade_type TEXT,
                    pre_trade_position REAL,
                    delta_shares REAL,
                    post_trade_position REAL,
                    commission REAL,
                    interest REAL,
                    nav REAL,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                );
                """
                
                # Create strategy_cash table
                strategy_cash_table = """
                CREATE TABLE IF NOT EXISTS strategy_cash (
                    strategy_idx INTEGER,
                    date TEXT,
                    cash REAL,
                    PRIMARY KEY (strategy_idx, date)
                );
                """
                
                cursor.execute(trades_table)
                cursor.execute(strategy_cash_table)
                self.conn.commit()
                print("Tables created successfully")
            except Error as e:
                print(f"Error creating tables: {e}")
    
    def export_to_sheet(self, detail_worksheet):
        with self.lock:
            # Get current date in Eastern timezone
            eastern = pytz.timezone('US/Eastern')
            current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
            
            cursor = self.conn.cursor()
            # Modified query to filter by today's date
            cursor.execute("SELECT * FROM trades WHERE date = ? ORDER BY id DESC", (current_date,))
            rows = cursor.fetchall()
            
            # Skip the ID and timestamp columns when exporting
            formatted_rows = []
            for row in rows:
                # Extract only the columns needed for Google Sheets (excluding id and timestamp)
                formatted_row = row[1:14] # Indices 1-13 contain the trade data
                formatted_rows.append(formatted_row)
                
            # Add rows in batches to avoid API limits
            batch_size = 10  # Reduced batch size from 100 to 10
            for i in range(0, len(formatted_rows), batch_size):
                batch = formatted_rows[i:i+batch_size]
                for row_data in batch:
                    detail_worksheet.append_row(row_data)
                time.sleep(1.2)  # Wait ~1.2 seconds between batches
                
            print(f"Exported {len(formatted_rows)} rows to Google Sheet (today's data only)")
            return True
    
    def insert_trade(self, trade_data):
        with self.lock:
            # Standardize the date format if it's the first element in trade_data
            if trade_data and len(trade_data) > 0:
                trade_data[0] = standardize_date_format(trade_data[0])
                
            sql = """
            INSERT INTO trades (
                date, ticker, net_units, total_abs_units, trade_price, 
                adj_close, trade_type, pre_trade_position, delta_shares, 
                post_trade_position, commission, interest, nav
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, trade_data)
                self.conn.commit()
                return cursor.lastrowid
            except Error as e:
                print(f"Error inserting trade data: {e}")
                return None
                
    def get_recent_trades(self, limit=100):
        with self.lock:
            sql = "SELECT * FROM trades ORDER BY id DESC LIMIT ?"
            
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, (limit,))
                return cursor.fetchall()
            except Error as e:
                print(f"Error fetching trades: {e}")
                return []
    
    def store_strategy_cash(self, strategy_idx, date, cash):
        """Store cash value for a strategy on a given date"""
        with self.lock:
            # Standardize date format
            date = standardize_date_format(date)
            
            sql = """
            INSERT OR REPLACE INTO strategy_cash (strategy_idx, date, cash)
            VALUES (?, ?, ?);
            """
            
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, (strategy_idx, date, cash))
                self.conn.commit()
                return True
            except Error as e:
                print(f"Error storing strategy cash: {e}")
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
            except Error as e:
                print(f"Error getting previous day cash: {e}")
                return None
    

    
    def store_strategy_cash(self, strategy_idx, date, cash):
        """Store cash value for a strategy on a given date"""
        # Standardize date format
        date = standardize_date_format(date)
        
        sql = """
        INSERT OR REPLACE INTO strategy_cash (strategy_idx, date, cash)
        VALUES (?, ?, ?);
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, (strategy_idx, date, cash))
            self.conn.commit()
            return True
        except Error as e:
            print(f"Error storing strategy cash: {e}")
            return False
        
    def get_previous_day_cash(self, strategy_idx):
        """Get the most recent cash value for a strategy"""
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
        except Error as e:
            print(f"Error getting previous day cash: {e}")
            return None

           



class PositionTracker:
    def __init__(self, storage_file='positions.json'):
        self.storage_file = storage_file
        self.positions = self._load_positions()
        
    def _load_positions(self):
        try:
            with open(self.storage_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
            
    def save_positions(self):
        with open(self.storage_file, 'w') as f:
            json.dump(self.positions, f, indent=2)
            
    def get_position(self, ticker):
        return self.positions.get(ticker, 0)
        
    
    def update_position(self, ticker, delta):
        current = self.positions.get(ticker, 0)
        new_position = current + delta
        
        if new_position == 0:
            # If the position is now zero, remove the ticker from positions
            if ticker in self.positions:
                del self.positions[ticker]
        else:
            # Otherwise update with the new position
            self.positions[ticker] = new_position
        
        self.save_positions()


class TradingApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextOrderId = None
        self.order_status = {}
        self.execution_details = {}
        self.commission_details = {}
        self.account_summary = {}
        self.account_lock = threading.Lock()
        self.req_id_counter = 0
        self.connected = False
        self.pnl_data = {}
        self.pnl_single_data = {}
        
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextOrderId = orderId
        self.connected = True
        print(f"Connected to TWS. Next valid order ID: {orderId}")
        
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        print(f"Error {errorCode}: {errorString}")
    
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                   remaining: float, avgFillPrice: float, permId: int,
                   parentId: int, lastFillPrice: float, clientId: int,
                   whyHeld: str, mktCapPrice: float):
        self.order_status[orderId] = {
            'status': status,
            'filled': filled,
            'remaining': remaining,
            'avgFillPrice': avgFillPrice
        }
        
    def execDetails(self, reqId: int, contract: Contract, execution):
        self.execution_details[execution.orderId] = {
            'execId': execution.execId,
            'time': execution.time,
            'acctNumber': execution.acctNumber,
            'shares': execution.shares,
            'price': execution.price
        }
        
    def commissionReport(self, commissionReport):
        self.commission_details[commissionReport.execId] = {
            'commission': commissionReport.commission,
            'currency': commissionReport.currency,
            'realizedPNL': commissionReport.realizedPNL
        }
        
    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        with self.account_lock:
            if account not in self.account_summary:
                self.account_summary[account] = {}
            if tag not in self.account_summary[account]:
                self.account_summary[account][tag] = {}
            self.account_summary[account][tag] = {
                'value': value,
                'currency': currency
            }
            
    def accountSummaryEnd(self, reqId: int):
        print(f"Account summary request {reqId} completed")
        
    def pnl(self, reqId, dailyPnL, unrealizedPnL, realizedPnL):
        """Handle PnL updates for the account"""
        self.pnl_data = {
            'dailyPnL': dailyPnL,
            'unrealizedPnL': unrealizedPnL,
            'realizedPnL': realizedPnL,
            'timestamp': datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
        }
        print(f"PnL Update: Daily={dailyPnL}, Unrealized={unrealizedPnL}, Realized={realizedPnL}")
    
    def pnlSingle(self, reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value):
        """Handle PnL updates for a single position"""
        if reqId not in self.pnl_single_data:
            self.pnl_single_data[reqId] = {}
        
        self.pnl_single_data[reqId] = {
            'position': pos,
            'dailyPnL': dailyPnL,
            'unrealizedPnL': unrealizedPnL,
            'realizedPnL': realizedPnL,
            'value': value,
            'timestamp': datetime.datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")
        }


        
    def place_order(self, action, quantity, symbol, order_type="MOC",tif="DAY"):
        if not self.connected or self.nextOrderId is None:
            print("Not connected to TWS or no valid order ID")
            return None
            
        # Create contract
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        # Create order
        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = order_type
        order.tif = tif
        order.eTradeOnly = ""
        order.firmQuoteOnly = ""
        
        # Place order
        order_id = self.nextOrderId
        self.nextOrderId += 1
        self.placeOrder(order_id, contract, order)
        return order_id
    
    def get_next_req_id(self):
        self.req_id_counter += 1
        return self.req_id_counter
        
    def request_account_summary(self):
        req_id = self.get_next_req_id()
        # Updated to include more account details
        self.reqAccountSummary(req_id, "All", "NetLiquidation,AccruedCash,AvailableFunds,TotalCashValue")
        return req_id
        
    def request_pnl(self, account=""):
        """Request PnL updates for an account"""
        if not account:
            # Get the first account from account_summary
            for acct in self.account_summary:
                account = acct
                break
        
        if account:
            req_id = self.get_next_req_id()
            self.reqPnL(req_id, account, "")
            return req_id
        return None

    def request_position_pnl(self, account, conId):
        """Request PnL updates for a specific position"""
        if account:
            req_id = self.get_next_req_id()
            self.reqPnLSingle(req_id, account, "", conId)
            return req_id
        return None


def connect_to_google_sheets(rate_limited_client, sheet_url=None, sheet_name=None):
    try:
        if sheet_url:
            worksheet = rate_limited_client.open_by_url(sheet_url).sheet1
        elif sheet_name:
            worksheet = rate_limited_client.open(sheet_name).sheet1
        else:
            return None, None, "No sheet URL or name provided"
        
        data = worksheet.get_all_records()
        return worksheet, data, None
    except Exception as e:
        return None, None, str(e)

# def fetch_adjusted_closing_prices(tickers, current_date):
#     """
#     Fetch adjusted closing prices for a list of tickers using Yahoo Finance.
    
#     Args:
#         tickers (list): List of ticker symbols
#         current_date (str): Current date in YYYY-MM-DD format
        
#     Returns:
#         dict: Dictionary mapping tickers to their adjusted closing prices
#     """
#     if not tickers:
#         return {}
        
#     # Convert current_date string to datetime object
#     try:
#         date_obj = datetime.datetime.strptime(current_date, "%Y-%m-%d")
#     except ValueError:
#         print(f"Invalid date format: {current_date}")
#         return {}
    
#     # Set end date as current_date and start date as 5 days before
#     # (to handle weekends and holidays)
#     end_date = date_obj
#     start_date = end_date - datetime.timedelta(days=5)
    
#     # Format dates for yfinance
#     start_str = start_date.strftime("%Y-%m-%d")
#     end_str = end_date.strftime("%Y-%m-%d")
    
#     # Create a dictionary to store results
#     ticker_prices = {}
    
#     try:
#         # Download data for all tickers at once
#         data = yf.download(tickers, start=start_str, end=end_str, progress=False)
        
#         # If only one ticker is requested, the structure is different
#         if len(tickers) == 1:
#             if 'Adj Close' in data.columns:
#                 # Get the most recent adjusted close price
#                 ticker_prices[tickers[0]] = data['Adj Close'].iloc[-1]
#             else:
#                 # If Adj Close is not available, calculate it from Close and adjustments
#                 ticker_prices[tickers[0]] = calculate_adjusted_close(data, tickers[0])
#         else:
#             # For multiple tickers
#             if 'Adj Close' in data.columns:
#                 # Get the most recent adjusted close for each ticker
#                 for ticker in tickers:
#                     try:
#                         ticker_prices[ticker] = data['Adj Close'][ticker].iloc[-1]
#                     except (KeyError, IndexError):
#                         print(f"No data found for {ticker}")
#             else:
#                 # If Adj Close is not available, calculate it
#                 for ticker in tickers:
#                     ticker_prices[ticker] = calculate_adjusted_close(data, ticker)
    
#     except Exception as e:
#         print(f"Error fetching adjusted closing prices: {str(e)}")
    
#     return ticker_prices

def fetch_adjusted_closing_prices(tickers, current_date):
    """
    Fetch adjusted closing prices for a list of tickers using Yahoo Finance.
    
    Args:
        tickers (list): List of ticker symbols
        current_date (str): Date in YYYY-MM-DD format
        
    Returns:
        dict: Mapping of tickers to their most recent adjusted closing price up to that date
    """
    if not tickers:
        return {}
    
    # Convert date string to datetime
    try:
        date_obj = datetime.datetime.strptime(current_date, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {current_date}")
        return {}
    
    # Set range (to cover weekends/holidays)
    start_date = date_obj - datetime.timedelta(days=7)
    end_date = date_obj + datetime.timedelta(days=1)  # include the date itself

    ticker_prices = {}

    try:
        # Download in batch (auto_adjust=True by default in latest yfinance)
        data = yf.download(
            tickers,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            progress=False
        )

        if data.empty:
            print("No data retrieved for any ticker.")
            return {ticker: None for ticker in tickers}

        # Single ticker returns a different shape
        if len(tickers) == 1:
            ticker = tickers[0]
            if 'Close' in data.columns:
                price_series = data['Close'].dropna()
            else:
                print(f"No 'Close' column found for {ticker}")
                return {ticker: None}

            if not price_series.empty:
                ticker_prices[ticker] = price_series.iloc[-1]
            else:
                print(f"No data found for {ticker}")
                ticker_prices[ticker] = None

        else:
            # Multiple tickers case
            close_prices = data['Close']
            for ticker in tickers:
                try:
                    price_series = close_prices[ticker].dropna()
                    if not price_series.empty:
                        ticker_prices[ticker] = price_series.iloc[-1]
                    else:
                        print(f"No data found for {ticker}")
                        ticker_prices[ticker] = None
                except (KeyError, IndexError):
                    print(f"No data found for {ticker}")
                    ticker_prices[ticker] = None

    except Exception as e:
        print(f"Error fetching prices: {str(e)}")
        return {ticker: None for ticker in tickers}

    return ticker_prices

def get_strategy_nav(rate_limited_client, sheet_url):
    """
    Get the latest NAV value from a strategy's Daily NAV sheet.
    """
    try:
        spreadsheet = rate_limited_client.open_by_url(sheet_url)
        
        # Get the Daily NAV sheet (3rd sheet)
        try:
            nav_sheet = spreadsheet.get_worksheet(2)
            if nav_sheet is None:
                return 100000  # Default if sheet doesn't exist
        except:
            return 100000
            
        # Get values from the sheet
        values = nav_sheet.get_all_values()
        
        # Skip header row and get the latest entry
        if len(values) > 1:
            latest_row = values[-1]
            if len(latest_row) >= 2:  # We need at least Date and NAV columns
                try:
                    return float(latest_row[1])
                except (ValueError, TypeError):
                    pass
                    
        return 100000  # Default if no valid NAV found
        
    except Exception as e:
        print(f"Error getting latest NAV for {sheet_url}: {str(e)}")
        return 100000


def update_combined_metrics_sheet(rate_limited_client, sheet_urls, app, individual_trackers, db_handler):
    """
    Create or update a tab that combines metrics across all strategies and adds IBKR account data.
    """
    try:
        # Change from using the first strategy sheet to your detailed worksheet
        # You need to specify the URL of your detailed worksheet here
        detailed_worksheet_url = "https://docs.google.com/spreadsheets/d/1rNq1lKYoGnZemMrIe73_hwgqRPrlmNgD6jQThvjvXZ4/edit?gid=0#gid=0"  # Replace with actual URL
        spreadsheet = rate_limited_client.open_by_url(detailed_worksheet_url)
        # spreadsheet = rate_limited_client.open_by_url(sheet_urls[0])
        
        # Check if the combined metrics sheet exists, create if not
        try:
            combined_sheet = spreadsheet.worksheet("Combined Metrics")
        except:
            combined_sheet = spreadsheet.add_worksheet(title="Combined Metrics", rows=1000, cols=12)
            
        # The rest of the function remains the same
        # Define headers
        headers = [
            'Date', 
            'Strategy 1 NAV', 'Strategy 2 NAV', 'Strategy 3 NAV',
            'Strategy 1 Cash', 'Strategy 2 Cash', 'Strategy 3 Cash',
            'Sum of NAVs', 'Sum of Cash', 
            'NAV IBKR', 'CASH IBKR'
        ]
        
        # Check if headers exist
        sheet_values = combined_sheet.get_all_values()
        if not sheet_values:
            combined_sheet.append_row(headers)
            time.sleep(1.2)  # Add delay after header creation
            
        # Get current date
        eastern = pytz.timezone('US/Eastern')
        current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
        
        # Fetch data from each strategy
        strategy_navs = []
        strategy_cash = []
        
        for i, url in enumerate(sheet_urls):
            if i >= 3:  # Only process up to 3 strategies
                break
                
            # Get strategy NAV and cash
            nav = get_strategy_nav(rate_limited_client, url)
            cash = db_handler.get_previous_day_cash(i) or 100000  # Default to 100000 if None
            
            strategy_navs.append(nav)
            strategy_cash.append(cash)
            
        # Pad strategy values to ensure we have 3 values even if fewer strategies
        while len(strategy_navs) < 3:
            strategy_navs.append(0)
        while len(strategy_cash) < 3:
            strategy_cash.append(0)
            
        # Calculate sums
        nav_sum = sum(strategy_navs)
        cash_sum = sum(strategy_cash)
        
        # Fetch IBKR data
        ibkr_nav = 0
        ibkr_cash = 0
        
        # Request account summary to refresh data
        app.request_account_summary()
        time.sleep(3)  # Wait for data to arrive
        
        # Extract NAV and cash values from account data
        for account, details in app.account_summary.items():
            if 'NetLiquidation' in details:
                nav_val = details['NetLiquidation'].get('value', '')
                ibkr_nav = float(nav_val) if nav_val and nav_val != '' else 0
                
            if 'TotalCashValue' in details:
                cash_val = details['TotalCashValue'].get('value', '')
                ibkr_cash = float(cash_val) if cash_val and cash_val != '' else 0
                
            # Only need to process one account
            break
        
        # Prepare row data
        row_data = [
            current_date,
            strategy_navs[0], strategy_navs[1], strategy_navs[2],
            strategy_cash[0], strategy_cash[1], strategy_cash[2],
            nav_sum, cash_sum,
            ibkr_nav, ibkr_cash
        ]
        
        # Add the row to the sheet
        combined_sheet.append_row(row_data)
        print(f"Updated Combined Metrics sheet with data for {current_date}")
        
    except Exception as e:
        print(f"Error updating Combined Metrics sheet: {str(e)}")


def calculate_adjusted_close(data, ticker):
    """
    Calculate adjusted close price if not directly available.
    This is a fallback method using Close, Dividends, and Stock Splits.
    
    Args:
        data: Yahoo Finance data
        ticker: Ticker symbol
        
    Returns:
        float: Calculated adjusted close price
    """
    try:
        # Get the most recent close price
        close_price = data['Close'][ticker].iloc[-1]
        
        # Apply adjustments for dividends and splits if available
        if 'Dividends' in data.columns and 'Stock Splits' in data.columns:
            # This is a simplified calculation - in reality, the adjustment
            # would need to account for the cumulative effect of all dividends and splits
            dividends = data['Dividends'][ticker].sum()
            splits = data['Stock Splits'][ticker].prod() or 1  # Default to 1 if no splits
            
            # Adjust close price
            adjusted_close = (close_price - dividends) / splits
            return adjusted_close
        else:
            # If adjustment data is not available, return the close price
            return close_price
    
    except Exception as e:
        print(f"Error calculating adjusted close for {ticker}: {str(e)}")
        return 0.0


# def calculate_strategy_metrics(db_handler, strategy_sheet_data, trade_results, individual_trackers, strategy_idx, ticker_abs_shares_all=None):
#     """Calculate metrics for an individual strategy with improved NAV calculation"""
#     # Get individual tracker for this strategy
#     tracker = individual_trackers[strategy_idx]
    
#     # Get latest cash value from database or use initial $100,000
#     initial_cash = 100000
#     previous_day_cash = db_handler.get_previous_day_cash(strategy_idx)
#     if previous_day_cash is not None:
#         initial_cash = previous_day_cash
    
#     # Current date
#     eastern = pytz.timezone('US/Eastern')
#     current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    
#     # Get tickers in this strategy for current date
#     tickers_in_strategy = []
#     for row in strategy_sheet_data:
#         # Standardize the date for comparison
#         row_date = standardize_date_format(row['date'])
#         if row_date == current_date:
#             tickers_in_strategy.append(row['ticker'])
    
#     # Calculate absolute shares for this strategy
#     strategy_ticker_abs_shares = {}
#     for row in strategy_sheet_data:
#         # Standardize the date for comparison
#         row_date = standardize_date_format(row['date'])
#         if row_date == current_date:
#             ticker = row['ticker']
#             delta = row['target_position'] - row['pre_trade_position']
#             abs_shares = abs(delta)
#             strategy_ticker_abs_shares[ticker] = abs_shares
    
#     # Distribute commission based on proportion of absolute shares traded
#     ticker_commission = {}
#     for ticker, strategy_abs_shares in strategy_ticker_abs_shares.items():
#         if ticker in trade_results and ticker in ticker_abs_shares_all and ticker_abs_shares_all[ticker] > 0:
#             # Get the total commission for this ticker
#             total_ticker_commission = trade_results.get(ticker, {}).get('commission', 0)
#             # Calculate commission proportion based on this strategy's share of absolute shares
#             ticker_commission[ticker] = (strategy_abs_shares / ticker_abs_shares_all[ticker]) * total_ticker_commission
#         else:
#             ticker_commission[ticker] = 0
    
#     # Calculate cash after all trades
#     cash_after_trades = initial_cash
#     for row in strategy_sheet_data:
#         # Standardize the date for comparison
#         row_date = standardize_date_format(row['date'])
#         if row_date == current_date:
#             ticker = row['ticker']
#             delta = row['target_position'] - row['pre_trade_position']
#             price = 0
            
#             # Get price from trade_results
#             if ticker in trade_results:
#                 price = trade_results[ticker].get('price', 0)
            
#             # Subtract cost of shares bought/sold
#             cash_after_trades -= delta * price
            
#             # Subtract commission
#             if ticker in ticker_commission:
#                 cash_after_trades -= ticker_commission[ticker]
    
#     # Get prices for all tickers in the portfolio using Yahoo Finance
#     all_tickers = list(tracker.positions.keys())
#     ticker_prices = fetch_adjusted_closing_prices(all_tickers, current_date)
    
#     # Calculate NAV = Cash + (Position × Price) for ALL tickers in the portfolio
#     nav = cash_after_trades
#     for ticker, position in tracker.positions.items():
#         if position == 0:
#             continue  # Skip tickers with zero position
            
#         # First try to get price from today's trades
#         price = 0
#         if ticker in trade_results:
#             price = trade_results[ticker].get('price', 0)
        
#         # If not traded today, use adjusted closing price from Yahoo Finance
#         if price == 0 and ticker in ticker_prices:
#             price = ticker_prices[ticker]
            
#         # Add to NAV
#         nav += position * price
    
#     # Prepare metrics for each ticker
#     metrics = {}
#     for row in strategy_sheet_data:
#         # Standardize the date for comparison
#         row_date = standardize_date_format(row['date'])
#         if row_date == current_date:
#             ticker = row['ticker']
#             pre_trade_position = row['pre_trade_position']
#             target_position = row['target_position']
            
#             # Get price and delta from trade_results
#             price = 0
#             delta_shares = target_position - pre_trade_position
            
#             if ticker in trade_results:
#                 price = trade_results[ticker].get('price', 0)
            
#             metrics[ticker] = {
#                 'pre_trade_position': pre_trade_position,
#                 'post_trade_position': target_position,
#                 'price': price,
#                 'delta_shares': delta_shares,
#                 'commission': ticker_commission.get(ticker, 0),
#                 'cash': cash_after_trades,
#                 'nav': nav
#             }
    
#     # Store cash for next day
#     db_handler.store_strategy_cash(strategy_idx, current_date, cash_after_trades)
    
#     return metrics

def calculate_strategy_metrics(db_handler, strategy_sheet_data, trade_results, individual_trackers, 
                              strategy_idx, ticker_abs_shares_all=None):
    """Calculate metrics for an individual strategy with improved NAV calculation"""
    
    # Get individual tracker for this strategy
    tracker = individual_trackers[strategy_idx]
    
    # Get latest cash value from database or use initial $100,000
    initial_cash = 100000
    previous_day_cash = db_handler.get_previous_day_cash(strategy_idx)
    if previous_day_cash is not None:
        initial_cash = previous_day_cash

    # Current date
    eastern = pytz.timezone('US/Eastern')
    current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")

    # Get tickers in this strategy for current date
    tickers_in_strategy = []
    for row in strategy_sheet_data:
        # Standardize the date for comparison
        row_date = standardize_date_format(row['date'])
        if row_date == current_date:
            tickers_in_strategy.append(row['ticker'])

    # Get all current positions including those that haven't changed
    all_positions = {}
    
    # First add positions from tracker (these are existing positions)
    for ticker, position in tracker.positions.items():
        if position != 0:
            all_positions[ticker] = position
    
    # Then update with target positions from today's trades
    for row in strategy_sheet_data:
        row_date = standardize_date_format(row['date'])
        if row_date == current_date:
            ticker = row['ticker']
            all_positions[ticker] = row['target_position']

    # Calculate absolute shares for this strategy
    strategy_ticker_abs_shares = {}
    for row in strategy_sheet_data:
        row_date = standardize_date_format(row['date'])
        if row_date == current_date:
            ticker = row['ticker']
            delta = row['target_position'] - row['pre_trade_position']
            abs_shares = abs(delta)
            strategy_ticker_abs_shares[ticker] = abs_shares

    # Distribute commission based on proportion of absolute shares traded
    ticker_commission = {}
    for ticker, strategy_abs_shares in strategy_ticker_abs_shares.items():
        if ticker in trade_results and ticker in ticker_abs_shares_all and ticker_abs_shares_all[ticker] > 0:
            total_ticker_commission = trade_results.get(ticker, {}).get('commission', 0)
            ticker_commission[ticker] = (strategy_abs_shares / ticker_abs_shares_all[ticker]) * total_ticker_commission
        else:
            ticker_commission[ticker] = 0

    # Calculate cash after all trades
    cash_after_trades = initial_cash
    for row in strategy_sheet_data:
        row_date = standardize_date_format(row['date'])
        if row_date == current_date:
            ticker = row['ticker']
            delta = row['target_position'] - row['pre_trade_position']
            price = 0
            if ticker in trade_results:
                price = trade_results[ticker].get('price', 0)
            cash_after_trades -= delta * price
            if ticker in ticker_commission:
                cash_after_trades -= ticker_commission[ticker]

    # Check if we have no positions after today's trades
    no_positions = len(all_positions) == 0 or all(pos == 0 for pos in all_positions.values())
    
    if no_positions:
        # When no positions exist, NAV equals cash
        nav = cash_after_trades
        
        # Prepare a minimal metrics response
        metrics = {'_placeholder_': {
            'pre_trade_position': 0,
            'post_trade_position': 0,
            'price': 0,
            'delta_shares': 0,
            'commission': 0,
            'cash': cash_after_trades,
            'nav': nav
        }}
        
        # Store cash for next day
        db_handler.store_strategy_cash(strategy_idx, current_date, cash_after_trades)
        
        return metrics

    # Get prices for all tickers in the portfolio using Yahoo Finance
    all_tickers = list(all_positions.keys())
    ticker_prices = fetch_adjusted_closing_prices(all_tickers, current_date)

    # Calculate NAV = Cash + (Position × Price) for ALL tickers in the portfolio
    nav = cash_after_trades
    for ticker, position in all_positions.items():
        if position == 0:
            continue  # Skip tickers with zero position
        
        # First try to get price from today's trades
        price = 0
        if ticker in trade_results:
            price = trade_results[ticker].get('price', 0)
        
        # If not traded today, use adjusted closing price from Yahoo Finance
        if price == 0 and ticker in ticker_prices and ticker_prices[ticker] is not None:
            price = ticker_prices[ticker]
        
        # Add position value to NAV
        nav += position * price

    # Prepare metrics for each ticker
    metrics = {}
    
    # If we have no trades today but have positions, create metrics entries
    # for all positions using yahoo prices
    if not tickers_in_strategy and all_positions:
        for ticker, position in all_positions.items():
            price = 0
            if ticker in ticker_prices and ticker_prices[ticker] is not None:
                price = ticker_prices[ticker]
            
            metrics[ticker] = {
                'pre_trade_position': position,
                'post_trade_position': position,
                'price': price,
                'delta_shares': 0,
                'commission': 0,
                'cash': cash_after_trades,
                'nav': nav
            }
    else:
        # Normal case - we have trades today
        for row in strategy_sheet_data:
            row_date = standardize_date_format(row['date'])
            if row_date == current_date:
                ticker = row['ticker']
                pre_trade_position = row['pre_trade_position']
                target_position = row['target_position']
                
                price = 0
                delta_shares = target_position - pre_trade_position
                if ticker in trade_results:
                    price = trade_results[ticker].get('price', 0)
                elif ticker in ticker_prices and ticker_prices[ticker] is not None:
                    price = ticker_prices[ticker]
                
                metrics[ticker] = {
                    'pre_trade_position': pre_trade_position,
                    'post_trade_position': target_position,
                    'price': price,
                    'delta_shares': delta_shares,
                    'commission': ticker_commission.get(ticker, 0),
                    'cash': cash_after_trades,
                    'nav': nav
                }

    # Store cash for next day
    db_handler.store_strategy_cash(strategy_idx, current_date, cash_after_trades)
    
    return metrics




def combine_positions_from_sheets(rate_limited_client, sheet_urls, individual_trackers=None):
    combined_positions = {}
    individual_sheet_data = []
    
    # Use US Eastern time for current date
    eastern = pytz.timezone('US/Eastern')
    current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    
    # Create a mapping of tickers to strategies where they appear
    ticker_strategy_map = {}
    
    for i, url in enumerate(sheet_urls):
        worksheet, data, error = connect_to_google_sheets(rate_limited_client, sheet_url=url)
        
        if error:
            print(f"Error reading sheet {i+1}: {error}")
            continue
        
        # Store this sheet's data
        sheet_data = []
        for row in data:
            ticker = row.get('Ticker')
            target_position = row.get('Target Position', 0)
            trade_date = standardize_date_format(row.get('Date', ''))
            
            if not ticker:
                continue
                
            # Convert to int if possible
            try:
                target_position = int(target_position)
            except (ValueError, TypeError):
                print(f"Warning: Invalid Target Position for {ticker} in sheet {i+1}. Using 0.")
                target_position = 0
                
            # Store original row data plus any calculations
            pre_trade_position = 0
            if individual_trackers and i < len(individual_trackers):
                pre_trade_position = individual_trackers[i].get_position(ticker)
                
            # Map ticker to this strategy
            if ticker not in ticker_strategy_map:
                ticker_strategy_map[ticker] = []
            if i not in ticker_strategy_map[ticker]:
                ticker_strategy_map[ticker].append(i)
                
            sheet_data.append({
                'ticker': ticker,
                'target_position': target_position,
                'date': trade_date,
                'pre_trade_position': pre_trade_position
            })
            
        # Priority logic for combining positions:
        for row in sheet_data:
            ticker = row['ticker']
            target_position = row['target_position']
            trade_date = row['date']
            
            if ticker not in combined_positions:
                # Case 1: New ticker, add it
                combined_positions[ticker] = {
                    'Target Position': target_position,
                    'Date': trade_date
                }
            elif standardize_date_format(trade_date) == standardize_date_format(current_date):
                # Case 2: Current date entry found, prioritize it
                if standardize_date_format(combined_positions[ticker]['Date']) == standardize_date_format(current_date):
                    # Already had today's date, just add positions
                    combined_positions[ticker]['Target Position'] += target_position
                else:
                    # Replace older date with today's date
                    combined_positions[ticker] = {
                        'Target Position': target_position,
                        'Date': trade_date
                    }
            else:
                # Case 3: Older date entry, only update position if doesn't have today's date
                if standardize_date_format(combined_positions[ticker]['Date']) != standardize_date_format(current_date):
                    combined_positions[ticker]['Target Position'] += target_position
                    # Keep the most recent date if neither is today
                    if trade_date > combined_positions[ticker]['Date']:
                        combined_positions[ticker]['Date'] = trade_date
                        
        individual_sheet_data.append(sheet_data)
    
    # Convert to list of dicts for processing
    result = []
    for ticker, data in combined_positions.items():
        result.append({
            'Ticker': ticker,
            'Target Position': data['Target Position'],
            'Date': data['Date'],
            'Strategies': ticker_strategy_map.get(ticker, []) # Store which strategies this ticker belongs to
        })
        
    return result, individual_sheet_data, ticker_strategy_map


def execute_all_orders(app, combined_data, position_tracker, current_date,individual_trackers):
    orders_info = []
    
    # Get all tickers currently in the portfolio with non-zero positions
    current_portfolio_tickers = {ticker for ticker, position in position_tracker.positions.items() 
                               if position != 0}
    
    # Get all tickers from today's input sheet
    today_tickers = {trade['Ticker'] for trade in combined_data 
                    if standardize_date_format(trade.get('Date', '')) == current_date}
    
    # Process tickers in today's input sheet
    for i, trade in enumerate(combined_data):
        ticker = trade['Ticker']
        target_position = trade['Target Position']
        trade_date = standardize_date_format(trade.get('Date', ''))
        strategy_indices = trade.get('Strategies', [])
        
        # Skip if date doesn't match current date
        if trade_date != current_date:
            print(f"Skipping {ticker} - Trade date {trade_date} doesn't match current date {current_date}")
            continue
        
        # Calculate order details
        pre_trade_position = position_tracker.get_position(ticker)
        delta_shares = target_position - pre_trade_position
        
        if delta_shares == 0:
            print(f"Skipping {ticker} - No change in position required")
            continue
            
        action = "BUY" if delta_shares > 0 else "SELL"
        quantity = abs(delta_shares)
        
        print(f"Placing order {i+1}: {action} {quantity} {ticker}")
        
        try:
            order_id = app.place_order(action, quantity, ticker)
            
            if order_id is None:
                print(f" - Failed to place order")
                continue
                
            # Store order information for later processing
            orders_info.append({
                'order_id': order_id,
                'ticker': ticker,
                'action': action,
                'quantity': quantity,
                'pre_trade_position': pre_trade_position,
                'delta_shares': delta_shares,
                'trade_date': trade_date,
                'strategy_indices': strategy_indices
            })
            
            print(f" - Order {order_id} placed successfully")
            
        except Exception as e:
            print(f" - Error placing order: {str(e)}")
    
    # Liquidate positions for tickers not in today's input sheet
    tickers_to_liquidate = current_portfolio_tickers - today_tickers
    for j, ticker in enumerate(tickers_to_liquidate):
        pre_trade_position = position_tracker.get_position(ticker)
        
        # Skip if position is already zero
        if pre_trade_position == 0:
            continue
            
        # Determine which strategies have positions in this ticker
        strategy_indices = []
        for idx, tracker in enumerate(individual_trackers):
            strategy_position = tracker.get_position(ticker)
            if strategy_position != 0:
                strategy_indices.append(idx)
        
        # Liquidate the position
        action = "SELL" if pre_trade_position > 0 else "BUY"
        quantity = abs(pre_trade_position)
        delta_shares = -pre_trade_position  # Negative because we're removing the position
        
        print(f"Liquidating position {j+1}: {action} {quantity} {ticker} (Strategies: {strategy_indices})")
        
        try:
            order_id = app.place_order(action, quantity, ticker)
            
            if order_id is None:
                print(f" - Failed to place liquidation order")
                continue
                
            # Store order information for later processing
            orders_info.append({
                'order_id': order_id,
                'ticker': ticker,
                'action': action,
                'quantity': quantity,
                'pre_trade_position': pre_trade_position,
                'delta_shares': delta_shares,
                'trade_date': current_date,  # Use current date for liquidation orders
                'strategy_indices': strategy_indices  # Include strategies with positions
            })
            
            print(f" - Liquidation order {order_id} placed successfully")
            
        except Exception as e:
            print(f" - Error placing liquidation order: {str(e)}")
    
    return orders_info


def add_liquidation_rows_to_individual_data(individual_trackers, individual_sheet_data, current_date):
    """
    For each strategy, add a liquidation row for any ticker that was held yesterday but is not in today's input.
    The input sheet columns are: Date, Ticker, Target Position, Shares bought/sold, Price
    """
    # Standardize the current date format
    std_current_date = standardize_date_format(current_date)
    
    for i, tracker in enumerate(individual_trackers):
        held_tickers = set(ticker for ticker, position in tracker.positions.items() if position != 0)
        # Only tickers present for today in this strategy's sheet
        today_tickers = set(row['ticker'] for row in individual_sheet_data[i] 
                           if standardize_date_format(row['date']) == std_current_date)
        
        to_liquidate = held_tickers - today_tickers
        for ticker in to_liquidate:
            pre_trade_pos = tracker.get_position(ticker)
            if pre_trade_pos != 0:
                # Append a liquidation row
                individual_sheet_data[i].append({
                    'date': std_current_date,
                    'ticker': ticker,
                    'target_position': 0,
                    'pre_trade_position': pre_trade_pos,
                    'delta_shares': -pre_trade_pos  # To fully close the position
                })


# Retrieve order details after delay
# def retrieve_order_details(app, orders_info, position_tracker, individual_trackers,
#                            individual_sheet_data, ticker_strategy_map, db_handler,
#                            output_worksheet=None):
#     results = []
#     trade_results = {}
    
#     if not orders_info:
#         print("No orders were executed")
#         return results, trade_results
    
#     # Wait for processing time
#     wait_minutes = 20
#     print(f"All orders placed. Waiting {wait_minutes} minutes before retrieving details...")
#     time.sleep(wait_minutes * 60)
    
#     # Request account summary to refresh data
#     app.request_account_summary()
#     time.sleep(3)  # Wait a few seconds for data to arrive
    
#     # Process each order's details
#     for order_data in orders_info:
#         order_id = order_data['order_id']
#         ticker = order_data['ticker']
#         action = order_data['action']
#         quantity = order_data['quantity']
#         pre_trade_position = order_data['pre_trade_position']
#         delta_shares = order_data['delta_shares']
#         trade_date = order_data['trade_date']
#         strategy_indices = order_data['strategy_indices']
        
#         print(f"Retrieving details for order {order_id}: {action} {quantity} {ticker}")
        
#         # Collect execution details
#         status = 'Unknown'
#         price = 0
#         commission = 0
        
#         if order_id in app.order_status:
#             status = app.order_status[order_id]['status']
#             price = app.order_status[order_id].get('avgFillPrice', 0)
            
#             # If price is empty string, convert to 0
#             if price == '':
#                 price = 0
                
#             # Check for execution details
#             if order_id in app.execution_details:
#                 exec_id = app.execution_details[order_id].get('execId')
                
#                 # Try to get price from execution details if not available from order status
#                 if price == 0 and 'price' in app.execution_details[order_id]:
#                     price = app.execution_details[order_id]['price']
                    
#                 if exec_id and exec_id in app.commission_details:
#                     commission = app.commission_details[exec_id].get('commission', 0)
#                     if commission == '':
#                         commission = 0
        
#         # Add detailed logging for debugging            
#         print(f" - Order {order_id} status: {status}, price: {price}, commission: {commission}")
#         print(f" - Execution details available: {order_id in app.execution_details}")
#         if order_id in app.execution_details:
#             print(f" - Execution ID: {app.execution_details[order_id].get('execId')}")
#             print(f" - Commission details available: {app.execution_details[order_id].get('execId') in app.commission_details}")
                
#         # Get account summary data
#         nav = 0
#         interest = 0
        
#         for account, details in app.account_summary.items():
#             if 'NetLiquidation' in details:
#                 nav_val = details['NetLiquidation'].get('value', '')
#                 nav = float(nav_val) if nav_val and nav_val != '' else 0
#             if 'AccruedCash' in details:
#                 interest_val = details['AccruedCash'].get('value', '')
#                 interest = float(interest_val) if interest_val and interest_val != '' else 0
                
#         is_executed = status in ['Filled', 'Submitted', 'PreSubmitted']
        
#         # Store trade information for updating input sheets later
#         trade_results[ticker] = {
#             'price': price if price else 0,
#             'status': status,
#             'delta_shares': delta_shares if is_executed else 0,
#             'individual_deltas': [],  # Will store individual strategy deltas
#             'commission': commission  # Add commission to trade results
#         }
        
#         # After collecting execution details
#         if is_executed:
#             # Save to SQLite database
#             trade_data = [
#                 trade_date,
#                 ticker,
#                 float(delta_shares),  # Net Units
#                 float(abs(delta_shares)),  # Total Abs Units
#                 float(price),
#                 float(price),  # Using trade price as Adj Close
#                 action,
#                 float(pre_trade_position),
#                 float(delta_shares),
#                 float(pre_trade_position + delta_shares),  # Post-trade position
#                 float(commission),
#                 float(interest),
#                 float(nav)
#             ]
            
#             # Insert into SQLite
#             db_handler.insert_trade(trade_data)
            
#             # Update combined position
#             position_tracker.update_position(ticker, delta_shares)
#             post_trade_position = position_tracker.get_position(ticker)
            
#             # Update individual positions ONLY for strategies that include this ticker
#             for strategy_idx in strategy_indices:
#                 if strategy_idx < len(individual_trackers) and strategy_idx < len(individual_sheet_data):
#                     # Find the target position for this ticker in this strategy
#                     strategy_target = 0
#                     for row in individual_sheet_data[strategy_idx]:
#                         if row['ticker'] == ticker and row['date'] == trade_date:
#                             strategy_target = row['target_position']
#                             break
                            
#                     # Only update if there's a target position for this strategy
#                     if strategy_target != 0:
#                         # Get current position for this strategy
#                         indiv_pre_trade = individual_trackers[strategy_idx].get_position(ticker)
                        
#                         # Calculate the delta for this strategy
#                         indiv_delta = strategy_target - indiv_pre_trade
                        
#                         # Only update if there's a position change for this strategy
#                         if indiv_delta != 0:
#                             individual_trackers[strategy_idx].update_position(ticker, indiv_delta)
                            
#                         # Store the individual delta for this strategy
#                         trade_results[ticker]['individual_deltas'].append({
#                             'strategy_index': strategy_idx,
#                             'delta': indiv_delta,
#                             'pre_trade': indiv_pre_trade,
#                             'post_trade': individual_trackers[strategy_idx].get_position(ticker)
#                         })
#         else:
#             post_trade_position = pre_trade_position
            
#         # Update output worksheet (still keep this for immediate trade confirmation)
#         if output_worksheet and is_executed:
#             try:
#                 # Use US Eastern time for timestamp
#                 eastern = pytz.timezone('US/Eastern')
#                 current_timestamp = datetime.datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
                
#                 output_worksheet.append_row([
#                     current_timestamp,
#                     ticker,
#                     action,
#                     quantity,
#                     price,
#                     status
#                 ])
#                 print(f" - Updated output sheet for order {order_id}")
#             except Exception as e:
#                 print(f" - Error updating output sheet: {str(e)}")
                
#         results.append({
#             'ticker': ticker,
#             'action': action,
#             'quantity': quantity,
#             'status': status,
#             'price': price,
#             'commission': commission
#         })
        
#         print(f" - Order {order_id} final status: {status}")
    
#     return results, trade_results
def retrieve_order_details(app, orders_info, position_tracker, individual_trackers,
                           individual_sheet_data, ticker_strategy_map, db_handler,
                           output_worksheet=None):
    results = []
    trade_results = {}
    
    if not orders_info:
        print("No orders were executed")
        return results, trade_results
    
    # Wait for processing time
    wait_minutes = 10
    print(f"All orders placed. Waiting {wait_minutes} minutes before retrieving details...")
    time.sleep(wait_minutes * 60)
    
    # Request account summary to refresh data
    app.request_account_summary()
    time.sleep(3)  # Wait a few seconds for data to arrive
    
    # Process each order's details
    for order_data in orders_info:
        order_id = order_data['order_id']
        ticker = order_data['ticker']
        action = order_data['action']
        quantity = order_data['quantity']
        pre_trade_position = order_data['pre_trade_position']
        delta_shares = order_data['delta_shares']
        trade_date = order_data['trade_date']
        strategy_indices = order_data['strategy_indices']
        
        print(f"Retrieving details for order {order_id}: {action} {quantity} {ticker}")
        
        # Collect execution details
        status = 'Unknown'
        price = 0
        commission = 0
        
        if order_id in app.order_status:
            status = app.order_status[order_id]['status']
            price = app.order_status[order_id].get('avgFillPrice', 0)
            
            # If price is empty string, convert to 0
            if price == '':
                price = 0
                
            # Check for execution details
            if order_id in app.execution_details:
                exec_id = app.execution_details[order_id].get('execId')
                
                # Try to get price from execution details if not available from order status
                if price == 0 and 'price' in app.execution_details[order_id]:
                    price = app.execution_details[order_id]['price']
                    
                if exec_id and exec_id in app.commission_details:
                    commission = app.commission_details[exec_id].get('commission', 0)
                    if commission == '':
                        commission = 0
        
        # Add detailed logging for debugging            
        print(f" - Order {order_id} status: {status}, price: {price}, commission: {commission}")
        print(f" - Execution details available: {order_id in app.execution_details}")
        if order_id in app.execution_details:
            print(f" - Execution ID: {app.execution_details[order_id].get('execId')}")
            print(f" - Commission details available: {app.execution_details[order_id].get('execId') in app.commission_details}")
                
        # Get account summary data
        nav = 0
        interest = 0
        
        for account, details in app.account_summary.items():
            if 'NetLiquidation' in details:
                nav_val = details['NetLiquidation'].get('value', '')
                nav = float(nav_val) if nav_val and nav_val != '' else 0
            if 'AccruedCash' in details:
                interest_val = details['AccruedCash'].get('value', '')
                interest = float(interest_val) if interest_val and interest_val != '' else 0
                
        is_executed = status in ['Filled', 'Submitted', 'PreSubmitted']
        
        # Store trade information for updating input sheets later
        trade_results[ticker] = {
            'price': price if price else 0,
            'status': status,
            'delta_shares': delta_shares if is_executed else 0,
            'individual_deltas': [],  # Will store individual strategy deltas
            'commission': commission  # Add commission to trade results
        }
        
        # After collecting execution details
        if is_executed:
            # Save to SQLite database
            trade_data = [
                trade_date,
                ticker,
                float(delta_shares),  # Net Units
                float(abs(delta_shares)),  # Total Abs Units
                float(price),
                float(price),  # Using trade price as Adj Close
                action,
                float(pre_trade_position),
                float(delta_shares),
                float(pre_trade_position + delta_shares),  # Post-trade position
                float(commission),
                float(interest),
                float(nav)
            ]
            
            # Insert into SQLite
            db_handler.insert_trade(trade_data)
            
            # Update combined position
            position_tracker.update_position(ticker, delta_shares)
            post_trade_position = position_tracker.get_position(ticker)
            
            # Update individual positions ONLY for strategies that include this ticker
            for strategy_idx in strategy_indices:
                if strategy_idx < len(individual_trackers) and strategy_idx < len(individual_sheet_data):
                    # Find the target position for this ticker in this strategy
                    strategy_target = 0
                    for row in individual_sheet_data[strategy_idx]:
                        if row['ticker'] == ticker and row['date'] == trade_date:
                            strategy_target = row['target_position']
                            break
                            
                    # Get current position for this strategy
                    indiv_pre_trade = individual_trackers[strategy_idx].get_position(ticker)
                    
                    # Calculate the delta for this strategy
                    indiv_delta = strategy_target - indiv_pre_trade
                    
                    # Only update if there's a position change for this strategy
                    if indiv_delta != 0:
                        individual_trackers[strategy_idx].update_position(ticker, indiv_delta)
                        
                    # Store the individual delta for this strategy
                    trade_results[ticker]['individual_deltas'].append({
                        'strategy_index': strategy_idx,
                        'delta': indiv_delta,
                        'pre_trade': indiv_pre_trade,
                        'post_trade': individual_trackers[strategy_idx].get_position(ticker)
                    })
        else:
            post_trade_position = pre_trade_position
            
        # Update output worksheet (still keep this for immediate trade confirmation)
        if output_worksheet and is_executed:
            try:
                # Use US Eastern time for timestamp
                eastern = pytz.timezone('US/Eastern')
                current_timestamp = datetime.datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
                
                output_worksheet.append_row([
                    current_timestamp,
                    ticker,
                    action,
                    quantity,
                    price,
                    status
                ])
                print(f" - Updated output sheet for order {order_id}")
            except Exception as e:
                print(f" - Error updating output sheet: {str(e)}")
                
        results.append({
            'ticker': ticker,
            'action': action,
            'quantity': quantity,
            'status': status,
            'price': price,
            'commission': commission
        })
        
        print(f" - Order {order_id} final status: {status}")
    
    return results, trade_results


def standardize_date_format(date_str):
    """
    Convert date string from various formats to YYYY-MM-DD
    Handles formats like:
    - MM/DD/YYYY
    - DD/MM/YYYY
    - MM-DD-YYYY
    - DD-MM-YYYY
    - YYYY/MM/DD
    - YYYY-M-D (without leading zeros)
    - Month DD, YYYY
    Returns:
    Standardized YYYY-MM-DD or original string if conversion fails
    """
    if not date_str or not isinstance(date_str, str):
        return date_str
        
    # If already in YYYY-MM-DD format, return as is
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
        
    # Handle YYYY-M-D format (without leading zeros)
    if re.match(r'^\d{4}-\d{1,2}-\d{1,2}$', date_str):
        try:
            parts = date_str.split('-')
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            return f"{year:04d}-{month:02d}-{day:02d}"
        except:
            pass
            
    try:
        # Try with dateutil parser (handles most formats)
        parsed_date = parser.parse(date_str, dayfirst=False)
        return parsed_date.strftime("%Y-%m-%d")
    except:
        try:
            # Try again with day-first parsing (European format)
            parsed_date = parser.parse(date_str, dayfirst=True)
            return parsed_date.strftime("%Y-%m-%d")
        except:
            pass
            
    # Manual parsing for specific formats
    formats = [
        '%m/%d/%Y', '%d/%m/%Y',
        '%m-%d-%Y', '%d-%m-%Y',
        '%Y/%m/%d', '%Y.%m.%d',
        '%d.%m.%Y', '%m.%d.%Y',
        '%b %d, %Y', '%d %b %Y',
        '%B %d, %Y', '%d %B %Y'
    ]
    
    for fmt in formats:
        try:
            parsed_date = datetime.datetime.strptime(date_str, fmt)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue
            
    print(f"Warning: Could not convert date format: {date_str}")
    return date_str  # Return original if all conversions fail


def update_input_sheets(credentials_file, sheet_urls, trade_results, ticker_strategy_map, 
                       individual_sheet_data, individual_trackers, db_handler):
    """Update input sheets with batched API requests to avoid rate limits."""
    # Calculate total absolute shares for commission distribution
    ticker_abs_shares_all = {}
    
    for i, strategy_data in enumerate(individual_sheet_data):
        eastern = pytz.timezone('US/Eastern')
        current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
        
        for row in strategy_data:
            if row['date'] == current_date:
                ticker = row['ticker']
                delta = row['target_position'] - row['pre_trade_position']
                abs_shares = abs(delta)
                
                if ticker not in ticker_abs_shares_all:
                    ticker_abs_shares_all[ticker] = 0
                
                ticker_abs_shares_all[ticker] += abs_shares
    
    # Update each strategy sheet
    for i, url in enumerate(sheet_urls):
        try:
            # Open the spreadsheet
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
            client = gspread.authorize(creds)
            
            # Open spreadsheet by URL
            spreadsheet = client.open_by_url(url)
            
            # Get or create detail worksheet (2nd sheet)
            try:
                detail_worksheet = spreadsheet.get_worksheet(1)
                if not detail_worksheet:
                    detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=20)
            except:
                detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=20)
            
            # Define headers for detail worksheet
            detail_headers = [
                'Date', 'Ticker', 'Net Units', 'Total Abs Units', 'Trade Price',
                'Adj_Close', 'Trade Type', 'Pre Trade Position', 'Delta Shares',
                'Post Trade Position', 'Commission', 'Interest', 'NAV', 'Cash'
            ]
            
            # Check if headers exist
            detail_values = detail_worksheet.get_all_values()
            if not detail_values:
                detail_worksheet.append_row(detail_headers)
                
            # Calculate metrics for this strategy
            metrics = calculate_strategy_metrics(
                db_handler, 
                individual_sheet_data[i], 
                trade_results, 
                individual_trackers, 
                i,
                ticker_abs_shares_all
            )
            
            # Get current date and timestamp
            eastern = pytz.timezone('US/Eastern')
            current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
            current_timestamp = datetime.datetime.now(eastern).strftime("%H:%M:%S")
            
            # Store NAV for later use with Daily NAV sheet
            strategy_nav = 0
            
            # Prepare batch updates for detail worksheet
            detail_batch_updates = []
            # Prepare batch updates for first sheet
            first_sheet_updates = []
            # Prepare batch updates for balance sheet
            balance_sheet_updates = []
            
            # Add rows to the detail worksheet
            row_to_add = len(detail_values) + 1
            
            for ticker, ticker_metrics in metrics.items():
                delta_shares = ticker_metrics['delta_shares']
                trade_type = 'BUY' if delta_shares > 0 else 'SELL'
                if delta_shares == 0:
                    trade_type = ''
                
                trade_price = ticker_metrics['price']
                net_units = delta_shares
                total_abs_units = abs(delta_shares)
                
                # Prepare detail worksheet row
                detail_row = [
                    current_date,
                    ticker,
                    net_units,
                    total_abs_units,
                    trade_price,
                    trade_price,
                    trade_type,
                    ticker_metrics['pre_trade_position'],
                    net_units,
                    ticker_metrics['post_trade_position'],
                    ticker_metrics['commission'],
                    0,  # Interest
                    ticker_metrics['nav'],
                    ticker_metrics['cash']
                ]
                
                # Add to batch updates for detail worksheet
                detail_batch_updates.append({
                    'range': f'A{row_to_add}:N{row_to_add}',
                    'values': [detail_row]
                })
                row_to_add += 1
                
                print(f"Added detail row for {ticker} in strategy {i+1}")
                
                # Store strategy NAV from the first ticker (they're all the same)
                if strategy_nav == 0:
                    strategy_nav = ticker_metrics['nav']
                
                # Add to first sheet updates
                first_sheet_updates.append({
                    'ticker': ticker,
                    'delta_shares': delta_shares,
                    'trade_price': trade_price,
                    'trade_date': current_date
                })
                
                # Add to balance sheet updates
                mkt_value = ticker_metrics['post_trade_position'] * trade_price
                balance_sheet_updates.append({
                    'date': current_date,
                    'timestamp': current_timestamp,
                    'ticker': ticker,
                    'position': ticker_metrics['post_trade_position'],
                    'trade_price': trade_price,
                    'mkt_value': mkt_value,
                    'cash': ticker_metrics['cash'],
                    'nav': ticker_metrics['nav']
                })
            
            # Execute batch updates for detail worksheet
            if detail_batch_updates:
                # Split into smaller batches if needed
                for i in range(0, len(detail_batch_updates), 50):
                    batch_chunk = detail_batch_updates[i:i+50]
                    retry_with_backoff(detail_worksheet.batch_update, batch_chunk)
            
            # Update first sheet with batch updates
            if first_sheet_updates:
                batch_update_multiple_tickers(credentials_file, url, first_sheet_updates)
            
            # Update balance sheet with batch updates
            if balance_sheet_updates:
                batch_update_balance_sheet(credentials_file, url, balance_sheet_updates)
            
            # Update Daily NAV sheet
            update_daily_nav_sheet(credentials_file, url, current_date, strategy_nav)
                
        except Exception as e:
            print(f"Error updating sheet {i+1}: {str(e)}")
            continue


def update_input_sheets_with_rate_limiting(rate_limited_client, sheet_urls, trade_results, ticker_strategy_map,
                                         individual_sheet_data, individual_trackers, db_handler, ticker_abs_shares_all):
    """Update input sheets with batched API requests using rate limiting."""
    # Update each strategy sheet
    for i, url in enumerate(sheet_urls):
        try:
            # Open spreadsheet by URL
            spreadsheet = rate_limited_client.open_by_url(url)
            
            # Get or create detail worksheet (2nd sheet)
            try:
                detail_worksheet = spreadsheet.get_worksheet(1)
                if not detail_worksheet:
                    detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=20)
            except:
                detail_worksheet = spreadsheet.add_worksheet(title="Trade Details", rows=1000, cols=20)
                
            # Define headers for detail worksheet
            detail_headers = [
                'Date', 'Ticker', 'Net Units', 'Total Abs Units', 'Trade Price',
                'Adj_Close', 'Trade Type', 'Pre Trade Position', 'Delta Shares',
                'Post Trade Position', 'Commission', 'Interest', 'NAV', 'Cash'
            ]
            
            # Check if headers exist
            detail_values = detail_worksheet.get_all_values()
            if not detail_values:
                detail_worksheet.append_row(detail_headers)
                time.sleep(1.2)  # Add delay after header creation
                
            # Calculate metrics for this strategy
            metrics = calculate_strategy_metrics(
                db_handler,
                individual_sheet_data[i],
                trade_results,
                individual_trackers,
                i,
                ticker_abs_shares_all
            )
            
            # Get current date and timestamp
            eastern = pytz.timezone('US/Eastern')
            current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
            current_timestamp = datetime.datetime.now(eastern).strftime("%H:%M:%S")
            
            # Store NAV for later use with Daily NAV sheet
            strategy_nav = 0
            
            # Prepare batch updates for detail worksheet
            detail_batch_updates = []
            
            # Prepare batch updates for first sheet
            first_sheet_updates = []
            
            # Prepare batch updates for balance sheet
            balance_sheet_updates = []
            
            # Add rows to the detail worksheet
            row_to_add = len(detail_values) + 1
            for ticker, ticker_metrics in metrics.items():
                delta_shares = ticker_metrics['delta_shares']
                trade_type = 'BUY' if delta_shares > 0 else 'SELL'
                if delta_shares == 0:
                    trade_type = ''
                trade_price = ticker_metrics['price']
                net_units = delta_shares
                total_abs_units = abs(delta_shares)
                
                # Prepare detail worksheet row
                detail_row = [
                    current_date,
                    ticker,
                    net_units,
                    total_abs_units,
                    trade_price,
                    trade_price,
                    trade_type,
                    ticker_metrics['pre_trade_position'],
                    net_units,
                    ticker_metrics['post_trade_position'],
                    ticker_metrics['commission'],
                    0,  # Interest
                    ticker_metrics['nav'],
                    ticker_metrics['cash']
                ]
                
                # Add to batch updates for detail worksheet
                detail_batch_updates.append({
                    'range': f'A{row_to_add}:N{row_to_add}',
                    'values': [detail_row]
                })
                row_to_add += 1
                
                # Store strategy NAV from the first ticker (they're all the same)
                if strategy_nav == 0:
                    strategy_nav = ticker_metrics['nav']
                    
                # Add to first sheet updates
                first_sheet_updates.append({
                    'ticker': ticker,
                    'delta_shares': delta_shares,
                    'trade_price': trade_price,
                    'trade_date': current_date
                })
                
                # Add to balance sheet updates
                mkt_value = ticker_metrics['post_trade_position'] * trade_price
                balance_sheet_updates.append({
                    'date': current_date,
                    'timestamp': current_timestamp,
                    'ticker': ticker,
                    'position': ticker_metrics['post_trade_position'],
                    'trade_price': trade_price,
                    'mkt_value': mkt_value,
                    'cash': ticker_metrics['cash'],
                    'nav': ticker_metrics['nav']
                })
                
            # Execute batch updates for detail worksheet
            if detail_batch_updates:
                # Split into smaller batches if needed
                for i in range(0, len(detail_batch_updates), 10):  # Reduced batch size to 10
                    batch_chunk = detail_batch_updates[i:i+10]
                    retry_with_backoff(detail_worksheet.batch_update, batch_chunk)
                    time.sleep(1.2)  # Add delay between batches
                    
            # Update first sheet with batch updates
            if first_sheet_updates:
                batch_update_multiple_tickers_with_rate_limiting(rate_limited_client, url, first_sheet_updates)
                
            # Update balance sheet with batch updates
            if balance_sheet_updates:
                batch_update_balance_sheet_with_rate_limiting(rate_limited_client, url, balance_sheet_updates)
                
            # Update Daily NAV sheet
            update_daily_nav_sheet_with_rate_limiting(rate_limited_client, url, current_date, strategy_nav)
            
        except Exception as e:
            print(f"Error updating sheet {i+1}: {str(e)}")
            continue


def initialize_strategy_cash(db_handler, num_strategies):
    """Initialize cash records for all strategies if not already present"""
    for strategy_idx in range(num_strategies):
        previous_cash = db_handler.get_previous_day_cash(strategy_idx)
        
        if previous_cash is None:
            # Initialize with $100,000
            eastern = pytz.timezone('US/Eastern')
            current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
            db_handler.store_strategy_cash(strategy_idx, current_date, 100000)
            print(f"Initialized strategy {strategy_idx+1} with $100,000 cash")

def initialize_detailed_worksheet(detail_worksheet, headers):
    try:
        existing_values = detail_worksheet.get_all_values()
        if not existing_values:
            # Only add headers if the worksheet is completely empty
            detail_worksheet.append_row(headers)
            print("Initialized detailed sheet headers")
        else:
            print("Using existing detailed worksheet with data")
    except Exception as e:
        print(f"Error checking detail worksheet: {str(e)}")

def retry_with_backoff(func, *args, max_retries=5, initial_delay=1, **kwargs):
    """Retry a function with exponential backoff."""
    retries = 0
    delay = initial_delay
    
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if ("[429]" in str(e) or "Quota exceeded" in str(e)) and retries < max_retries:
                print(f"Rate limit exceeded. Retrying in {delay} seconds...")
                time.sleep(delay)
                retries += 1
                delay *= 2  # Exponential backoff
            else:
                raise e
    
    raise Exception(f"Failed after {max_retries} retries")



def update_strategy_first_sheet(rate_limited_client, sheet_url, ticker, delta_shares, trade_price, trade_date):
    """Update the first sheet of each strategy with trade details using batch updates."""
    try:
        # Open the spreadsheet
        spreadsheet = rate_limited_client.open_by_url(sheet_url)
        first_sheet = spreadsheet.get_worksheet(0)  # Get first worksheet
        
        # Find the row with the ticker and date
        all_values = first_sheet.get_all_values()
        header_row = all_values[0] if all_values else []
        
        # Find columns for Date and Ticker
        date_col = next((i for i, val in enumerate(header_row) if val.lower() == 'date'), -1)
        ticker_col = next((i for i, val in enumerate(header_row) if val.lower() == 'ticker'), -1)
        
        if date_col == -1 or ticker_col == -1:
            # Try alternate column names
            date_col = next((i for i, val in enumerate(header_row) if 'date' in val.lower()), -1)
            ticker_col = next((i for i, val in enumerate(header_row) if 'ticker' in val.lower()), -1)
            
            if date_col == -1 or ticker_col == -1:
                print(f"Could not find Date/Ticker columns in sheet: {sheet_url}")
                return
                
        # Find row with matching date and ticker
        row_index = None
        std_trade_date = standardize_date_format(trade_date)
        
        for i, row in enumerate(all_values[1:], start=2):  # Start at row 2 (1-indexed)
            if len(row) > max(date_col, ticker_col):
                sheet_date = standardize_date_format(row[date_col])
                if sheet_date == std_trade_date and row[ticker_col] == ticker:
                    row_index = i
                    break
                    
        if not row_index:
            print(f"No match for {ticker} on {trade_date} in sheet: {sheet_url}")
            return
            
        # Find/create columns for trade details
        shares_col = next((i+1 for i, val in enumerate(header_row)
                          if 'shares bought/sold' in val.lower()), None)
        price_col = next((i+1 for i, val in enumerate(header_row)
                         if 'price' in val.lower()), None)
                         
        # Create columns if needed
        batch_requests = []
        if not shares_col:
            shares_col = len(header_row) + 1
            batch_requests.append({
                'range': f'1:{shares_col}',
                'values': [['' for _ in range(shares_col-1)] + ["Shares bought/sold"]]
            })
            
        if not price_col:
            price_col = len(header_row) + 2 if shares_col == len(header_row) + 1 else len(header_row) + 1
            batch_requests.append({
                'range': f'1:{price_col}',
                'values': [['' for _ in range(price_col-1)] + ["Price"]]
            })
            
        # Create batch update for the actual values
        cell_updates = []
        
        # Add shares update
        a1_shares = gspread.utils.rowcol_to_a1(row_index, shares_col)
        cell_updates.append({
            'range': a1_shares,
            'values': [[delta_shares]]
        })
        
        # Add price update
        a1_price = gspread.utils.rowcol_to_a1(row_index, price_col)
        cell_updates.append({
            'range': a1_price,
            'values': [[trade_price]]
        })
        
        # Execute all updates in a single batch
        if batch_requests:
            retry_with_backoff(first_sheet.batch_update, batch_requests)
            
        if cell_updates:
            retry_with_backoff(first_sheet.batch_update, cell_updates)
            
        print(f"Updated trade details for {ticker} in first sheet")
        
    except Exception as e:
        print(f"Error updating first sheet: {str(e)}")


def batch_update_multiple_tickers(credentials_file, sheet_url, updates_data):
    """
    Batch update multiple tickers at once in the first sheet.
    
    Args:
        credentials_file: Path to credentials file
        sheet_url: URL of the Google Sheet
        updates_data: List of dictionaries with format:
                     [{'ticker': ticker, 'delta_shares': delta_shares, 
                       'trade_price': trade_price, 'trade_date': trade_date}, ...]
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_url(sheet_url)
        first_sheet = spreadsheet.get_worksheet(0)
        
        # Get all values and header row
        all_values = first_sheet.get_all_values()
        header_row = all_values[0] if all_values else []
        
        # Find columns for Date and Ticker
        date_col = next((i for i, val in enumerate(header_row) if val.lower() == 'date'), -1)
        ticker_col = next((i for i, val in enumerate(header_row) if val.lower() == 'ticker'), -1)
        
        if date_col == -1 or ticker_col == -1:
            # Try alternate column names
            date_col = next((i for i, val in enumerate(header_row) if 'date' in val.lower()), -1)
            ticker_col = next((i for i, val in enumerate(header_row) if 'ticker' in val.lower()), -1)
        
        if date_col == -1 or ticker_col == -1:
            print(f"Could not find Date/Ticker columns in sheet: {sheet_url}")
            return
        
        # Find/create columns for trade details
        shares_col = next((i+1 for i, val in enumerate(header_row) 
                         if 'shares bought/sold' in val.lower()), None)
        price_col = next((i+1 for i, val in enumerate(header_row) 
                        if 'price' in val.lower()), None)
        
        # Create columns if needed
        if not shares_col:
            shares_col = len(header_row) + 1
            first_sheet.update_cell(1, shares_col, "Shares bought/sold")
        
        if not price_col:
            price_col = len(header_row) + 2 if shares_col == len(header_row) + 1 else len(header_row) + 1
            first_sheet.update_cell(1, price_col, "Price")
        
        # Prepare batch updates
        batch_updates = []
        
        # Process each ticker update
        for update in updates_data:
            ticker = update['ticker']
            delta_shares = update['delta_shares']
            trade_price = update['trade_price']
            trade_date = update['trade_date']
            
            # Find row with matching date and ticker
            row_index = None
            for i, row in enumerate(all_values[1:], start=2):
                if (len(row) > max(date_col, ticker_col) and 
                    row[date_col] == trade_date and 
                    row[ticker_col] == ticker):
                    row_index = i
                    break
            
            if not row_index:
                print(f"No match for {ticker} on {trade_date} in sheet: {sheet_url}")
                continue
            
            # Add shares update to batch
            shares_a1 = gspread.utils.rowcol_to_a1(row_index, shares_col)
            batch_updates.append({
                'range': shares_a1,
                'values': [[delta_shares]]
            })
            
            # Add price update to batch
            price_a1 = gspread.utils.rowcol_to_a1(row_index, price_col)
            batch_updates.append({
                'range': price_a1,
                'values': [[trade_price]]
            })
        
        # Execute all updates in a single batch
        if batch_updates:
            retry_with_backoff(first_sheet.batch_update, batch_updates)
            print(f"Updated {len(updates_data)} tickers in first sheet")
        else:
            print("No updates to perform")
            
    except Exception as e:
        print(f"Error updating sheet: {str(e)}")


def batch_update_multiple_tickers_with_rate_limiting(rate_limited_client, sheet_url, updates_data):
    """Batch update multiple tickers with rate limiting."""
    try:
        # Open the spreadsheet
        spreadsheet = rate_limited_client.open_by_url(sheet_url)
        first_sheet = spreadsheet.get_worksheet(0)
        
        # Get all values and header row
        all_values = first_sheet.get_all_values()
        header_row = all_values[0] if all_values else []
        
        # Find columns for Date and Ticker
        date_col = next((i for i, val in enumerate(header_row) if val.lower() == 'date'), -1)
        ticker_col = next((i for i, val in enumerate(header_row) if val.lower() == 'ticker'), -1)
        target_pos_col = next((i for i, val in enumerate(header_row) if 'target position' in val.lower()), -1)
        
        if date_col == -1 or ticker_col == -1:
            # Try alternate column names
            date_col = next((i for i, val in enumerate(header_row) if 'date' in val.lower()), -1)
            ticker_col = next((i for i, val in enumerate(header_row) if 'ticker' in val.lower()), -1)
            target_pos_col = next((i for i, val in enumerate(header_row) if 'target' in val.lower()), -1)
            
            if date_col == -1 or ticker_col == -1:
                print(f"Could not find Date/Ticker columns in sheet: {sheet_url}")
                return
                
        # Find/create columns for trade details
        shares_col = next((i+1 for i, val in enumerate(header_row)
                          if 'shares bought/sold' in val.lower()), None)
        price_col = next((i+1 for i, val in enumerate(header_row)
                         if 'price' in val.lower()), None)
                         
        # Create columns if needed
        if not shares_col:
            shares_col = len(header_row) + 1
            first_sheet.update_cell(1, shares_col, "Shares bought/sold")
            time.sleep(1.2)  # Add delay to avoid rate limits
            
        if not price_col:
            price_col = len(header_row) + 2 if shares_col == len(header_row) + 1 else len(header_row) + 1
            first_sheet.update_cell(1, price_col, "Price")
            time.sleep(1.2)  # Add delay to avoid rate limits
            
        # Prepare batch updates
        batch_updates = []
        
        # Process each ticker update
        for update in updates_data:
            ticker = update['ticker']
            delta_shares = update['delta_shares']
            trade_price = update['trade_price']
            trade_date = update['trade_date']
            
            # Standardize the trade date for comparison
            std_trade_date = standardize_date_format(trade_date)
            
            # Find row with matching date and ticker
            row_index = None
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) > max(date_col, ticker_col):
                    # Standardize the date from the sheet for comparison
                    sheet_date = standardize_date_format(row[date_col])
                    
                    if sheet_date == std_trade_date and row[ticker_col] == ticker:
                        row_index = i
                        break
                    
            # If no row exists, create a new row for this liquidated ticker
            if not row_index:
                print(f"No row found for {ticker} on {trade_date}. Creating new row for liquidated position.")
                
                # Create a new row at the end of the sheet
                new_row = [""] * len(header_row)
                new_row[date_col] = trade_date
                new_row[ticker_col] = ticker
                
                # If target position column exists, set it to 0 (liquidated)
                if target_pos_col != -1:
                    new_row[target_pos_col] = 0
                
                # Append the new row
                first_sheet.append_row(new_row)
                time.sleep(1.2)  # Add delay to avoid rate limits
                
                # Get updated values to find the new row index
                all_values = first_sheet.get_all_values()
                row_index = len(all_values)  # The new row is at the end
            
            # Add shares update to batch
            shares_a1 = gspread.utils.rowcol_to_a1(row_index, shares_col)
            batch_updates.append({
                'range': shares_a1,
                'values': [[delta_shares]]
            })
            
            # Add price update to batch
            price_a1 = gspread.utils.rowcol_to_a1(row_index, price_col)
            batch_updates.append({
                'range': price_a1,
                'values': [[trade_price]]
            })
            
        # Execute all updates in smaller batches
        if batch_updates:
            for i in range(0, len(batch_updates), 10):  # Process in batches of 10
                batch_chunk = batch_updates[i:i+10]
                retry_with_backoff(first_sheet.batch_update, batch_chunk)
                time.sleep(1.2)  # Add delay between batches
                
            print(f"Updated {len(updates_data)} tickers in first sheet")
        else:
            print("No updates to perform")
            
    except Exception as e:
        print(f"Error updating sheet: {str(e)}")


def update_daily_nav_sheet(credentials_file, sheet_url, date, nav):
    """Create and update a 3rd sheet for tracking daily NAV using batch updates."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_url(sheet_url)
        
        # Check if Daily NAV sheet exists, create if not
        try:
            nav_sheet = spreadsheet.worksheet("Daily NAV")
        except:
            nav_sheet = spreadsheet.add_worksheet(title="Daily NAV", rows=1000, cols=3)
            
            # Add headers in batch
            header_update = {
                'range': 'A1:B1',
                'values': [["Date", "Daily NAV"]]
            }
            retry_with_backoff(nav_sheet.batch_update, [header_update])
        
        # Check if date already exists in sheet
        dates = nav_sheet.col_values(1)
        
        # Prepare update
        if date in dates:
            # Update existing row
            row_idx = dates.index(date) + 1  # +1 because sheets are 1-indexed
            update_data = {
                'range': f'B{row_idx}',
                'values': [[nav]]
            }
        else:
            # Add new row
            update_data = {
                'range': f'A{len(dates) + 1}:B{len(dates) + 1}',
                'values': [[date, nav]]
            }
        
        # Execute update
        retry_with_backoff(nav_sheet.batch_update, [update_data])
        
        print(f"Updated Daily NAV sheet for {date}")
    except Exception as e:
        print(f"Error updating Daily NAV sheet: {str(e)}")


def update_daily_nav_sheet_with_rate_limiting(rate_limited_client, sheet_url, date, nav):
    """Create and update a 3rd sheet for tracking daily NAV with rate limiting."""
    try:
        # Open the spreadsheet
        spreadsheet = rate_limited_client.open_by_url(sheet_url)
        
        # Check if Daily NAV sheet exists, create if not
        try:
            nav_sheet = spreadsheet.worksheet("Daily NAV")
        except:
            nav_sheet = spreadsheet.add_worksheet(title="Daily NAV", rows=1000, cols=3)
            
        # Add headers in batch
        header_update = {
            'range': 'A1:B1',
            'values': [["Date", "Daily NAV"]]
        }
        
        retry_with_backoff(nav_sheet.batch_update, [header_update])
        time.sleep(1.2)  # Add delay to avoid rate limits
        
        # Check if date already exists in sheet
        dates = nav_sheet.col_values(1)
        
        # Prepare update
        if date in dates:
            # Update existing row
            row_idx = dates.index(date) + 1  # +1 because sheets are 1-indexed
            update_data = {
                'range': f'B{row_idx}',
                'values': [[nav]]
            }
        else:
            # Add new row
            update_data = {
                'range': f'A{len(dates) + 1}:B{len(dates) + 1}',
                'values': [[date, nav]]
            }
            
        # Execute update
        retry_with_backoff(nav_sheet.batch_update, [update_data])
        time.sleep(1.2)  # Add explicit delay after update
        print(f"Updated Daily NAV sheet for {date}")
        
    except Exception as e:
        print(f"Error updating Daily NAV sheet: {str(e)}")



def update_balance_sheet(credentials_file, sheet_url, date, timestamp, ticker, position, 
                         trade_price, mkt_value, cash, nav):
    """Create and update a 4th sheet for balance sheet details using batch updates."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_url(sheet_url)
        
        # Check if Balance Sheet exists, create if not
        try:
            balance_sheet = spreadsheet.worksheet("Balance Sheet")
        except:
            balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=8)
            
            # Add headers in batch
            header_update = {
                'range': 'A1:H1',
                'values': [[
                    "Date", "Timestamp", "Ticker", "Position", "Trade Price", 
                    "MKT Value", "Cash", "NAV"
                ]]
            }
            retry_with_backoff(balance_sheet.batch_update, [header_update])
        
        # Add new row with balance data
        row_data = {
            'range': f'A{balance_sheet.row_count + 1}:H{balance_sheet.row_count + 1}',
            'values': [[
                date, timestamp, ticker, position, trade_price, 
                mkt_value, cash, nav
            ]]
        }
        
        retry_with_backoff(balance_sheet.batch_update, [row_data])
        
        print(f"Updated Balance Sheet for {ticker} on {date}")
    except Exception as e:
        print(f"Error updating Balance Sheet: {str(e)}")

def batch_update_balance_sheet(credentials_file, sheet_url, balance_updates):
    """
    Batch update balance sheet for multiple tickers in a single request.
    
    Args:
        credentials_file: Path to credentials file
        sheet_url: URL of the Google Sheet
        balance_updates: List of dictionaries with format:
                        [{'date': date, 'timestamp': timestamp, 'ticker': ticker,
                          'position': position, 'trade_price': trade_price,
                          'mkt_value': mkt_value, 'cash': cash, 'nav': nav}, ...]
    """
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_url(sheet_url)
        
        # Check if Balance Sheet exists, create if not
        try:
            balance_sheet = spreadsheet.worksheet("Balance Sheet")
        except:
            balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=8)
            
            # Add headers
            header_update = {
                'range': 'A1:H1',
                'values': [[
                    "Date", "Timestamp", "Ticker", "Position", "Trade Price", 
                    "MKT Value", "Cash", "NAV"
                ]]
            }
            retry_with_backoff(balance_sheet.batch_update, [header_update])
        
        # Get current row count
        all_values = balance_sheet.get_all_values()
        current_row = len(all_values) + 1
        
        # Prepare batch updates
        batch_updates = []
        
        for update in balance_updates:
            row_data = {
                'range': f'A{current_row}:H{current_row}',
                'values': [[
                    update['date'],
                    update['timestamp'],
                    update['ticker'],
                    update['position'],
                    update['trade_price'],
                    update['mkt_value'],
                    update['cash'],
                    update['nav']
                ]]
            }
            batch_updates.append(row_data)
            current_row += 1
        
        # Execute batch update
        if batch_updates:
            # Split into groups of 50 to avoid exceeding request size limits
            for i in range(0, len(batch_updates), 50):
                batch_chunk = batch_updates[i:i+50]
                retry_with_backoff(balance_sheet.batch_update, batch_chunk)
            
            print(f"Updated Balance Sheet with {len(balance_updates)} entries")
        else:
            print("No balance updates to perform")
            
    except Exception as e:
        print(f"Error updating Balance Sheet: {str(e)}")


def batch_update_balance_sheet_with_rate_limiting(rate_limited_client, url, updates_data):
    """Batch update balance sheet with rate limiting."""
    try:
        # Open the spreadsheet
        spreadsheet = rate_limited_client.open_by_url(url)
        
        # Get or create balance sheet (4th sheet)
        try:
            balance_sheet = spreadsheet.get_worksheet(3)  # 0-indexed, so 3 is the 4th sheet
            if not balance_sheet:
                balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=20)
        except:
            balance_sheet = spreadsheet.add_worksheet(title="Balance Sheet", rows=1000, cols=20)
            
        # Define headers for balance sheet
        balance_headers = [
            'Date', 'Time', 'Ticker', 'Position', 'Price', 'Market Value', 'Cash', 'NAV'
        ]
        
        # Check if headers exist
        balance_values = balance_sheet.get_all_values()
        header_row = balance_values[0] if balance_values else []
        
        if not balance_values or len(header_row) < len(balance_headers):
            balance_sheet.append_row(balance_headers)
            time.sleep(1.2)  # Add delay after header creation
            header_row = balance_headers
            balance_values = [header_row]
            
        # Find column indices
        date_col = next((i for i, val in enumerate(header_row) if val.lower() == 'date'), 0)
        time_col = next((i for i, val in enumerate(header_row) if val.lower() == 'time'), 1)
        ticker_col = next((i for i, val in enumerate(header_row) if val.lower() == 'ticker'), 2)
        position_col = next((i for i, val in enumerate(header_row) if val.lower() == 'position'), 3)
        price_col = next((i for i, val in enumerate(header_row) if val.lower() == 'price'), 4)
        mkt_value_col = next((i for i, val in enumerate(header_row) if 'market value' in val.lower() or 'mkt value' in val.lower()), 5)
        cash_col = next((i for i, val in enumerate(header_row) if val.lower() == 'cash'), 6)
        nav_col = next((i for i, val in enumerate(header_row) if val.lower() == 'nav'), 7)
        
        # Prepare batch updates
        batch_updates = []
        
        # Process each update
        for update in updates_data:
            date = update['date']
            timestamp = update.get('timestamp', '')
            ticker = update['ticker']
            position = update['position']
            trade_price = update['trade_price']
            mkt_value = update['mkt_value']
            cash = update['cash']
            nav = update['nav']
            
            # Standardize date for comparison
            std_date = standardize_date_format(date)
            
            # Find row with matching date and ticker
            row_index = None
            for i, row in enumerate(balance_values[1:], start=2):  # Start at row 2 (1-indexed)
                if len(row) > max(date_col, ticker_col):
                    sheet_date = standardize_date_format(row[date_col])
                    if sheet_date == std_date and row[ticker_col] == ticker:
                        row_index = i
                        break
                        
            # If no row exists, create a new row for this ticker (including liquidated ones)
            if not row_index:
                # Create a new row at the end of the sheet
                new_row = [""] * len(header_row)
                new_row[date_col] = date
                new_row[time_col] = timestamp
                new_row[ticker_col] = ticker
                
                # Append the new row
                balance_sheet.append_row(new_row)
                time.sleep(1.2)  # Add delay to avoid rate limits
                
                # Get updated values to find the new row index
                balance_values = balance_sheet.get_all_values()
                row_index = len(balance_values)  # The new row is at the end
                
            # Create batch updates
            updates_to_make = [
                {'col': position_col, 'value': position},
                {'col': price_col, 'value': trade_price},
                {'col': mkt_value_col, 'value': mkt_value},
                {'col': cash_col, 'value': cash},
                {'col': nav_col, 'value': nav}
            ]
            
            for update_item in updates_to_make:
                col = update_item['col']
                value = update_item['value']
                a1_notation = gspread.utils.rowcol_to_a1(row_index, col+1)  # +1 because gspread is 1-indexed
                
                batch_updates.append({
                    'range': a1_notation,
                    'values': [[value]]
                })
                
        # Execute updates in batches
        if batch_updates:
            for i in range(0, len(batch_updates), 10):  # Process in batches of 10
                batch_chunk = batch_updates[i:i+10]
                retry_with_backoff(balance_sheet.batch_update, batch_chunk)
                time.sleep(1.2)  # Add delay between batches
                
            print(f"Updated {len(updates_data)} records in balance sheet")
        else:
            print("No balance sheet updates to perform")
            
    except Exception as e:
        print(f"Error updating balance sheet: {str(e)}")



def generate_portfolio_summary(app, position_tracker, trade_results=None, current_date=None):
    """Generate portfolio summary including positions, cash, NAV, and P/L with timestamps"""
    # Get current date if not provided
    eastern = pytz.timezone('US/Eastern')
    if not current_date:
        current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    
    # Get current timestamp
    current_timestamp = datetime.datetime.now(eastern).strftime("%H:%M:%S")
    
    # Initialize summary data
    summary = {
        'date': current_date,
        'timestamp': current_timestamp,
        'positions': [],
        'cash': 0,
        'nav': 0
    }
    
    # Get account data from TWS
    for account, details in app.account_summary.items():
        if 'NetLiquidation' in details:
            nav_val = details['NetLiquidation'].get('value', '')
            summary['nav'] = float(nav_val) if nav_val and nav_val != '' else 0
        
        # Get Cash value if available
        if 'AvailableFunds' in details:
            cash_val = details['AvailableFunds'].get('value', '')
            summary['cash'] = float(cash_val) if cash_val and cash_val != '' else 0
    
    # Request PnL once - no continuous pinging
    # pnl_req_id = app.request_pnl()
    # time.sleep(1)  # Wait for data to be received
    
    # Get all positions
    for ticker, shares in position_tracker.positions.items():
        price = 0
        
        # Update with today's trade information if available
        if trade_results and ticker in trade_results:
            price = trade_results[ticker].get('price', 0)
        
        # Calculate market value
        mkt_value = shares * price
        
        # Try to find P/L data for this ticker
        daily_pl = 0
        
        # Create a ticker->conId mapping (this would need to be implemented elsewhere)
        # Here we'll use pnl_single_data if available
        for req_id, pnl_data in app.pnl_single_data.items():
            if 'position' in pnl_data and abs(pnl_data['position'] - shares) < 0.1:
                # This is likely the position we're looking for
                daily_pl = pnl_data.get('dailyPnL', 0)
                break
        
        position_data = {
            'ticker': ticker,
            'position': shares,
            'trade_price': price,
            'mkt_value': mkt_value,
            'daily_pl': daily_pl  # Now using actual P/L data
        }
        
        summary['positions'].append(position_data)
    
    return summary


def update_portfolio_balance_tab(credentials_file, detail_sheet_url, summary_data):
    """Update the portfolio balance as a tab in the detail sheet with modified columns."""
    try:
        # Connect to Google Sheets
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        
        # Open the detail spreadsheet
        spreadsheet = client.open_by_url(detail_sheet_url)
        
        # Try to get the "Portfolio Balance" tab, or create it if it doesn't exist
        try:
            balance_worksheet = spreadsheet.worksheet("Portfolio Balance")
        except:
            # Create the tab
            balance_worksheet = spreadsheet.add_worksheet(title="Portfolio Balance", rows=1000, cols=10)
        
        # Define headers with the requested changes
        headers = [
            'Date', 'Timestamp', 'Ticker', 'Position', 'Trade Price', 
            'MKT Value', 'Daily P/L', 'Cash', 'NAV'
        ]
        
        # Check if headers exist, if not add them
        all_values = balance_worksheet.get_all_values()
        if not all_values or len(all_values[0]) != len(headers):
            # If worksheet is empty or headers don't match, set headers
            if all_values:
                balance_worksheet.clear()  # Clear existing data if headers don't match
            balance_worksheet.append_row(headers)
        
        # Get current date and timestamp
        current_date = summary_data['date']
        current_timestamp = summary_data['timestamp']
        
        # Add updated position data
        for position in summary_data['positions']:
            ticker = position['ticker']
            shares = position['position']
            price = position['trade_price']
            
            # Calculate MKT Value
            mkt_value = position['mkt_value']
            
            # Get Daily P/L 
            daily_pl = position['daily_pl']
            
            row_data = [
                current_date,
                current_timestamp,
                ticker,
                shares,
                price,
                mkt_value,
                daily_pl,
                summary_data['cash'] if position == summary_data['positions'][0] else "",  # Only show in first row
                summary_data['nav'] if position == summary_data['positions'][0] else ""    # Only show in first row
            ]
            
            balance_worksheet.append_row(row_data)
        
        # If no positions, add a row with just the date, timestamp, cash and NAV
        if not summary_data['positions']:
            balance_worksheet.append_row([
                current_date,
                current_timestamp,
                "", "", "", "", "",
                summary_data['cash'],
                summary_data['nav']
            ])
        
        print(f"Updated portfolio balance tab with {len(summary_data['positions'])} positions")
        return True
    
    except Exception as e:
        print(f"Error updating portfolio balance tab: {str(e)}")
        return False
    
def calculate_ticker_abs_shares_all(individual_sheet_data):
    """Calculate total absolute shares for commission distribution"""
    ticker_abs_shares_all = {}
    eastern = pytz.timezone('US/Eastern')
    current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    
    for i, strategy_data in enumerate(individual_sheet_data):
        for row in strategy_data:
            if row['date'] == current_date:
                ticker = row['ticker']
                delta = row['target_position'] - row['pre_trade_position']
                abs_shares = abs(delta)
                if ticker not in ticker_abs_shares_all:
                    ticker_abs_shares_all[ticker] = 0
                ticker_abs_shares_all[ticker] += abs_shares
                
    return ticker_abs_shares_all


def get_previous_nav(db_handler, rate_limited_client, sheet_url):
    """
    Get the previous day's NAV from either the Daily NAV sheet or a default value.
    """
    try:
        spreadsheet = rate_limited_client.open_by_url(sheet_url)
        # Get the Daily NAV sheet (3rd sheet)
        try:
            nav_sheet = spreadsheet.get_worksheet(2)
            if nav_sheet is None:
                return 100000  # Default if sheet doesn't exist
        except:
            return 100000
        
        # Get values from the sheet
        values = nav_sheet.get_all_values()
        
        # Skip header row and get the latest entry
        if len(values) > 1:
            latest_row = values[-1]
            if len(latest_row) >= 2:  # We need at least Date and NAV columns
                try:
                    return float(latest_row[1])
                except (ValueError, TypeError):
                    pass
        
        return 100000  # Default if no valid NAV found
    except Exception as e:
        print(f"Error getting previous NAV for {sheet_url}: {str(e)}")
        return 100000


def update_first_input_sheet_after_20min(rate_limited_client, sheet_url, trade_results, individual_sheet_data):
    """
    Update the first input sheet's Shares bought/sold and Price columns after 20 minutes.
    """
    # Prepare updates for all tickers in the first sheet for today
    eastern = pytz.timezone('US/Eastern')
    current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    updates = []
    for row in individual_sheet_data[0]:
        ticker = row['ticker']
        trade_date = row['date']
        if trade_date == current_date and ticker in trade_results:
            delta_shares = trade_results[ticker]['delta_shares']
            trade_price = trade_results[ticker]['price']
            updates.append({
                'ticker': ticker,
                'delta_shares': delta_shares,
                'trade_price': trade_price,
                'trade_date': current_date
            })
    if updates:
        batch_update_multiple_tickers_with_rate_limiting(rate_limited_client, sheet_url, updates)
    else:
        print("No updates for first input sheet after 20 minutes.")



def update_portfolio_balance_tab_with_rate_limiting(rate_limited_client, detail_sheet_url, summary_data):
    """Update the portfolio balance tab with rate limiting."""
    try:
        # Open the detail spreadsheet
        spreadsheet = rate_limited_client.open_by_url(detail_sheet_url)
        
        # Try to get the "Portfolio Balance" tab, or create it if it doesn't exist
        try:
            balance_worksheet = spreadsheet.worksheet("Portfolio Balance")
        except:
            # Create the tab
            balance_worksheet = spreadsheet.add_worksheet(title="Portfolio Balance", rows=1000, cols=10)
            
        # Define headers with the requested changes
        headers = [
            'Date', 'Timestamp', 'Ticker', 'Position', 'Trade Price',
            'MKT Value', 'Daily P/L', 'Cash', 'NAV'
        ]
        
        # Check if headers exist, if not add them
        all_values = balance_worksheet.get_all_values()
        if not all_values or len(all_values[0]) != len(headers):
            # If worksheet is empty or headers don't match, set headers
            if all_values:
                balance_worksheet.clear()  # Clear existing data if headers don't match
                
            header_update = {
                'range': 'A1:I1',
                'values': [headers]
            }
            
            retry_with_backoff(balance_worksheet.batch_update, [header_update])
            time.sleep(1.2)  # Add delay to avoid rate limits
            
        # Get current date and timestamp
        current_date = summary_data['date']
        current_timestamp = summary_data['timestamp']
        
        # Prepare batch updates
        batch_updates = []
        current_row = len(all_values) + 1
        
        # Add updated position data
        for position in summary_data['positions']:
            ticker = position['ticker']
            shares = position['position']
            price = position['trade_price']
            
            # Calculate MKT Value
            mkt_value = position['mkt_value']
            
            # Get Daily P/L
            daily_pl = position['daily_pl']
            
            row_data = {
                'range': f'A{current_row}:I{current_row}',
                'values': [[
                    current_date,
                    current_timestamp,
                    ticker,
                    shares,
                    price,
                    mkt_value,
                    daily_pl,
                    summary_data['cash'] if position == summary_data['positions'][0] else "",  # Only show in first row
                    summary_data['nav'] if position == summary_data['positions'][0] else ""  # Only show in first row
                ]]
            }
            
            batch_updates.append(row_data)
            current_row += 1
            
        # If no positions, add a row with just the date, timestamp, cash and NAV
        if not summary_data['positions']:
            row_data = {
                'range': f'A{current_row}:I{current_row}',
                'values': [[
                    current_date,
                    current_timestamp,
                    "", "", "", "", "",
                    summary_data['cash'],
                    summary_data['nav']
                ]]
            }
            
            batch_updates.append(row_data)
            
        # Execute batch update in smaller chunks
        if batch_updates:
            for i in range(0, len(batch_updates), 10):  # Reduced batch size
                batch_chunk = batch_updates[i:i+10]
                retry_with_backoff(balance_worksheet.batch_update, batch_chunk)
                time.sleep(1.2)  # Add delay between batches
                
            print(f"Updated portfolio balance tab with {len(summary_data['positions'])} positions")
            return True
        else:
            print("No portfolio balance updates to perform")
            return False
            
    except Exception as e:
        print(f"Error updating portfolio balance tab: {str(e)}")
        return False

def run_loop(app):
    """Run the TWS client connection"""
    app.run()


def main():
    # --- Configuration ---
    credentials_file = 'credentials_IBKR.json'
    strategy_sheet_urls = [
        'https://docs.google.com/spreadsheets/d/1OG2Be49zA_AScbnmjDCKbEXlLGWVgPymt8UcPal7PnY/edit?gid=1787511998#gid=1787511998',
        'https://docs.google.com/spreadsheets/d/1hPxz6xeoOX1gZhimlNvM9aJ7o-EhnugCE_1ZYg2ebnI/edit?gid=0#gid=0',
        'https://docs.google.com/spreadsheets/d/1w4maKF6MAHj08tyyc0pFritEoaP7lvPJzdXWWgvkcUg/edit?gid=390866879#gid=390866879'
    ]
    output_sheet_url = 'https://docs.google.com/spreadsheets/d/1xWE7ajeuxSG8ItMoeEo9xblJ3QDC1HLPsZ2H1GHLTvo/edit?gid=0#gid=0'
    detail_sheet_url = 'https://docs.google.com/spreadsheets/d/1rNq1lKYoGnZemMrIe73_hwgqRPrlmNgD6jQThvjvXZ4/edit?gid=0#gid=0'

    db_handler = DatabaseHandler('trading_data.db')
    rate_limited_client = RateLimitedClient(credentials_file, max_calls_per_minute=50)
    combined_tracker = PositionTracker('combined_positions.json')
    individual_trackers = [
        PositionTracker('strategy1_positions.json'),
        PositionTracker('strategy2_positions.json'),
        PositionTracker('strategy3_positions.json')
    ]

    # Ensure position files are properly loaded
    for i, tracker in enumerate(individual_trackers):
        print(f"Loaded positions for strategy {i+1}: {tracker.positions}")

    initialize_strategy_cash(db_handler, len(individual_trackers))

    # Connect to TWS
    app = TradingApp()
    app.connect("127.0.0.1", 7497, 0)
    api_thread = threading.Thread(target=run_loop, args=(app,), daemon=True)
    api_thread.start()

    # Wait for connection
    wait_time = 0
    max_wait = 30
    while app.nextOrderId is None and wait_time < max_wait:
        time.sleep(1)
        wait_time += 1
    if app.nextOrderId is None:
        print("Failed to connect to TWS.")
        app.disconnect()
        return

    # Request account summary
    app.request_account_summary()
    time.sleep(3)  # Wait for account data

    # Combine positions from strategy sheets
    # combined_data, individual_sheet_data, ticker_strategy_map = combine_positions_from_sheets(
    #     rate_limited_client, strategy_sheet_urls, individual_trackers
    # )
    # if not combined_data:
    #     print("No positions to trade")
    #     app.disconnect()
    #     return
    # eastern = pytz.timezone('US/Eastern')
    # current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    # add_liquidation_rows_to_individual_data(individual_trackers, individual_sheet_data, current_date)

    combined_data, individual_sheet_data, ticker_strategy_map = combine_positions_from_sheets(
    rate_limited_client, strategy_sheet_urls, individual_trackers
    )
    if not combined_data:
        print("No positions to trade")
        app.disconnect()
        return
    eastern = pytz.timezone('US/Eastern')
    current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
    add_liquidation_rows_to_individual_data(individual_trackers, individual_sheet_data, current_date)


    # Connect to Google Sheets using rate-limited client
    output_worksheet = None
    detail_worksheet = None
    try:
        output_worksheet = rate_limited_client.open_by_url(output_sheet_url).sheet1
        detail_worksheet = rate_limited_client.open_by_url(detail_sheet_url).sheet1
    except Exception as e:
        print(f"Error connecting to sheets: {str(e)}")

    # --- Main trading workflow ---
    try:
        # Get current date
        eastern = pytz.timezone('US/Eastern')
        current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")

        # Execute all orders first
        orders_info = execute_all_orders(app, combined_data, combined_tracker, current_date, individual_trackers)

        # Retrieve order details after delay (20 minutes)
        results, trade_results = retrieve_order_details(
            app, orders_info, combined_tracker,
            individual_trackers, individual_sheet_data,
            ticker_strategy_map, db_handler,
            output_worksheet
        )

        # Save positions for all trackers
        combined_tracker.save_positions()
        for i, tracker in enumerate(individual_trackers):
            tracker.save_positions()
            print(f"Saved positions for strategy {i+1}: {tracker.positions}")

        # Export data from SQLite to Google Sheets (detail worksheet)
        if detail_worksheet:
            db_handler.export_to_sheet(detail_worksheet)
            print("Exported data from SQLite to detailed worksheet")

        # --- Update first input sheet's Shares bought/sold and Price columns after 20 minutes ---
        def update_first_input_sheet_after_20min():
            updates = []
            eastern = pytz.timezone('US/Eastern')
            current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
            for row in individual_sheet_data[0]:
                ticker = row['ticker']
                trade_date = row['date']
                if trade_date == current_date and ticker in trade_results:
                    delta_shares = trade_results[ticker]['delta_shares']
                    trade_price = trade_results[ticker]['price']
                    updates.append({
                        'ticker': ticker,
                        'delta_shares': delta_shares,
                        'trade_price': trade_price,
                        'trade_date': current_date
                    })
            if updates:
                batch_update_multiple_tickers_with_rate_limiting(
                    rate_limited_client,
                    strategy_sheet_urls[0],
                    updates
                )
                print("Updated first input sheet after 20 minutes.")
            else:
                print("No updates for first input sheet after 20 minutes.")

        update_first_input_sheet_after_20min()

        # --- Schedule the rest of the updates (all sheets, NAV, etc.) after 1 hour ---
        def delayed_full_update():
            print("Starting delayed full update (all sheets, NAV, etc.) after 1 hour...")
            ticker_abs_shares_all = calculate_ticker_abs_shares_all(individual_sheet_data)
            update_input_sheets_with_rate_limiting(
                rate_limited_client,
                strategy_sheet_urls,
                trade_results,
                ticker_strategy_map,
                individual_sheet_data,
                individual_trackers,
                db_handler,
                ticker_abs_shares_all
            )
            # After trade execution and updating individual sheets
            update_combined_metrics_sheet(rate_limited_client, strategy_sheet_urls, app, individual_trackers, db_handler)

            # Update portfolio summary and balance tab
            eastern = pytz.timezone('US/Eastern')
            current_date = datetime.datetime.now(eastern).strftime("%Y-%m-%d")
            portfolio_summary = generate_portfolio_summary(app, combined_tracker, trade_results, current_date)
            update_portfolio_balance_tab_with_rate_limiting(rate_limited_client, detail_sheet_url, portfolio_summary)
            print("Delayed full update complete.")

        timer = threading.Timer(60 * 12, delayed_full_update)
        timer.start()
        print("Scheduled full sheet/NAV update for 1 hour later.")

        print(f"Processed {len(results)} trades")
        print(f"Saved combined positions: {combined_tracker.positions}")
        print("Updated first sheet. Remaining sheets/NAV will update after 1 hour.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Disconnect from TWS
        app.disconnect()

if __name__ == "__main__":
    main()

