"""
Opulent Aurum Core - NSE Equity Data Pipeline
Database connection and data management utilities
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, date
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NSEDataPipeline:
    def __init__(self, db_url="postgresql://postgres@localhost:5432/opulent_aurum_db"):
        """
        Initialize database connection for NSE data pipeline

        Args:
            db_url (str): PostgreSQL connection URL
        """
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.db_name = db_url.split('/')[-1]  # Extract database name from URL

    def test_connection(self):
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Connected to PostgreSQL: {version}")
                return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def create_tables(self):
        """Create necessary tables if they don't exist"""
        with self.engine.connect() as conn:
            # NSE Equity Data Table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS nse_equity_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    date DATE NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                );
            """))

            # Create indexes for performance
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_nse_equity_symbol_date
                ON nse_equity_data(symbol, date);
            """))

            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_nse_equity_date
                ON nse_equity_data(date);
            """))

            conn.commit()
            logger.info("Database tables created successfully")

    def insert_equity_data(self, symbol, date, open_price, high_price, low_price, close_price, volume):
        """
        Insert NSE equity OHLCV data

        Args:
            symbol (str): Stock symbol (e.g., 'RELIANCE', 'TCS')
            date (date): Trading date
            open_price, high_price, low_price, close_price (float): OHLC prices
            volume (int): Trading volume
        """
        with self.SessionLocal() as session:
            try:
                query = text("""
                    INSERT INTO nse_equity_data (symbol, date, open_price, high_price, low_price, close_price, volume)
                    VALUES (:symbol, :date, :open_price, :high_price, :low_price, :close_price, :volume)
                    ON CONFLICT (symbol, date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume;
                """)

                session.execute(query, {
                    'symbol': symbol,
                    'date': date,
                    'open_price': open_price,
                    'high_price': high_price,
                    'low_price': low_price,
                    'close_price': close_price,
                    'volume': volume
                })

                session.commit()
                logger.info(f"Inserted/Updated data for {symbol} on {date}")

            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting data: {e}")
                raise

    def get_equity_data(self, symbol, start_date=None, end_date=None):
        """
        Retrieve NSE equity data for a symbol

        Args:
            symbol (str): Stock symbol
            start_date (date): Start date for data retrieval
            end_date (date): End date for data retrieval

        Returns:
            pd.DataFrame: OHLCV data
        """
        query = "SELECT * FROM nse_equity_data WHERE symbol = :symbol"
        params = {'symbol': symbol}

        if start_date:
            query += " AND date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params['end_date'] = end_date

        query += " ORDER BY date"

        try:
            df = pd.read_sql(text(query), self.engine, params=params, index_col='date')
            return df
        except Exception as e:
            logger.error(f"Error retrieving data: {e}")
            return pd.DataFrame()

    def get_available_symbols(self):
        """Get list of all available symbols in the database"""
        try:
            query = text("SELECT DISTINCT symbol FROM nse_equity_data ORDER BY symbol")
            with self.engine.connect() as conn:
                result = conn.execute(query)
                symbols = [row[0] for row in result.fetchall()]
                return symbols
        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            return []

    def get_date_range(self, symbol):
        """Get the date range available for a symbol"""
        try:
            query = text("""
                SELECT MIN(date) as start_date, MAX(date) as end_date
                FROM nse_equity_data
                WHERE symbol = :symbol
            """)

            with self.engine.connect() as conn:
                result = conn.execute(query, {'symbol': symbol})
                row = result.fetchone()
                return row[0], row[1] if row else (None, None)
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
            return None, None


class FNODataPipeline:
    """
    Futures & Options (F&O) data pipeline for NSE
    Handles derivatives data including futures and options contracts
    """

    def __init__(self, db_url="postgresql://postgres@localhost:5432/opulent_aurum"):
        """
        Initialize database connection for F&O data pipeline

        Args:
            db_url (str): PostgreSQL connection URL
        """
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.db_name = db_url.split('/')[-1]

    def test_connection(self):
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Connected to F&O Database: {version}")
                return True
        except Exception as e:
            logger.error(f"F&O Database connection failed: {e}")
            return False

    def create_tables(self):
        """Create F&O data tables"""
        with self.engine.connect() as conn:
            # Futures Data Table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS nse_futures_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    expiry_date DATE NOT NULL,
                    date DATE NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume BIGINT,
                    open_interest BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, expiry_date, date)
                );
            """))

            # Options Data Table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS nse_options_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(50) NOT NULL,
                    expiry_date DATE NOT NULL,
                    strike_price DECIMAL(10,2) NOT NULL,
                    option_type VARCHAR(10) NOT NULL, -- 'CE' or 'PE'
                    date DATE NOT NULL,
                    open_price DECIMAL(10,2),
                    high_price DECIMAL(10,2),
                    low_price DECIMAL(10,2),
                    close_price DECIMAL(10,2),
                    volume BIGINT,
                    open_interest BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, expiry_date, strike_price, option_type, date)
                );
            """))

            # Create indexes for performance
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_futures_symbol_expiry_date
                ON nse_futures_data(symbol, expiry_date, date);
            """))

            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_options_symbol_expiry_strike
                ON nse_options_data(symbol, expiry_date, strike_price, option_type, date);
            """))

            conn.commit()
            logger.info("F&O database tables created successfully")

    def insert_futures_data(self, symbol, expiry_date, date, open_price, high_price,
                          low_price, close_price, volume, open_interest):
        """
        Insert NSE futures data

        Args:
            symbol (str): Underlying symbol
            expiry_date (date): Contract expiry date
            date (date): Trading date
            open_price, high_price, low_price, close_price (float): OHLC prices
            volume (int): Trading volume
            open_interest (int): Open interest
        """
        with self.SessionLocal() as session:
            try:
                query = text("""
                    INSERT INTO nse_futures_data
                    (symbol, expiry_date, date, open_price, high_price, low_price, close_price, volume, open_interest)
                    VALUES (:symbol, :expiry_date, :date, :open_price, :high_price, :low_price, :close_price, :volume, :open_interest)
                    ON CONFLICT (symbol, expiry_date, date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        open_interest = EXCLUDED.open_interest;
                """)

                session.execute(query, {
                    'symbol': symbol, 'expiry_date': expiry_date, 'date': date,
                    'open_price': open_price, 'high_price': high_price, 'low_price': low_price,
                    'close_price': close_price, 'volume': volume, 'open_interest': open_interest
                })

                session.commit()
                logger.info(f"Inserted/Updated futures data for {symbol} expiring {expiry_date}")

            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting futures data: {e}")
                raise

    def insert_options_data(self, symbol, expiry_date, strike_price, option_type, date,
                          open_price, high_price, low_price, close_price, volume, open_interest):
        """
        Insert NSE options data

        Args:
            symbol (str): Underlying symbol
            expiry_date (date): Contract expiry date
            strike_price (float): Option strike price
            option_type (str): 'CE' (Call) or 'PE' (Put)
            date (date): Trading date
            open_price, high_price, low_price, close_price (float): OHLC prices
            volume (int): Trading volume
            open_interest (int): Open interest
        """
        with self.SessionLocal() as session:
            try:
                query = text("""
                    INSERT INTO nse_options_data
                    (symbol, expiry_date, strike_price, option_type, date, open_price, high_price, low_price, close_price, volume, open_interest)
                    VALUES (:symbol, :expiry_date, :strike_price, :option_type, :date, :open_price, :high_price, :low_price, :close_price, :volume, :open_interest)
                    ON CONFLICT (symbol, expiry_date, strike_price, option_type, date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        open_interest = EXCLUDED.open_interest;
                """)

                session.execute(query, {
                    'symbol': symbol, 'expiry_date': expiry_date, 'strike_price': strike_price,
                    'option_type': option_type, 'date': date, 'open_price': open_price,
                    'high_price': high_price, 'low_price': low_price, 'close_price': close_price,
                    'volume': volume, 'open_interest': open_interest
                })

                session.commit()
                logger.info(f"Inserted/Updated {option_type} option data for {symbol} strike {strike_price}")

            except Exception as e:
                session.rollback()
                logger.error(f"Error inserting options data: {e}")
                raise

    def get_futures_data(self, symbol, expiry_date=None, start_date=None, end_date=None):
        """Retrieve futures data for a symbol"""
        query = "SELECT * FROM nse_futures_data WHERE symbol = :symbol"
        params = {'symbol': symbol}

        if expiry_date:
            query += " AND expiry_date = :expiry_date"
            params['expiry_date'] = expiry_date

        if start_date:
            query += " AND date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params['end_date'] = end_date

        query += " ORDER BY date"

        try:
            df = pd.read_sql(text(query), self.engine, params=params, index_col='date')
            return df
        except Exception as e:
            logger.error(f"Error retrieving futures data: {e}")
            return pd.DataFrame()

    def get_options_data(self, symbol, option_type=None, strike_price=None, expiry_date=None,
                        start_date=None, end_date=None):
        """Retrieve options data with flexible filtering"""
        query = "SELECT * FROM nse_options_data WHERE symbol = :symbol"
        params = {'symbol': symbol}

        if option_type:
            query += " AND option_type = :option_type"
            params['option_type'] = option_type

        if strike_price:
            query += " AND strike_price = :strike_price"
            params['strike_price'] = strike_price

        if expiry_date:
            query += " AND expiry_date = :expiry_date"
            params['expiry_date'] = expiry_date

        if start_date:
            query += " AND date >= :start_date"
            params['start_date'] = start_date

        if end_date:
            query += " AND date <= :end_date"
            params['end_date'] = end_date

        query += " ORDER BY date"

        try:
            df = pd.read_sql(text(query), self.engine, params=params, index_col='date')
            return df
        except Exception as e:
            logger.error(f"Error retrieving options data: {e}")
            return pd.DataFrame()

    def get_available_expiry_dates(self, symbol, instrument_type='futures'):
        """Get available expiry dates for a symbol"""
        table = 'nse_futures_data' if instrument_type == 'futures' else 'nse_options_data'

        try:
            query = text(f"SELECT DISTINCT expiry_date FROM {table} WHERE symbol = :symbol ORDER BY expiry_date")
            with self.engine.connect() as conn:
                result = conn.execute(query, {'symbol': symbol})
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error getting expiry dates: {e}")
            return []

    def get_strike_prices(self, symbol, expiry_date, option_type=None):
        """Get available strike prices for options"""
        try:
            query = text("""
                SELECT DISTINCT strike_price FROM nse_options_data
                WHERE symbol = :symbol AND expiry_date = :expiry_date
                ORDER BY strike_price
            """)
            params = {'symbol': symbol, 'expiry_date': expiry_date}

            if option_type:
                query = text(query.text + " AND option_type = :option_type")
                params['option_type'] = option_type

            with self.engine.connect() as conn:
                result = conn.execute(query, params)
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error getting strike prices: {e}")
            return []


# Example usage and testing
if __name__ == "__main__":
    # Test NSE Equity Pipeline in both databases
    print("=== NSE Equity Data in opulent_aurum_db ===")
    nse_pipeline_db = NSEDataPipeline("postgresql://postgres@localhost:5432/opulent_aurum_db")
    if nse_pipeline_db.test_connection():
        print("✅ NSE Database (opulent_aurum_db) connection successful!")
        symbols_db = nse_pipeline_db.get_available_symbols()
        print(f"📊 Available equity symbols: {symbols_db}")
        record_count_db = len(nse_pipeline_db.get_equity_data('RELIANCE'))
        print(f"📈 RELIANCE records in opulent_aurum_db: {record_count_db}")

    print("\n=== NSE Equity Data in opulent_aurum ===")
    nse_pipeline_main = NSEDataPipeline("postgresql://postgres@localhost:5432/opulent_aurum")
    if nse_pipeline_main.test_connection():
        print("✅ NSE Database (opulent_aurum) connection successful!")
        symbols_main = nse_pipeline_main.get_available_symbols()
        print(f"📊 Available equity symbols: {symbols_main}")
        record_count_main = len(nse_pipeline_main.get_equity_data('RELIANCE'))
        print(f"📈 RELIANCE records in opulent_aurum: {record_count_main}")

    # Test F&O Pipeline (opulent_aurum)
    print("\n=== F&O Data Pipeline (opulent_aurum) ===")
    fno_pipeline = FNODataPipeline()
    if fno_pipeline.test_connection():
        print("✅ F&O Database connection successful!")
        expiry_dates = fno_pipeline.get_available_expiry_dates('RELIANCE', 'futures')
        print(f"📅 Available expiry dates for RELIANCE futures: {expiry_dates}")

    print("\n✅ Dual Database Architecture with NSE Equity Data in Both!")
    print("📊 opulent_aurum_db: NSE Equity + Custom equity_data table")
    print("📊 opulent_aurum: NSE Equity + F&O Derivatives Data")