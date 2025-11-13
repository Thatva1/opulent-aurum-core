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


# Example usage and testing
if __name__ == "__main__":
    # Initialize the pipeline
    pipeline = NSEDataPipeline()

    # Test connection
    if pipeline.test_connection():
        print("✅ Database connection successful!")

        # Create tables
        pipeline.create_tables()

        # Example: Insert sample data for RELIANCE
        sample_data = [
            ('RELIANCE', date(2024, 1, 1), 2500.00, 2520.00, 2480.00, 2510.00, 1000000),
            ('RELIANCE', date(2024, 1, 2), 2510.00, 2530.00, 2500.00, 2525.00, 1200000),
            ('TCS', date(2024, 1, 1), 3200.00, 3220.00, 3180.00, 3210.00, 500000),
        ]

        print("📊 Inserting sample NSE equity data...")
        for data in sample_data:
            pipeline.insert_equity_data(*data)

        # Retrieve and display data
        print("\n📈 Available symbols:", pipeline.get_available_symbols())

        reliance_data = pipeline.get_equity_data('RELIANCE')
        if not reliance_data.empty:
            print("\n📊 RELIANCE data:")
            print(reliance_data.head())

        print("\n✅ NSE Equity Data Pipeline setup complete!")
    else:
        print("❌ Database connection failed. Please check your PostgreSQL setup.")