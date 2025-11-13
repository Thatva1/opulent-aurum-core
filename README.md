# Opulent Aurum Core

A comprehensive AI-powered financial analysis and algorithmic trading platform.

## Features

- Advanced machine learning models for financial prediction
- Algorithmic trading strategies with backtesting
- Real-time market data integration
- PostgreSQL database for data persistence
- NSE equity data pipeline (OHLCV historical data)
- Modern data science and visualization tools

## Tech Stack

- **Machine Learning**: TensorFlow, XGBoost, scikit-learn
- **Data Science**: NumPy, Pandas, Matplotlib, Seaborn
- **Financial Data**: yfinance, kiteconnect
- **Trading**: backtrader
- **Database**: PostgreSQL with SQLAlchemy
- **Backend**: Python 3.13

## Installation

### 1. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 2. Install PostgreSQL Database
```bash
# PostgreSQL 17 is already installed and configured
# Database: opulent_aurum_db
# User: postgres (password-less local access configured)
```

### 3. Database Setup
The NSE data pipeline is automatically initialized when you run the pipeline script:

```python
from nse_data_pipeline import NSEDataPipeline

pipeline = NSEDataPipeline()
pipeline.create_tables()  # Creates NSE equity data tables
```

## Database Architecture

### Triple Database Design
- **`opulent_aurum_db`**: NSE Equity Data + Custom equity_data table
- **`opulent_aurum`**: NSE Equity Data + F&O Derivatives Data
- **NSE Equity data is replicated in both databases** for flexibility

### Database Setup
```bash
# PostgreSQL 17 is installed and configured
# Two databases with NSE equity data in both for maximum flexibility
```

### NSE Equity Data Schema (`opulent_aurum_db`)
```sql
CREATE TABLE nse_equity_data (
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
```

### F&O Data Schema (`opulent_aurum`)
```sql
-- Futures Data
CREATE TABLE nse_futures_data (
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

-- Options Data
CREATE TABLE nse_options_data (
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
```

## Usage

### NSE Equity Data (Available in Both Databases)

```python
from nse_data_pipeline import NSEDataPipeline

# Use opulent_aurum_db
nse_db = NSEDataPipeline("postgresql://postgres@localhost:5432/opulent_aurum_db")
reliance_data_db = nse_db.get_equity_data('RELIANCE')

# Use opulent_aurum
nse_main = NSEDataPipeline("postgresql://postgres@localhost:5432/opulent_aurum")
reliance_data_main = nse_main.get_equity_data('RELIANCE')

# Both contain the same NSE equity data
print(f"Records in opulent_aurum_db: {len(reliance_data_db)}")
print(f"Records in opulent_aurum: {len(reliance_data_main)}")
```

### Run Complete Test
```bash
python nse_data_pipeline.py
```

## Data Pipeline Features

- **OHLCV Data Storage**: Store Open, High, Low, Close, Volume data for NSE stocks
- **Efficient Indexing**: Optimized database indexes for fast queries
- **Duplicate Handling**: Automatic handling of duplicate data entries
- **Bulk Operations**: Support for batch data insertion
- **Time Series Queries**: Easy retrieval of historical data by date ranges

## Future Enhancements

- [ ] F&O (Futures & Options) data integration
- [ ] Real-time data streaming
- [ ] Advanced technical indicators
- [ ] Machine learning prediction models
- [ ] Algorithmic trading strategies
- [ ] Risk management modules

## Contributing

[Add contribution guidelines here]

## License

[Add license information here]