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

### Dual Database Design
- **`opulent_aurum_db`**: NSE Equity Data (OHLCV historical data)
- **`opulent_aurum`**: F&O Derivatives Data (Futures & Options)

### Database Setup
```bash
# PostgreSQL 17 is installed and configured
# Two databases created for organized data management
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

### NSE Equity Data Pipeline (`opulent_aurum_db`)

```python
from nse_data_pipeline import NSEDataPipeline

# Initialize NSE equity pipeline
nse_pipeline = NSEDataPipeline()  # Defaults to opulent_aurum_db

# Insert equity data
nse_pipeline.insert_equity_data(
    symbol='RELIANCE',
    date=datetime.date(2024, 1, 1),
    open_price=2500.00,
    high_price=2520.00,
    low_price=2480.00,
    close_price=2510.00,
    volume=1000000
)

# Retrieve data
reliance_data = nse_pipeline.get_equity_data('RELIANCE')
print(reliance_data.head())
```

### F&O Data Pipeline (`opulent_aurum`)

```python
from nse_data_pipeline import FNODataPipeline

# Initialize F&O pipeline
fno_pipeline = FNODataPipeline()  # Defaults to opulent_aurum

# Insert futures data
fno_pipeline.insert_futures_data(
    symbol='RELIANCE',
    expiry_date=datetime.date(2024, 1, 25),
    date=datetime.date(2024, 1, 1),
    open_price=2520.00,
    high_price=2540.00,
    low_price=2500.00,
    close_price=2530.00,
    volume=500000,
    open_interest=2000000
)

# Insert options data
fno_pipeline.insert_options_data(
    symbol='RELIANCE',
    expiry_date=datetime.date(2024, 1, 25),
    strike_price=2500.00,
    option_type='CE',  # 'CE' for Call, 'PE' for Put
    date=datetime.date(2024, 1, 1),
    open_price=45.00,
    high_price=52.00,
    low_price=42.00,
    close_price=48.00,
    volume=100000,
    open_interest=500000
)

# Retrieve data
futures_data = fno_pipeline.get_futures_data('RELIANCE')
options_data = fno_pipeline.get_options_data('RELIANCE', option_type='CE')
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