"""
Title: Load Script for MySQL Database
Author: Shalini Tata
Created: 24-11-2025
Description: Load validated data and fact tables into MySQL database
"""

import mysql.connector
from mysql.connector import Error
import pandas as pd
from logger import logger
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG={
    'host': os.getenv("DB_HOST","localhost"),
    'port': int(os.getenv("DB_PORT",3306)),
    'user': os.getenv("DB_USER","root"),
    'password': os.getenv("DB_PASSWORD","shalu 025"),
    'database': 'stocks_new'
}

def get_db_conn():
    """create and return database connection"""
    try:
        connection=mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None

def create_db():
    """create stocks_new database if not exists"""
    try:
        connection=mysql.connector.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor=connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS stocks_new")
        cursor.execute("USE stocks_new")
        logger.info("Database 'stocks_new' created or already exists")
        cursor.close()
        connection.close()
    except Error as e:
        logger.error(f"Failed to create database: {e}")

def create_tbl_stocks_raw(df:pd.DataFrame):
    """create and load stocks_raw table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()
        
        create_query="""
        CREATE TABLE IF NOT EXISTS stocks_raw (
            id INT AUTO_INCREMENT PRIMARY KEY,
            company VARCHAR(255) NOT NULL,
            ticker VARCHAR(50) NOT NULL,
            date DATE NOT NULL,
            open FLOAT NOT NULL,
            high FLOAT NOT NULL,
            low FLOAT NOT NULL,
            close FLOAT NOT NULL,
            volume BIGINT NOT NULL,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini',
            UNIQUE KEY unique_stock (ticker, date)
        )
        """
        cursor.execute(create_query)
        logger.info("Table stocks_raw created")
        
        insert_query="""
        INSERT IGNORE INTO stocks_raw (company, ticker, date, open, high, low, close, volume, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                str(row['company']),
                str(row['ticker']),
                pd.to_datetime(row['date']).date(),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume']),
                'shalini',
                'shalini',
                'shalini'
            ))
        
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into stocks_raw")
        
    except Error as e:
        logger.error(f"failed to load stocks_raw: {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_company_metadata(df):
    """create and load company_metadata table"""
    connection=get_db_conn()
    if not connection:
        logger.error("no database connection for company_metadata")
        raise Exception("database connection failed for company_metadata")
    
    cursor=None
    try:
        cursor=connection.cursor()
        
        create_query="""
        CREATE TABLE IF NOT EXISTS company_metadata (
            `rank` INT PRIMARY KEY,
            company VARCHAR(255) NOT NULL UNIQUE,
            industry VARCHAR(255),
            revenue FLOAT,
            employees FLOAT,
            ticker VARCHAR(50),
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini'
        )
        """
        cursor.execute(create_query)
        logger.info("Table company_metadata created successfully")
        
        insert_query="""
        INSERT IGNORE INTO company_metadata (`rank`, company, industry, revenue, employees, ticker, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        #add row-by-row
        cnt=0
        for idx,row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    int(row['rank']),
                    str(row['company']),
                    str(row['industry']) if pd.notna(row['industry']) else None,
                    float(row['revenue']) if pd.notna(row['revenue']) else None,
                    float(row['employees']) if pd.notna(row['employees']) else None,
                    str(row['ticker']) if pd.notna(row['ticker']) else None,
                    'shalini',
                    'shalini',
                    'shalini'
                ))
                cnt+=1
            except Exception as row_error:
                logger.error(f"failed to insert row {idx}: {row.to_dict()}")
                logger.error(f"failed bcoz: {row_error}")
        connection.commit()
        logger.info(f"successfully inserted {cnt}/{len(df)} rows into company_metadata")
        
    except Error as e:
        logger.error(f"failed to load company_metadata: {e}")
        logger.error(f"failed bcoz: {str(e)}")
        raise e 
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_exchange_rates(df):
    """create and load exchange_rates table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()    
        create_query="""
        CREATE TABLE IF NOT EXISTS exchange_rates (
            date DATE PRIMARY KEY,
            usd_inr_rate FLOAT NOT NULL,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini'
        )
        """
        cursor.execute(create_query)
        logger.info("Table exchange_rates created") 
        insert_query="""
        INSERT IGNORE INTO exchange_rates (date, usd_inr_rate, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s)
        """     
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                pd.to_datetime(row['date']).date(),
                float(row['usd_inr_rate']),
                'shalini',
                'shalini',
                'shalini'
            ))  
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into exchange_rates")
        
    except Error as e:
        logger.error(f"failed to load exchange_rates: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_macro_raw(df):
    """create and load macro_raw table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()
        create_query="""
        CREATE TABLE IF NOT EXISTS macro_raw (
            quarter VARCHAR(20) PRIMARY KEY,
            GDP FLOAT,
            inflation FLOAT,
            unemployment FLOAT,
            interest_rate FLOAT,
            consumer_spending FLOAT,
            industrial_production FLOAT,
            housing_starts FLOAT,
            retail_sales FLOAT,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini'
        )
        """
        cursor.execute(create_query)
        logger.info("Table macro_raw created")    
        insert_query="""
        INSERT IGNORE INTO macro_raw (quarter, GDP, inflation, unemployment, interest_rate, consumer_spending, industrial_production, housing_starts, retail_sales, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """  
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                str(row['quarter']),
                float(row['GDP']) if pd.notna(row['GDP']) else None,
                float(row['inflation']) if pd.notna(row['inflation']) else None,
                float(row['unemployment']) if pd.notna(row['unemployment']) else None,
                float(row['interest_rate']) if pd.notna(row['interest_rate']) else None,
                float(row['consumer_spending']) if pd.notna(row['consumer_spending']) else None,
                float(row['industrial_production']) if pd.notna(row['industrial_production']) else None,
                float(row['housing_starts']) if pd.notna(row['housing_starts']) else None,
                float(row['retail_sales']) if pd.notna(row['retail_sales']) else None,
                'shalini',
                'shalini',
                'shalini'
            ))  
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into macro_raw")       
    except Error as e:
        logger.error(f"failed to load macro_raw: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_sentiment(df):
    """create and load sentiment table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()
        
        create_query="""
        CREATE TABLE IF NOT EXISTS sentiment (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50) NOT NULL,
            company VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            sentiment_score FLOAT NOT NULL,
            article_count INT NOT NULL,
            bullish INT NOT NULL,
            bearish INT NOT NULL,
            neutral INT NOT NULL,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini',
            UNIQUE KEY unique_sentiment (ticker, date)
        )
        """
        cursor.execute(create_query)
        logger.info("Table sentiment created") 
        insert_query="""
        INSERT IGNORE INTO sentiment (ticker, company, date, sentiment_score, article_count, bullish, bearish, neutral, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """     
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                str(row['ticker']),
                str(row['company']),
                pd.to_datetime(row['date']).date(),
                float(row['sentiment_score']),
                int(row['article_count']),
                int(row['bullish']),
                int(row['bearish']),
                int(row['neutral']),
                'shalini',
                'shalini',
                'shalini'
            ))      
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into sentiment")
        
    except Error as e:
        logger.error(f"failed to load sentiment: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_stock_facts(df:pd.DataFrame):
    """create and load stock_facts table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()    
        create_query="""
        CREATE TABLE IF NOT EXISTS stock_facts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50) NOT NULL,
            company VARCHAR(255) NOT NULL,
            date DATE NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT,
            daily_return FLOAT,
            volatility_30d FLOAT,
            moving_avg_30d FLOAT,
            moving_avg_90d FLOAT,
            trading_volume_avg FLOAT,
            price_range FLOAT,
            year INT,
            month INT,
            quarter INT,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini',
            UNIQUE KEY unique_stock_fact (ticker, date)
        )
        """
        cursor.execute(create_query)
        logger.info("Table stock_facts created")
        insert_query="""
        INSERT IGNORE INTO stock_facts (ticker, company, date, open, high, low, close, volume, daily_return, volatility_30d, moving_avg_30d, moving_avg_90d, trading_volume_avg, price_range, year, month, quarter, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """  
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                str(row['ticker']),
                str(row['company']),
                pd.to_datetime(row['date']).date(),
                float(row['open']) if pd.notna(row['open']) else None,
                float(row['high']) if pd.notna(row['high']) else None,
                float(row['low']) if pd.notna(row['low']) else None,
                float(row['close']) if pd.notna(row['close']) else None,
                int(row['volume']) if pd.notna(row['volume']) else None,
                float(row['daily_return']) if pd.notna(row['daily_return']) else None,
                float(row['volatility_30d']) if pd.notna(row['volatility_30d']) else None,
                float(row['moving_avg_30d']) if pd.notna(row['moving_avg_30d']) else None,
                float(row['moving_avg_90d']) if pd.notna(row['moving_avg_90d']) else None,
                float(row['trading_volume_avg']) if pd.notna(row['trading_volume_avg']) else None,
                float(row['price_range']) if pd.notna(row['price_range']) else None,
                int(row['year']) if pd.notna(row['year']) else None,
                int(row['month']) if pd.notna(row['month']) else None,
                int(row['quarter']) if pd.notna(row['quarter']) else None,
                'shalini',
                'shalini',
                'shalini'
            ))
        
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into stock_facts")
        
    except Error as e:
        logger.error(f"failed to load stock_facts: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_macro_facts(df):
    """create and load macro_facts table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()    
        create_query="""
        CREATE TABLE IF NOT EXISTS macro_facts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            quarter VARCHAR(20) NOT NULL UNIQUE,
            year INT,
            GDP FLOAT,
            inflation FLOAT,
            unemployment FLOAT,
            interest_rate FLOAT,
            consumer_spending FLOAT,
            industrial_production FLOAT,
            housing_starts FLOAT,
            retail_sales FLOAT,
            gdp_growth FLOAT,
            inflation_trend FLOAT,
            is_recession INT,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini'
        )
        """
        cursor.execute(create_query)
        logger.info("Table macro_facts created")
        insert_query="""
        INSERT IGNORE INTO macro_facts(quarter, year, GDP, inflation, unemployment, interest_rate, consumer_spending, industrial_production, housing_starts, retail_sales, gdp_growth, inflation_trend, is_recession, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """  
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                str(row['quarter']),
                int(row['year']) if pd.notna(row['year']) else None,
                float(row['GDP']) if pd.notna(row['GDP']) else None,
                float(row['inflation']) if pd.notna(row['inflation']) else None,
                float(row['unemployment']) if pd.notna(row['unemployment']) else None,
                float(row['interest_rate']) if pd.notna(row['interest_rate']) else None,
                float(row['consumer_spending']) if pd.notna(row['consumer_spending']) else None,
                float(row['industrial_production']) if pd.notna(row['industrial_production']) else None,
                float(row['housing_starts']) if pd.notna(row['housing_starts']) else None,
                float(row['retail_sales']) if pd.notna(row['retail_sales']) else None,
                float(row['gdp_growth']) if pd.notna(row['gdp_growth']) else None,
                float(row['inflation_trend']) if pd.notna(row['inflation_trend']) else None,
                int(row['is_recession']) if pd.notna(row['is_recession']) else None,
                'shalini',
                'shalini',
                'shalini'
            ))
        
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into macro_facts")
        
    except Error as e:
        logger.error(f"failed to load macro_facts: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_sector_lookup(df:pd.DataFrame):
    """create and load sector_lookup table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()      
        create_query="""
        CREATE TABLE IF NOT EXISTS sector_lookup (
            sector_id INT PRIMARY KEY,
            sector_name VARCHAR(255) NOT NULL UNIQUE,
            sector_code VARCHAR(10),
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini'
        )
        """
        cursor.execute(create_query)
        logger.info("Table sector_lookup created")      
        insert_query="""
        INSERT IGNORE INTO sector_lookup (sector_id, sector_name, sector_code, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                int(row['sector_id']),
                str(row['sector_name']),
                str(row['sector_code']) if pd.notna(row['sector_code']) else None,
                'shalini',
                'shalini',
                'shalini'
            ))    
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into sector_lookup")
        
    except Error as e:
        logger.error(f"failed to load sector_lookup: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_tbl_analytics_summary(df:pd.DataFrame):
    """create and load analytics_summary table"""
    connection=get_db_conn()
    if not connection:
        return
    
    try:
        cursor=connection.cursor()    
        create_query="""
        CREATE TABLE IF NOT EXISTS analytics_summary (
            id INT AUTO_INCREMENT PRIMARY KEY,
            ticker VARCHAR(50) NOT NULL,
            company VARCHAR(255) NOT NULL,
            sector_name VARCHAR(255),
            sector_code VARCHAR(10),
            year INT,
            quarter INT,
            avg_return FLOAT,
            avg_volatility FLOAT,
            correlation_gdp FLOAT,
            correlation_inflation FLOAT,
            currency_impact FLOAT,
            sector_rank FLOAT,
            sector_avg_return FLOAT,
            outperforming_sector INT,
            recession_resilience_score FLOAT,
            created_by VARCHAR(100) DEFAULT 'shalini',
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR(100) DEFAULT 'shalini',
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            last_updated_login VARCHAR(100) DEFAULT 'shalini',
            UNIQUE KEY unique_analytics (ticker,year,quarter)
        )
        """
        cursor.execute(create_query)
        logger.info("Table analytics_summary created")  
        insert_query="""
        INSERT IGNORE INTO analytics_summary (ticker, company, sector_name, sector_code, year, quarter, avg_return, avg_volatility, correlation_gdp, correlation_inflation, currency_impact, sector_rank, sector_avg_return, outperforming_sector, recession_resilience_score, created_by, updated_by, last_updated_login)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """     
        for _, row in df.iterrows():
            cursor.execute(insert_query, (
                str(row['ticker']),
                str(row['company']),
                str(row['sector_name']) if pd.notna(row['sector_name']) else None,
                str(row['sector_code']) if pd.notna(row['sector_code']) else None,
                int(row['year']) if pd.notna(row['year']) else None,
                int(row['quarter']) if pd.notna(row['quarter']) else None,
                float(row['avg_return']) if pd.notna(row['avg_return']) else None,
                float(row['avg_volatility']) if pd.notna(row['avg_volatility']) else None,
                float(row['correlation_gdp']) if pd.notna(row['correlation_gdp']) else None,
                float(row['correlation_inflation']) if pd.notna(row['correlation_inflation']) else None,
                float(row['currency_impact']) if pd.notna(row['currency_impact']) else None,
                float(row['sector_rank']) if pd.notna(row['sector_rank']) else None,
                float(row['sector_avg_return']) if pd.notna(row['sector_avg_return']) else None,
                int(row['outperforming_sector']) if pd.notna(row['outperforming_sector']) else None,
                float(row['recession_resilience_score']) if pd.notna(row['recession_resilience_score']) else None,
                'shalini',
                'shalini',
                'shalini'
            ))       
        connection.commit()
        logger.info(f"Inserted {len(df)} rows into analytics_summary")
        
    except Error as e:
        logger.error(f"failed to load analytics_summary: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__=="__main__":
    create_db()
    logger.info("database initialization complete")