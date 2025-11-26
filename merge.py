"""
Title: Merging Script for Financial Data ELT Process
Author: Shalini Tata
Created: 23-11-2024
Description: Merge raw data files, apply DQ rules, and load valid data to database
Updated: stocks_raw as base table with referential integrity
"""

import os
import json
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from logger import logger
from rules import validate_table
from load import (create_tbl_company_metadata, create_tbl_exchange_rates, 
                  create_tbl_macro_raw, create_tbl_sentiment, create_tbl_stocks_raw,
                  create_tbl_stock_facts, create_tbl_macro_facts, create_tbl_sector_lookup,
                  create_tbl_analytics_summary)
from transform import calculate_all_kpis
from report import generate_all_reports

# Load config
with open("config.json") as f:
    config=json.load(f)

def execution_dir():
    """create and return execution directory path"""
    now=datetime.now()
    exec_dir=os.path.join(os.getcwd(), "Execution", str(now.year), 
                           f"{now.month:02d}", f"{now.day:02d}", 
                           now.strftime("%H:%M"))
    os.makedirs(exec_dir,exist_ok=True)
    return exec_dir

def stocks_raw(history, start_date, end_date, exec_dir):
    """process stocks_raw table """
    table_name="stocks_raw"
    logger.info(f"Processing {table_name} (BASE TABLE)")
    
    if history:
        if isinstance(start_date, str):
            start_date=datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date=datetime.strptime(end_date, "%Y-%m-%d").date()
        
        all_data=[]
        table_dir=os.path.join("stocks_data")
        if not os.path.exists(table_dir):
            logger.error(f"No folder found for {table_name} - CRITICAL: Base table missing")
            return None
        
        for year in os.listdir(table_dir):
            year_path=os.path.join(table_dir, year)
            if not os.path.isdir(year_path):
                continue
            try:
                year_int=int(year)
            except:
                continue
            if year_int<start_date.year or year_int>end_date.year:
                continue
            
            for month in os.listdir(year_path):
                month_path=os.path.join(year_path, month)
                if not os.path.isdir(month_path):
                    continue
                try:
                    month_int=int(month)
                except:
                    continue
                if (year_int==start_date.year and month_int<start_date.month) or \
                   (year_int==end_date.year and month_int>end_date.month):
                    continue
                
                for day in os.listdir(month_path):
                    day_path=os.path.join(month_path, day)
                    if not os.path.isdir(day_path):
                        continue
                    try:
                        day_int=int(day)
                    except:
                        continue
                    current_date=date(year_int, month_int, day_int)
                    if not (start_date<=current_date<=end_date):
                        continue
                    
                    file_path=os.path.join(day_path, "stocks.csv")
                    if os.path.exists(file_path):
                        try:
                            df=pd.read_csv(file_path)
                            all_data.append(df)
                            logger.info(f"Read {len(df)} rows from {year}/{month}/{day}/stocks.csv")
                        except Exception as e:
                            logger.error(f"Failed to read {file_path}: {e}")
        
        if not all_data:
            logger.error(f"No data found for {table_name} - CRITICAL: Base table has no data")
            return None
        
        merged_df=pd.concat(all_data, ignore_index=True)
        merged_path=os.path.join(exec_dir, f"merged_{table_name}.csv")
        merged_df.to_csv(merged_path, index=False)
        logger.info(f"Merged {len(merged_df)} rows for {table_name}")
    else:
        # Live mode
        today=date.today()
        file_path=os.path.join("stocks_data", str(today.year), 
                                f"{today.month:02d}", f"{today.day:02d}", "stocks.csv")
        if not os.path.exists(file_path):
            logger.error(f"No live data for {table_name} - CRITICAL: Base table missing")
            return None
        try:
            merged_df=pd.read_csv(file_path)
            logger.info(f"Read {len(merged_df)} rows from {table_name}")
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None
    
    valid_df,invalid_df=validate_table(merged_df, table_name)
    
    # remove duplicates before saving and loading to DB
    initial_count=len(valid_df)
    valid_df=valid_df.drop_duplicates(subset=['ticker', 'date'], keep='last')
    duplicate=initial_count-len(valid_df)
    if duplicate>0:
        logger.info(f"Removed {duplicate} duplicate records based on ticker-date")
    
    valid_path=os.path.join(exec_dir, f"valid_{table_name}.csv")
    invalid_path=os.path.join(exec_dir, f"invalid_{table_name}.csv")
    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)
    logger.info(f"Valid: {len(valid_df)}, Invalid: {len(invalid_df)}")
    
    # Load to db
    if len(valid_df)>0:
        try:
            create_tbl_stocks_raw(valid_df)
            logger.info(f"Loaded {len(valid_df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            return None
    else:
        logger.error(f"No valid data in {table_name} - CRITICAL: Base table has no valid records")
        return None
    
    return valid_df

def company_metadata(history, start_date, end_date, exec_dir, refs):
    """process company_metadata table """
    table_name="company_metadata"
    logger.info(f"Processing {table_name}")
    
    if history:
        if isinstance(start_date, str):
            start_date=datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date=datetime.strptime(end_date, "%Y-%m-%d").date()
        
        all_data=[]
        table_dir=os.path.join("company_metadata")
        if not os.path.exists(table_dir):
            logger.warning(f"No folder found for {table_name}")
            return None
        
        # Loop through years and read company_details.csv
        for year in os.listdir(table_dir):
            year_path=os.path.join(table_dir, year)
            if not os.path.isdir(year_path):
                continue
            try:
                year_int=int(year)
            except:
                continue
            if year_int<start_date.year or year_int>end_date.year:
                continue
            
            file_path=os.path.join(year_path,"company_details.csv")
            if os.path.exists(file_path):
                try:
                    df=pd.read_csv(file_path)
                    all_data.append(df)
                    logger.info(f"Read {len(df)} rows from {year}/company_details.csv")
                except Exception as e:
                    logger.error(f"Failed to read {file_path}: {e}")
        
        if not all_data:
            logger.warning(f"No data found for {table_name}")
            return None
        
        merged_df=pd.concat(all_data, ignore_index=True)
        merged_path=os.path.join(exec_dir, f"merged_{table_name}.csv")
        merged_df.to_csv(merged_path, index=False)
        logger.info(f"Merged {len(merged_df)} rows for {table_name}")
    else:
        # Live mode - current year
        today=date.today()
        file_path=os.path.join("company_metadata",str(today.year),"company_details.csv")
        if not os.path.exists(file_path):
            logger.warning(f"No live data for {table_name}")
            return None
        try:
            # Read the CSV
            merged_df=pd.read_csv(file_path)
            logger.info(f"Read {len(merged_df)} rows from {file_path}")
            required_columns=['rank','company','industry','revenue','employees','ticker']
            merged_df=merged_df[required_columns]
            logger.info(f"Filtered to {len(required_columns)} columns matching table structure")
            if 'revenue' in merged_df.columns:
                merged_df['revenue']=merged_df['revenue'].astype(str).str.replace('$','').str.replace(',','')
                merged_df['revenue']=pd.to_numeric(merged_df['revenue'],errors='coerce')
                
            if 'employees' in merged_df.columns:
                merged_df['employees']=merged_df['employees'].astype(str).str.replace(',','')
                merged_df['employees']=pd.to_numeric(merged_df['employees'],errors='coerce')
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None
    valid_df, invalid_df=validate_table(merged_df, table_name,refs=refs)
    
    # Remove duplicates
    initial_count=len(valid_df)
    valid_df=valid_df.drop_duplicates(subset=['rank'], keep='last')
    duplicate=initial_count-len(valid_df)
    if duplicate>0:
        logger.info(f"Removed {duplicate} duplicate records based on rank")
    
    valid_path=os.path.join(exec_dir, f"valid_{table_name}.csv")
    invalid_path=os.path.join(exec_dir, f"invalid_{table_name}.csv")
    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)
    logger.info(f"Valid: {len(valid_df)}, Invalid: {len(invalid_df)}")
    
    # Load to db
    if len(valid_df)>0:
        try:
            create_tbl_company_metadata(valid_df)
            logger.info(f"Loaded {len(valid_df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
            logger.error(f"Exception type: {type(e).__name__}")
            import traceback
            logger.error(traceback.format_exc())
            return None  
    else:
        logger.warning(f"No valid data for {table_name}")
        return None
    
    return valid_df

def exchange_rates(history, start_date, end_date, exec_dir):
    """process exchange_rates table """
    table_name="exchange_rates"
    logger.info(f"Processing {table_name}")
    
    if history:
        if isinstance(start_date, str):
            start_date=datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date=datetime.strptime(end_date, "%Y-%m-%d").date()
        
        all_data=[]
        table_dir=os.path.join("exchange_rates")
        if not os.path.exists(table_dir):
            logger.warning(f"No folder found for {table_name}")
            return None
        
        # Read year.csv files
        for file in os.listdir(table_dir):
            if file.endswith(".csv"):
                try:
                    year_int=int(file.replace(".csv", ""))
                except:
                    continue
                if year_int<start_date.year or year_int>end_date.year:
                    continue
                
                file_path=os.path.join(table_dir, file)
                try:
                    df=pd.read_csv(file_path)
                    all_data.append(df)
                    logger.info(f"Read {len(df)} rows from {file}")
                except Exception as e:
                    logger.error(f"Failed to read {file}: {e}")
        
        if not all_data:
            logger.warning(f"No data found for {table_name}")
            return None
        
        merged_df=pd.concat(all_data, ignore_index=True)
        merged_path=os.path.join(exec_dir, f"merged_{table_name}.csv")
        merged_df.to_csv(merged_path, index=False)
        logger.info(f"Merged {len(merged_df)} rows for {table_name}")
    else:
        # Live mode-current year
        today=date.today()
        file_path=os.path.join("exchange_rates", f"{today.year}.csv")
        if not os.path.exists(file_path):
            logger.warning(f"No live data for {table_name}")
            return None
        try:
            merged_df=pd.read_csv(file_path)
            logger.info(f"Read {len(merged_df)} rows from {table_name}")
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None
    valid_df,invalid_df=validate_table(merged_df, table_name)
    
    # Remove duplicates
    initial_count=len(valid_df)
    valid_df=valid_df.drop_duplicates(subset=['date'], keep='last')
    duplicate=initial_count-len(valid_df)
    if duplicate>0:
        logger.info(f"Removed {duplicate} duplicate records based on date")
    
    valid_path=os.path.join(exec_dir, f"valid_{table_name}.csv")
    invalid_path=os.path.join(exec_dir, f"invalid_{table_name}.csv")
    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)
    logger.info(f"Valid: {len(valid_df)}, Invalid: {len(invalid_df)}")
    
    # Load to db
    if len(valid_df)>0:
        try:
            create_tbl_exchange_rates(valid_df)
            logger.info(f"Loaded {len(valid_df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
    
    return valid_df

def macro_raw(history, start_date, end_date, exec_dir):
    """process macro_raw table """
    table_name="macro_raw"
    logger.info(f"Processing {table_name}")
    
    if history:
        if isinstance(start_date, str):
            start_date=datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date=datetime.strptime(end_date, "%Y-%m-%d").date()
        
        all_data=[]
        table_dir=os.path.join("macro_data")
        if not os.path.exists(table_dir):
            logger.warning(f"No folder found for {table_name}")
            return None
        
        # Read year.csv files
        for file in os.listdir(table_dir):
            if file.endswith(".csv"):
                try:
                    year_int=int(file.replace(".csv", ""))
                except (ValueError, TypeError):
                    logger.warning(f"Skipping invalid filename: {file}")
                    continue
                # Include files for years that overlap with date range
                if year_int<start_date.year or year_int>end_date.year:
                    continue
                
                file_path=os.path.join(table_dir, file)
                try:
                    df=pd.read_csv(file_path)
                    if df.empty:
                        logger.warning(f"Skipping empty file: {file}")
                        continue
                    df['year']=year_int
                    df['quarter_num']=df['quarter'].str.extract(r'Q(\d)')[0].astype(int)
                    df['quarter']=df['year'].astype(str)+'-'+df['quarter']
                    
                    all_data.append(df)
                    logger.info(f"Read {len(df)} rows from {file}")
                except Exception as e:
                    logger.error(f"Failed to read {file}: {e}")
                    continue
        
        if not all_data:
            logger.warning(f"No data found for {table_name}")
            return None
        
        merged_df=pd.concat(all_data, ignore_index=True)
        
        #filter by actual quarter range based on start_date and end_date
        start_quarter=(start_date.month-1)//3+1
        end_quarter=(end_date.month-1)//3+1
        
        #filter logic
        filtered_rows=[]
        for idx, row in merged_df.iterrows():
            year=row['year']
            quarter_num=row['quarter_num']
            
            # Include if:
            # - Year is between start and end years
            # - For start year, quarter >= start_quarter
            # - For end year, quarter <= end_quarter
            if year==start_date.year and year==end_date.year:
                if start_quarter<=quarter_num<=end_quarter:
                    filtered_rows.append(row)
            elif year==start_date.year:
                # Start year - include quarters >= start_quarter
                if quarter_num>=start_quarter:
                    filtered_rows.append(row)
            elif year==end_date.year:
                # End year - include quarters <= end_quarter
                if quarter_num<=end_quarter:
                    filtered_rows.append(row)
            elif start_date.year<year<end_date.year:
                # Years in between - include all quarters
                filtered_rows.append(row)
        
        if not filtered_rows:
            logger.warning(f"No data found in date range for {table_name}")
            return None
        
        merged_df=pd.DataFrame(filtered_rows)
        
        # Drop the temporary quarter_num column before saving
        if 'quarter_num' in merged_df.columns:
            merged_df=merged_df.drop(columns=['quarter_num'])
        
        merged_path=os.path.join(exec_dir, f"merged_{table_name}.csv")
        merged_df.to_csv(merged_path, index=False)
        logger.info(f"Merged {len(merged_df)} rows for {table_name} from Q{start_quarter} {start_date.year} to Q{end_quarter} {end_date.year}")
        logger.info(f"Years included: {sorted(merged_df['year'].unique())}")
        logger.info(f"Quarters included: {sorted(merged_df['quarter'].unique())}")
    else:
        # Live mode - read current year file
        today=date.today()
        file_path=os.path.join("macro_data", f"{today.year}.csv")
        if not os.path.exists(file_path):
            logger.warning(f"No live data for {table_name}")
            return None
        try:
            merged_df=pd.read_csv(file_path)
            
            # Add year column from current year
            merged_df['year']=today.year
            
            # Store original quarter for later use
            original_quarter=merged_df['quarter'].copy()
            
            # Create full quarter string (e.g., "2024-Q1")
            merged_df['quarter']=str(today.year)+'-'+merged_df['quarter']
            
            logger.info(f"Read {len(merged_df)} rows from {table_name}")
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None
    
    valid_df, invalid_df=validate_table(merged_df, table_name)
    
    # Remove duplicates
    initial_count=len(valid_df)
    valid_df=valid_df.drop_duplicates(subset=['quarter'], keep='last')
    duplicate=initial_count-len(valid_df)
    if duplicate>0:
        logger.info(f"Removed {duplicate} duplicate records based on quarter")
    
    valid_path=os.path.join(exec_dir, f"valid_{table_name}.csv")
    invalid_path=os.path.join(exec_dir, f"invalid_{table_name}.csv")
    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)
    logger.info(f"Valid: {len(valid_df)}, Invalid: {len(invalid_df)}")
    
    # Load to db
    if len(valid_df)>0:
        try:
            create_tbl_macro_raw(valid_df)
            logger.info(f"Loaded {len(valid_df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
    
    return valid_df

def sentiment(history, start_date, end_date, exec_dir, refs):
    """process sentiment table """
    table_name="sentiment"
    logger.info(f"Processing {table_name}")
    
    if history:
        logger.warning(f"No historical data available for {table_name} - only live data exists")
        return None
    else:
        # Live mode only
        today=date.today()
        file_path=os.path.join("sentiment_analysis", str(today.year), 
                                f"{today.month:02d}", f"{today.day:02d}", "sentiment.csv")
        if not os.path.exists(file_path):
            logger.warning(f"No live data for {table_name}")
            return None
        try:
            merged_df=pd.read_csv(file_path)
            logger.info(f"Read {len(merged_df)} rows from {table_name}")
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return None
    
    # Apply rules with refs to validate against stocks_raw
    valid_df,invalid_df=validate_table(merged_df, table_name, refs=refs)
    
    # Remove duplicates
    initial_count=len(valid_df)
    valid_df=valid_df.drop_duplicates(subset=['ticker', 'date'], keep='last')
    duplicate=initial_count-len(valid_df)
    if duplicate>0:
        logger.info(f"Removed {duplicate} duplicate records based on ticker-date")
    
    valid_path=os.path.join(exec_dir, f"valid_{table_name}.csv")
    invalid_path=os.path.join(exec_dir, f"invalid_{table_name}.csv")
    valid_df.to_csv(valid_path, index=False)
    invalid_df.to_csv(invalid_path, index=False)
    logger.info(f"Valid: {len(valid_df)}, Invalid: {len(invalid_df)}")
    
    # Load to db
    if len(valid_df)>0:
        try:
            create_tbl_sentiment(valid_df)
            logger.info(f"Loaded {len(valid_df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load {table_name}: {e}")
    
    return valid_df

def merge(history, start_date=None, end_date=None):
    """Main merge function with stocks_raw as base table"""
    cwd=os.getcwd()
    exec_dir=execution_dir()
    
    logger.info("Starting merge process")
    logger.info(f"Mode: {'Historical' if history else 'Live'}")
    if history:
        logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Execution dir: {exec_dir}")    
    refs={}
    
    #Process stocks_raw 
    logger.info("\n"+"="*60)
    logger.info("Processing BASE TABLE (stocks_raw)")
    stocks_df=stocks_raw(history, start_date, end_date, exec_dir)
    
    if stocks_df is None or len(stocks_df)==0:
        logger.error("="*60)
        logger.error("Base table stocks_raw failed to process")
        logger.error("Cannot continue without base table - terminating pipeline")
        return
    
    refs["stocks_raw"]=stocks_df
    unique_tickers=stocks_df['ticker'].unique()
    logger.info(f"Base table established with {len(stocks_df)} records")
    logger.info(f"Found {len(unique_tickers)} unique tickers: {', '.join(sorted(unique_tickers))}")
    
    #Process dependent tables (reference stocks_raw)
    logger.info("\n"+"="*60)
    logger.info("Processing DEPENDENT TABLES")
    
    # Process company_metadata with reference to stocks_raw
    cm_df=company_metadata(history, start_date, end_date, exec_dir, refs)
    if cm_df is not None:
        refs["company_metadata"]=cm_df
        logger.info(f"company_metadata processed and linked to stocks_raw")
    else:
        logger.warning("company_metadata not available - continuing without it")
    
    #Process independent tables
    logger.info("\n"+"="*60)
    logger.info("Processing INDEPENDENT TABLES")

    # Process exchange_rates (independent table)
    er_df=exchange_rates(history, start_date, end_date, exec_dir)
    if er_df is not None:
        logger.info(f"exchange_rates processed")
    else:
        logger.warning("exchange_rates not available")
    
    # Process macro_raw (independent table)
    macro_df=macro_raw(history, start_date, end_date, exec_dir)
    if macro_df is not None:
        logger.info(f"macro_raw processed")
    else:
        logger.warning("macro_raw not available")
    
    #Process sentiment (references stocks_raw, live mode only)
    logger.info("\n"+"="*60)
    logger.info("Processing SENTIMENT DATA")    
    sentiment_df=sentiment(history, start_date, end_date, exec_dir, refs)
    if sentiment_df is not None:
        logger.info(f"sentiment processed and linked to stocks_raw")
    else:
        logger.warning("sentiment not available")
    
    #calculate KPIs and generate fact tables
    logger.info("\n"+"="*60)
    logger.info("KPI CALCULATIONS AND FACT TABLE GENERATION")
    
    try:
        fact_tables=calculate_all_kpis(exec_dir,start_date)
        
        if fact_tables:
            logger.info("KPI calculations completed successfully")
            
            # Load fact tables to database
            try:
                create_tbl_stock_facts(fact_tables['stock_facts'])
                logger.info("Loaded stock_facts to database")
                
                create_tbl_macro_facts(fact_tables['macro_facts'])
                logger.info("Loaded macro_facts to database")
                
                create_tbl_sector_lookup(fact_tables['sector_lookup'])
                logger.info("Loaded sector_lookup to database")
                
                create_tbl_analytics_summary(fact_tables['analytics_summary'])
                logger.info("Loaded analytics_summary to database")
                
            except Exception as e:
                logger.error(f"Failed to load fact tables to database: {e}")
        else:
            logger.warning("KPI calculation returned no data")
            
    except Exception as e:
        logger.error(f"Failed to calculate KPIs: {e}")
 
    return exec_dir
        
    

if __name__=="__main__":
    merge(history=True, start_date="2024-11-01", end_date="2024-12-31")
    # merge(history=False)