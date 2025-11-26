"""
__title__: Main Scirpt for the Local Data warehouse Project
__employee__id: 800338
__author__: Shalini Tata
__created__: 22-11-2024
__purpose__: Extract the different indicator values required for the project and call merge function
version :v1.0
"""

import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
import time
import requests
from logger import logger
from merge import merge
import sys
import os
import json
import schedule
from report import generate_all_reports

def stocks_hist(year:int,start_date:str,end_date:str):
    """fetch historical stock data for a specific year """
    logger.info(f"\n{'='*60}")
    logger.info(f"Fetching HISTORICAL data for {year}")
    logger.info(f"Period: {start_date} to {end_date}")
    
    
    # load companies from that year
    csv_file=Path(f"top_companies/fortune_{year}.csv")
    if not csv_file.exists():
        logger.error(f"Error: {csv_file} not found")
        return
    
    df=pd.read_csv(csv_file)
    ticker_comp=df[df['ticker'].notna()].copy()
    
    logger.info(f"total companies with tickers: {len(ticker_comp)}")
    
    # parse dates
    start=datetime.strptime(start_date,"%Y-%m-%d")
    end=datetime.strptime(end_date,"%Y-%m-%d")
    
    # check which dates already exist (quick scan)
    year_folder=Path(f"stocks_data/{year}")
    existing_dates=set()
    
    #collect dates which already have stocks data
    if year_folder.exists():
        for stocks_file in year_folder.rglob("stocks.csv"):
            try:
                # extract date from path -> stocks_data/year/month/day/stocks.csv
                parts=stocks_file.parts
                y,m,d=parts[-4],parts[-3],parts[-2]
                existing_dates.add(f"{y}-{m}-{d}")
            except:
                pass
    
    logger.info(f"found {len(existing_dates)} days already saved")
    
    # fetch data for each company 
    all_data={}
    for idx,row in ticker_comp.iterrows():
        company=row['company']
        ticker=row['ticker']       
        logger.info(f"\n[{idx+1}] ->  {company} ({ticker})")    
        try:
            stock=yf.Ticker(ticker)
            if start_date==end_date:
                end_date_new=(datetime.strptime(end_date,"%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                end_date_new=end_date
            hist=stock.history(start=start_date,end=end_date_new)            
            if hist.empty:
                logger.info(f"No data available")
                continue
            
            logger.info(f"Got {len(hist)} days of data")
            all_data[ticker]={'company':company,'data':hist}
            
        except Exception as e:
            logger.error(f"Error: {e}")
            continue
        
        time.sleep(0.01)
    
    logger.info("Historic stocks data collected")


def stocks_live():
    """fetch live stock data for current year companies"""
    cur_year=datetime.now().year
    today=datetime.now()
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Fetching LIVE data for {cur_year}")
    logger.info(f"Date: {today.strftime('%Y-%m-%d')}")
    
    # load current year companies
    csv_file=Path(f"top_companies/fortune_{cur_year}.csv")
    if not csv_file.exists():
        logger.info(f"Error: {csv_file} not found")
        return
    
    df=pd.read_csv(csv_file)
    ticker_comp=df[df['ticker'].notna()].copy()
    
    logger.info(f"Total companies with tickers: {len(ticker_comp)}")
    
    # check if today's data already exists
    yr_str=today.strftime("%Y")
    month_str=today.strftime("%m")
    day_str=today.strftime("%d")
    output_dir=Path(f"stocks_data/{yr_str}/{month_str}/{day_str}")
    output_file=output_dir/"stocks.csv"
    
    if output_file.exists():
        logger.info(f"Today's data already exists — skipping")
        return
    
    daily_records=[]
    
    for idx,row in ticker_comp.iterrows():
        company=row['company']
        ticker=row['ticker']
        
        logger.info(f"\n[{idx+1}/{len(ticker_comp)}] {company} ({ticker})")
        
        try:
            stock=yf.Ticker(ticker)
            hist=stock.history(period="2d")
            
            if hist.empty:
                logger.info(f"No data available")
                continue
            
            latest=hist.iloc[-1]
            latest_date=hist.index[-1].strftime("%Y-%m-%d")
            
            logger.info(f"Got data for {latest_date}")
            
            daily_records.append({
                'company':company,
                'ticker':ticker,
                'date':latest_date,
                'open':round(latest['Open'],2),
                'high':round(latest['High'],2),
                'low':round(latest['Low'],2),
                'close':round(latest['Close'],2),
                'volume':int(latest['Volume'])
            })
            
        except Exception as e:
            logger.error(f"  Error: {e}")
            continue
        
        time.sleep(0.01)
    
    # save today's data 
    if daily_records:
        output_dir.mkdir(parents=True,exist_ok=True)
        daily_df=pd.DataFrame(daily_records)
        daily_df.to_csv(output_file,index=False)
        logger.info(f"Saved: {output_file} ({len(daily_df)} companies)")
    else:
        logger.warning("No live daily records collected — nothing to save")



def comp_data_hist(start_year, end_year):
    """company metadata files for historical years"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Creating HISTORICAL company metadata")
    logger.info(f"Years: {start_year} to {end_year}")
   
    
    for year in range(start_year, end_year+1):
        csv_file=Path(f"top_companies/fortune_{year}.csv")      
        if not csv_file.exists():
            logger.info(f"\n{year}: CSV not found, skipping")
            continue
        
        output_dir=Path(f"company_metadata/{year}")
        output_file=output_dir/"company_details.csv"
        
        if output_file.exists():
            logger.info(f"\n{year}: Metadata already exists — skipping")
            continue
        
        logger.info(f"\n{year}: Creating metadata...")
        
        # get tickers from stock data
        stocks_dir=Path(f"stocks_data/{year}")
        tkr_data=set()
        
        if stocks_dir.exists():
            # find first stocks.csv file and read it
            stock_files=list(stocks_dir.rglob("stocks.csv"))
            if stock_files:
                try:
                    stocks_df=pd.read_csv(stock_files[0])
                    if 'ticker' in stocks_df.columns:
                        tkr_data=set(stocks_df['ticker'].unique())
                except:
                    pass
        
        if not tkr_data:
            logger.info(f"No stock data found")
            continue
        
        logger.info(f"Found {len(tkr_data)} tickers with stock data")
        
        # load fortune csv
        df=pd.read_csv(csv_file)
        df_tkr=df[df['ticker'].isin(tkr_data)].copy()
        if len(df_tkr)==0:
            logger.info(f"No matching companies found")
            continue
        # prepare metadata 
        metadata_df=pd.DataFrame()
        if 'rank' in df_tkr.columns:
            metadata_df['rank']=df_tkr['rank'].values
        if 'company' in df_tkr.columns:
            metadata_df['company']=df_tkr['company'].values
        if 'industry' in df_tkr.columns:
            metadata_df['industry']=df_tkr['industry'].values
        if 'revenue (rounded)' in df_tkr.columns:
            metadata_df['revenue']=df_tkr['revenue (rounded)'].values
        elif 'revenue' in df_tkr.columns:
            metadata_df['revenue']=df_tkr['revenue'].values    
        if 'employees' in df_tkr.columns:
            metadata_df['employees']=df_tkr['employees'].values
        if 'ticker' in df_tkr.columns:
            metadata_df['ticker']=df_tkr['ticker'].values

        output_dir.mkdir(parents=True, exist_ok=True)
        metadata_df.to_csv(output_file, index=False)
        
        logger.info(f" Saved: {output_file} ({len(metadata_df)} companies)")

    logger.info(f"\nMetadata creation completed")

def comp_data_live():
    """create company metadata for current year"""
    cur_yr=datetime.now().year
    logger.info(f"\n{'='*60}")
    logger.info(f"Creating LIVE company metadata for {cur_yr}")
    
    csv_file=Path(f"top_companies/fortune_{cur_yr}.csv")
    
    if not csv_file.exists():
        logger.info(f"Error: {csv_file} not found")
        return
    
    output_dir=Path(f"company_metadata/{cur_yr}")
    output_file=output_dir/"company_details.csv"
    
    if output_file.exists():
        logger.info(f"Metadata already exists — skipping")
        return
    logger.info(f"Creating metadata...")
    
    df=pd.read_csv(csv_file)
    
    # get tickers that have stock data
    stocks_dir=Path(f"stocks_data/{cur_yr}")
    tkr_data=set()
    
    if stocks_dir.exists():
        for stocks_file in stocks_dir.rglob("stocks.csv"):
            try:
                stocks_df=pd.read_csv(stocks_file)
                if 'ticker' in stocks_df.columns:
                    tkr_data.update(stocks_df['ticker'].unique())
            except:
                continue
    
    logger.info(f"Found {len(tkr_data)} tickers with stock data")
    
    # filtering companies that have stock data
    df_tkr=df[df['ticker'].isin(tkr_data)].copy()
    
    if len(df_tkr)==0:
        logger.info(f"No companies with stock data found")
        return
    
    # preparing metadata
    metadata_records=[]
    
    for idx, row in df_tkr.iterrows():
        logger.info(f"[{idx+1}] -> Fetching data for {row['company']} ({row['ticker']})")
        
        record={
            'rank': row.get('rank'),
            'company': row.get('company'),
            'industry': row.get('industry'),
            'revenue': row.get('revenue (rounded)') or row.get('revenue'),
            'employees': row.get('employees'),
            'ticker': row.get('ticker')
        }
        
        # fetch additional data from yfinance
        try:
            stock=yf.Ticker(row['ticker'])
            info=stock.info
            
            record['marketCap']=info.get('marketCap')
            record['sector']=info.get('sector')
            record['country']=info.get('country')
            record['exchange']=info.get('exchange')
            record['currency']=info.get('currency')
            
            logger.info(f"Got yfinance data (marketCap: {record['marketCap']})")
            
        except Exception as e:
            logger.info(f"  yfinance error: {e}")
            record['marketCap']=None
            record['sector']=None
            record['country']=None
            record['exchange']=None
            record['currency']=None
        
        metadata_records.append(record)
        time.sleep(0.01)
    
    metadata_df=pd.DataFrame(metadata_records)
    
    output_dir.mkdir(parents=True,exist_ok=True)
    metadata_df.to_csv(output_file,index=False)
    
    logger.info(f"\nSaved: {output_file} ({len(metadata_df)} companies)")


fred_key="44858ac33dc4109129a6900fd0d0854c"

# Macroeconomic indicators to fetch
indicators={
    "GDP": "GDP",
    "inflation": "CPIAUCSL",
    "unemployment": "UNRATE",
    "interest_rate": "FEDFUNDS",
    "consumer_spending": "PCE",
    "industrial_production": "INDPRO",
    "housing_starts": "HOUST",
    "retail_sales": "RSXFS"
}

def fred_series(series_id,start,end):
    """fetch data from FRED api"""
    url="https://api.stlouisfed.org/fred/series/observations"
    params={
        "series_id": series_id,
        "api_key": fred_key,
        "file_type": "json",
        "observation_start": start,
        "observation_end": end
    }
    
    try:
        r=requests.get(url,params=params,timeout=10)
        r.raise_for_status()
        data=r.json()["observations"]
        df=pd.DataFrame(data)
        df["date"]=pd.to_datetime(df["date"])
        df["value"]=pd.to_numeric(df["value"],errors="coerce")
        return df[["date", "value"]]
    except Exception as e:
        logger.info(f"Error fetching {series_id}: {e}")
        return pd.DataFrame()

def macro_hist(start_year, end_year):
    """
    create yearly macroeconomic data for historic data"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Creating HISTORICAL macro data")
    logger.info(f"Years: {start_year} to {end_year}")
    
    for year in range(start_year,end_year+1):
        output_file=Path(f"macro_data/{year}.csv")
        
        if output_file.exists():
            logger.info(f"\n{year}: already exists — skipping")
            continue
        
        logger.info(f"\n{year}: creating macro data...")
        
        start_date=f"{year}-01-01"
        end_date=f"{year}-12-31"
        
        # fetch all indicators for this year
        yearly_data=[]
        
        for name, ser_id in indicators.items():
            logger.info(f"  Fetching {name}...", end=" ")
            df=fred_series(ser_id,start_date,end_date)
            
            if df.empty:
                logger.info("No data")
                continue
            
            # calculate quarterly averages
            q_values={}
            for quarter in range(1, 5):
                q_start={1: f"{year}-01-01", 2: f"{year}-04-01", 3: f"{year}-07-01", 4: f"{year}-10-01"}[quarter]
                q_end={1: f"{year}-03-31", 2: f"{year}-06-30", 3: f"{year}-09-30", 4: f"{year}-12-31"}[quarter]
                
                mask=(df["date"]>=q_start) & (df["date"]<=q_end)
                quarter_data=df[mask]
                
                if not quarter_data.empty:
                    q_values[f"Q{quarter}"]=round(quarter_data["value"].mean(), 4)
                else:
                    q_values[f"Q{quarter}"]=None
            
            yearly_data.append({
                "indicator": name,
                "Q1": q_values.get("Q1"),
                "Q2": q_values.get("Q2"),
                "Q3": q_values.get("Q3"),
                "Q4": q_values.get("Q4")
            })
            
            logger.info(f"Done")
        
        if yearly_data:
            output_file.parent.mkdir(parents=True,exist_ok=True)
            year_df=pd.DataFrame(yearly_data)
            # transpose: rows become columns, columns become rows
            year_df=year_df.set_index('indicator').T
            year_df.index.name='quarter'
            year_df.reset_index(inplace=True)
            year_df.to_csv(output_file, index=False)
            logger.info(f"Saved: {output_file} ({len(yearly_data)} indicators)")
        else:
            logger.info(f"No data for {year}")
    
    logger.info(f"\nMacro data creation complete")

def macro_live():
    """create macroeconomic data for current year"""
    cur_yr=datetime.now().year
    
    logger.info(f"\n{'='*60}")
    logger.info(f"creating LIVE macro data for {cur_yr}")
    output_file=Path(f"macro_data/{cur_yr}.csv")
    
    if output_file.exists():
        logger.info(f"current year data already exists — skipping")
        return
    
    logger.info(f"Fetching data for {cur_yr}...")
    
    start_date=f"{cur_yr}-01-01"
    end_date=datetime.now().strftime("%Y-%m-%d") 
    yearly_data=[]
    
    for name,ser_id in indicators.items():
        logger.info(f"Fetching {name}...")
        df=fred_series(ser_id,start_date, end_date)
        
        if df.empty:
            logger.info("No data")
            continue
        
        #calculate quarterly average 
        q_values={}
        for quarter in range(1, 5):
            q_start={1: f"{cur_yr}-01-01", 2: f"{cur_yr}-04-01", 
                     3: f"{cur_yr}-07-01", 4: f"{cur_yr}-10-01"}[quarter]
            q_end={1: f"{cur_yr}-03-31", 2: f"{cur_yr}-06-30", 
                   3: f"{cur_yr}-09-30", 4: f"{cur_yr}-12-31"}[quarter]
            
            #up to today
            if q_start>end_date:
                q_values[f"Q{quarter}"]=None
                continue
            
            actual_end=min(q_end,end_date)
            mask=(df["date"]>=q_start) & (df["date"]<=actual_end)
            q_data=df[mask]
            
            if not q_data.empty:
                q_values[f"Q{quarter}"]=round(q_data["value"].mean(), 4)
            else:
                q_values[f"Q{quarter}"]=None
        
        yearly_data.append({
            "indicator": name,
            "Q1": q_values.get("Q1"),
            "Q2": q_values.get("Q2"),
            "Q3": q_values.get("Q3"),
            "Q4": q_values.get("Q4")
        })
        
        logger.info(f"Done")
    
    if yearly_data:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        year_df=pd.DataFrame(yearly_data)
        year_df=year_df.set_index('indicator').T
        year_df.index.name='quarter'
        year_df.reset_index(inplace=True)
        year_df.to_csv(output_file, index=False)
        logger.info(f"\nSaved: {output_file} ({len(yearly_data)} indicators)")
    else:
        logger.info("\nNo data fetched")



news_key="e4ddb799366a45f4a9ff5d27b3e53fab"  

def sentiment_score(text):
    """sentiment calculation based on keywords"""
    if not text:
        return 0
    
    text=text.lower()
    positive=["profit", "growth", "gain", "surge", "bullish", "record", "strong", 
              "beat", "exceed", "rally", "upgrade", "positive", "success", "rise"]
    negative=["loss", "decline", "fall", "bearish", "weak", "miss", "drop", 
              "cut", "downgrade", "negative", "risk", "concern", "plunge"]
    
    pos_cnt=sum(1 for word in positive if word in text)
    neg_cnt=sum(1 for word in negative if word in text)
    
    if pos_cnt+neg_cnt==0:
        return 0
    
    return (pos_cnt-neg_cnt)/(pos_cnt+neg_cnt)

def sentiment_live():
    """fetch live sentiment using NewsAPI"""
    tday=datetime.now()
    yesterday=(tday-timedelta(days=1)).strftime("%Y-%m-%d")
    yr_str=tday.strftime("%Y")
    month_str=tday.strftime("%m")
    day_str=tday.strftime("%d")
    
    op_dir=Path(f"sentiment_analysis/{yr_str}/{month_str}/{day_str}")
    op_file=op_dir/"sentiment.csv"
    
    if op_file.exists():
        logger.info(f"sentiment already exists — skipping")
        return
    
    logger.info(f"\nfetching sentiment for {tday.strftime('%Y-%m-%d')}...")
    
    # load tickers and company names
    comp_mtdt=Path(f"company_metadata/{tday.year}/company_details.csv")
    if not comp_mtdt.exists():
        logger.info(f"Error: {comp_mtdt} not found")
        return
    
    df=pd.read_csv(comp_mtdt)
    results=[]
    
    for idx, row in df.iterrows():
        comp=row['company']
        ticker=row['ticker']   
        if pd.isna(ticker):
            continue
        
        logger.info(f"[{idx+1}/{len(df)}] {comp} ({ticker})")

        # fetch news for this company
        url="https://newsapi.org/v2/everything"
        params={
            "apiKey":news_key,
            "q": comp,
            "from": yesterday,
            "language": "en",
            "sortBy": "relevancy",
            "pageSize": 10
        }
        
        try:
            r=requests.get(url,params=params,timeout=10)
            r.raise_for_status()
            data=r.json()
            
            if data.get("status")!="ok":
                logger.error(f"Error")
                continue
            
            articles=data.get("articles", [])
            
            if not articles:
                logger.info(f"No news")
                continue
            
            # calculate sentiment from headlines and descriptions
            scores=[]
            for article in articles:
                title=article.get("title", "")
                desc=article.get("description", "")
                score=sentiment_score(f"{title} {desc}")
                scores.append(score)
            
            if scores:
                avg_score=sum(scores)/len(scores)
                bull=sum(1 for s in scores if s>0.2)
                bear=sum(1 for s in scores if s<-0.2)
                neutral=len(scores)-bull-bear
                
                results.append({
                    "ticker": ticker,
                    "company": comp,
                    "date": tday.strftime("%Y-%m-%d"),
                    "sentiment_score": round(avg_score, 4),
                    "article_count": len(scores),
                    "bullish": bull,
                    "bearish": bear,
                    "neutral": neutral
                })
                
                logger.info(f"Score:{avg_score:.3f} ({len(scores)} articles)")
        
        except Exception as e:
            logger.info(f"Error: {e}")
    
    if results:
        op_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(results).to_csv(op_file, index=False)
        logger.info(f"\nSaved: {op_file} ({len(results)} companies)")
    else:
        logger.info("\nNo sentiment data")



def exchange_rate(start_date, end_date):
    """fetch USD-INR exchange rate from FRED"""
    url="https://api.stlouisfed.org/fred/series/observations"
    params={
        "series_id": "DEXINUS",
        "api_key": fred_key,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date
    }
    
    try:
        r=requests.get(url,params=params,timeout=10)
        r.raise_for_status()
        data=r.json()
        
        if "observations" not in data:
            logger.info(f"No data available")
            return pd.DataFrame()
        
        obs=data["observations"]
        df=pd.DataFrame(obs)
        df["date"]=pd.to_datetime(df["date"])
        df["value"]=pd.to_numeric(df["value"], errors="coerce")
        df=df[df["value"].notna()]
        df=df.rename(columns={"value": "usd_inr_rate"})
        return df[["date", "usd_inr_rate"]]
    
    except Exception as e:
        logger.info(f"Error: {e}")
        return pd.DataFrame()

def exchange_hist(start_year, end_year):
    """fetch historical USD-INR exchange rates from FRED"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Fetching HISTORICAL exchange rates (USD-INR) from FRED")
    logger.info(f"Years: {start_year} to {end_year}")
    
    for year in range(start_year, end_year+1):
        output_file=Path(f"exchange_rates/{year}.csv")   
        if output_file.exists():
            logger.info(f"\n{year}: Already exists — skipping")
            continue
        logger.info(f"\n{year}: Fetching exchange rates...")  
        start_date=f"{year}-01-01"
        end_date=f"{year}-12-31"
        df=exchange_rate(start_date, end_date)
        
        if not df.empty:
            output_file.parent.mkdir(parents=True, exist_ok=True)
            df["date"]=df["date"].dt.strftime("%Y-%m-%d")
            df.to_csv(output_file, index=False)
            logger.info(f"  Saved: {output_file} ({len(df)} days)")
        else:
            logger.info(f"  No data for {year}")
    
    logger.info(f"\nExchange rate data complete")

def exchange_live():
    """Fetch current year USD-INR exchange rates from FRED"""
    current_year=datetime.now().year
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Fetching LIVE exchange rates (USD-INR) from FRED for {current_year}") 
    output_file=Path(f"exchange_rates/{current_year}.csv")
    
    if output_file.exists():
        logger.info(f"Current year data already exists — skipping")
        return 
    logger.info(f"Fetching rates for {current_year}...")
    start_date=f"{current_year}-01-01"
    end_date=datetime.now().strftime("%Y-%m-%d")
    df=exchange_rate(start_date, end_date)
    
    if not df.empty:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df["date"]=df["date"].dt.strftime("%Y-%m-%d")
        df.to_csv(output_file, index=False)
        logger.info(f"\nSaved: {output_file} ({len(df)} days)")
    else:
        logger.info("\nNo data fetched")

def run_daily_live_collection():
    """collect all live data tables one by one"""
    logger.info("="*60)
    logger.info("DAILY LIVE DATA COLLECTION STARTED")
    logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    
    try:
        logger.info("\n[1/5] Collecting live stocks data...")
        stocks_live()
        
        logger.info("\n[2/5] Collecting company metadata...")
        comp_data_live()
        
        logger.info("\n[3/5] Collecting macroeconomic data...")
        macro_live()
        
        logger.info("\n[4/5] Collecting sentiment analysis...")
        sentiment_live()
        
        logger.info("\n[5/5] Collecting exchange rates...")
        exchange_live()
        
        logger.info("\n[MERGE] Merging all collected data...")
        exec_dir=merge(history=False)
        reports_out_dir=os.path.join(exec_dir, "reports")
        start_date="1996-01-01"
        end_date= datetime.today().strftime("%Y-%m-%d")
        report_results=generate_all_reports(exec_dir, reports_out_dir,start_date,end_date)
        
        logger.info("="*60)
        logger.info("DAILY COLLECTION COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error during daily collection: {e}")

with open("config.json") as f:
    config=json.load(f)

def run_live_schedule():
    """run continuous live data collection scheduled at 10:30 AM daily"""
    logger.info("="*60)
    logger.info("LIVE SCHEDULER STARTED")
    logger.info("="*60)
    
    schedule.every().day.at(config['sched_time']).do(run_daily_live_collection)
    
    logger.info("Scheduled task:")
    logger.info("- Daily live data collection: Every day")
    logger.info("  (Stocks -> Metadata -> Macro -> Sentiment -> Exchange -> Merge)")
    logger.info("="*60)
    logger.info("Scheduler is now running.")
    
    while True:
        schedule.run_pending()
        time.sleep(30)


# MAIN EXECUTION

if __name__=="__main__":
    if len(sys.argv)>1:
        if sys.argv[1]=="--mode" and len(sys.argv)>2:
            if sys.argv[2]=="live":
                run_live_schedule()
                sys.exit(0)

        elif sys.argv[1]=="--config" and len(sys.argv)>2:
            config_path=sys.argv[2]
            if not os.path.exists(config_path):
                logger.error(f"Config file not found: {config_path}")
                sys.exit(1)

            try:
                with open(config_path,'r') as f:
                    config=json.load(f)

                # safe sched_time handling (default + validation)
                sched_time=config.get("sched_time","10:30")
                if not isinstance(sched_time,str):
                    logger.warning("sched_time must be a string - using default 10:30")
                    sched_time="10:30"
                import re
                if not re.match(r"^([01]\d|2[0-3]):[0-5]\d$",sched_time):
                    logger.warning(f"sched_time '{sched_time}' invalid - expected HH:MM. Using default 10:30")
                    sched_time="10:30"
                config["sched_time"]=sched_time
                logger.info(f"Scheduler time set to: {sched_time}")

                start_date_str=config.get("start_date")
                end_date_str=config.get("end_date")
                if not start_date_str or not end_date_str:
                    logger.error("Config must contain start_date and end_date")
                    sys.exit(1)

                try:
                    start_dt=datetime.strptime(start_date_str,"%Y-%m-%d").date()
                    end_dt=datetime.strptime(end_date_str,"%Y-%m-%d").date()
                except Exception as e:
                    logger.error(f"Invalid date format in config. Use YYYY-MM-DD. Error: {e}")
                    sys.exit(1)

                if start_dt>end_dt:
                    logger.error("start_date must be before or equal to end_date in config")
                    sys.exit(1)

                today=datetime.today().date()
                logger.info("="*60)

                # if both dates equal today -> start live schedule
                if start_dt==end_dt==today:
                    logger.info("Start and end date are both today's date. Starting live scheduler.")
                    logger.info(f"Date: {today.isoformat()}")
                    logger.info("="*60)
                    run_live_schedule()
                    sys.exit(0)

                s_year=start_dt.year
                e_year=end_dt.year

                logger.info("HISTORICAL PROCESSING FROM CONFIG")
                logger.info(f"Range: {start_date_str} to {end_date_str}")
                logger.info("="*60)

                for yr in range(s_year,e_year+1):
                    y_start=max(start_date_str,f"{yr}-01-01")
                    y_end=min(end_date_str,f"{yr}-12-31")
                    stocks_hist(year=yr,start_date=y_start,end_date=y_end)

                comp_data_hist(s_year,e_year)
                macro_hist(s_year,e_year)
                exchange_hist(s_year,e_year)

                #call merge and get exec_dir
                exec_dir=merge(history=True,start_date=start_date_str,end_date=end_date_str)

                logger.info("\n"+"="*60)
                logger.info("GENERATING REPORTS")
                logger.info(f"Date Range: {start_date_str} to {end_date_str}")
                logger.info("="*60)
                try:
                    from report import generate_all_reports
                    reports_out_dir=os.path.join(exec_dir,"reports")
                    report_results=generate_all_reports(exec_dir,reports_out_dir,start_date_str,end_date_str)
                    logger.info("Reports generated successfully")
                    logger.info(f"Reports saved to: {reports_out_dir}")
                except Exception as e:
                    logger.error(f"failed to generate reports: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

                logger.log_footer()

            except json.JSONDecodeError:
                logger.error("invalid JSON in config file")
                sys.exit(1)
            except Exception as e:
                logger.error(f"Error processing config: {e}")
                sys.exit(1)

    else:
        logger.error("No arguments provided. Use --mode live or --config <path_to_config>")
        sys.exit(1)
