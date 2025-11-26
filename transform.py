"""
Title: Transform Script for KPI Calculations
Author: Shalini Tata
Created: 24-11-2024
Description: Calculate all KPI metrics from validated data and generate fact tables
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
from logger import logger

def calculate_all_kpis(exec_dir,start_date=None):
    """calculate all KPI metrics from valid data and generate fact tables"""
    logger.info("Starting KPI calculations")
    
    #load all valid data
    try:
        #check if files exist 
        stocks_file=f"{exec_dir}/valid_stocks_raw.csv"
        macro_file=f"{exec_dir}/valid_macro_raw.csv"
        company_file=f"{exec_dir}/valid_company_metadata.csv"
        exchange_file=f"{exec_dir}/valid_exchange_rates.csv"
        
        if not os.path.exists(stocks_file):
            logger.error(f"File not found:{stocks_file}")
            return None
        if not os.path.exists(macro_file):
            logger.error(f"File not found:{macro_file}")
            return None
        if not os.path.exists(company_file):
            logger.error(f"File not found:{company_file}")
            return None
        if not os.path.exists(exchange_file):
            logger.error(f"File not found:{exchange_file}")
            return None
        
        #read files 
        stocks_df=pd.read_csv(stocks_file)
        macro_df=pd.read_csv(macro_file)
        company_df=pd.read_csv(company_file)
        exchange_df=pd.read_csv(exchange_file)
        
        logger.info(f"Loaded stocks_raw: {len(stocks_df)} rows")
        logger.info(f"Loaded macro_raw: {len(macro_df)} rows")
        logger.info(f"Loaded company_metadata: {len(company_df)} rows")
        logger.info(f"Loaded exchange_rates: {len(exchange_df)} rows")
        
        #check if dataframes are empty
        if len(stocks_df)==0:
            logger.error("stocks_raw is empty")
            return None
        if len(macro_df)==0:
            logger.error("macro_raw is empty")
            return None
        if len(company_df)==0:
            logger.error("company_metadata is empty")
            return None
        if len(exchange_df)==0:
            logger.error("exchange_rates is empty")
            return None
            
    except Exception as e:
        logger.error(f"failed to load valid data: {e}")
        return None
    
    #convert date columns
    try:
        stocks_df['date']=pd.to_datetime(stocks_df['date'])
        exchange_df['date']=pd.to_datetime(exchange_df['date'])
    except Exception as e:
        logger.error(f"failed to convert date columns: {e}")
        return None
    
    #calculate stock_facts
    logger.info("calculating stock_facts metrics...")
    try:
        stocks_df=stocks_df.sort_values(['ticker','date'])
        
        #calculate daily return
        stocks_df['previous_close']=stocks_df.groupby('ticker')['close'].shift(1)
        stocks_df['daily_return']=np.where(
            (stocks_df['previous_close'].notna()) & (stocks_df['previous_close']!=0),
            ((stocks_df['close']-stocks_df['previous_close'])/stocks_df['previous_close'])*100,
            0
        )
        
        #replace inf and -inf with 0
        stocks_df['daily_return']=stocks_df['daily_return'].replace([np.inf,-np.inf], 0)
        stocks_df['daily_return']=stocks_df['daily_return'].fillna(0)
        
        #calculating volatility 30 day
        stocks_df['volatility_30d']=stocks_df.groupby('ticker')['daily_return'].transform(
            lambda x: x.rolling(window=30, min_periods=1).std()
        )
        stocks_df['volatility_30d']=stocks_df['volatility_30d'].replace([np.inf,-np.inf], 0)
        stocks_df['volatility_30d']=stocks_df['volatility_30d'].fillna(0)
        
        #calculate moving averages
        stocks_df['moving_avg_30d']=stocks_df.groupby('ticker')['close'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        stocks_df['moving_avg_30d']=stocks_df['moving_avg_30d'].replace([np.inf,-np.inf], 0)
        stocks_df['moving_avg_30d']=stocks_df['moving_avg_30d'].fillna(0)
        
        stocks_df['moving_avg_90d']=stocks_df.groupby('ticker')['close'].transform(
            lambda x: x.rolling(window=90, min_periods=1).mean()
        )
        stocks_df['moving_avg_90d']=stocks_df['moving_avg_90d'].replace([np.inf,-np.inf], 0)
        stocks_df['moving_avg_90d']=stocks_df['moving_avg_90d'].fillna(0)
        
        #calculate trading volume average
        stocks_df['trading_volume_avg']=stocks_df.groupby('ticker')['volume'].transform(
            lambda x: x.rolling(window=30, min_periods=1).mean()
        )
        stocks_df['trading_volume_avg']=stocks_df['trading_volume_avg'].replace([np.inf,-np.inf], 0)
        stocks_df['trading_volume_avg']=stocks_df['trading_volume_avg'].fillna(0)
        
        #calculating price range
        stocks_df['price_range']=stocks_df['high']-stocks_df['low']
        stocks_df['price_range']=stocks_df['price_range'].replace([np.inf,-np.inf], 0)
        stocks_df['price_range']=stocks_df['price_range'].fillna(0)
        
        #extract time dimensions
        stocks_df['year']=stocks_df['date'].dt.year
        stocks_df['month']=stocks_df['date'].dt.month
        stocks_df['quarter']=stocks_df['date'].dt.quarter
        
        #creating stock_facts df
        stock_facts=stocks_df[['ticker', 'company', 'date', 'open', 'high', 'low', 'close', 'volume',
                                'daily_return', 'volatility_30d', 'moving_avg_30d', 'moving_avg_90d',
                                'trading_volume_avg', 'price_range', 'year', 'month', 'quarter']].copy()
        
        # numeric columns
        numeric_cols=['open', 'high', 'low', 'close', 'volume', 'daily_return', 'volatility_30d',
                      'moving_avg_30d', 'moving_avg_90d', 'trading_volume_avg', 'price_range']
        for col in numeric_cols:
            stock_facts[col]=stock_facts[col].replace([np.inf, -np.inf], 0)
            stock_facts[col]=stock_facts[col].fillna(0)        
        stock_facts.to_csv(f"{exec_dir}/stock_facts.csv", index=False)
        logger.info(f"stock facts calculated: {len(stock_facts)} rows")
    except Exception as e:
        logger.error(f"failed to calculate stock_facts: {e}")
        return None
    
    #calculate macro_facts
    logger.info("calculating macro_facts metrics...")
    
    try:
        ip_yr=pd.to_datetime(start_date).year if start_date else datetime.now().year
        if macro_df['quarter'].str.contains('-').any():
            #if YYYY-QX
            macro_df['year']=macro_df['quarter'].str[:4].astype(int)
            macro_df['qtr']=macro_df['quarter'].str[-1].astype(int)
        else:
            #if QX - assign current year 
            macro_df['qtr']=macro_df['quarter'].str[-1].astype(int)
            macro_df['year']=ip_yr
        
        #calculate yearly aggr
        macro_yearly=macro_df.groupby('year').agg({
            'GDP': 'mean',
            'inflation': 'mean',
            'unemployment': 'mean',
            'interest_rate': 'mean',
            'consumer_spending': 'mean',
            'industrial_production': 'mean',
            'housing_starts': 'mean',
            'retail_sales': 'mean'
        }).reset_index()
        
        macro_yearly.columns=['year', 'avg_gdp', 'avg_inflation', 'avg_unemployment', 'avg_interest_rate',
                              'avg_consumer_spending', 'avg_industrial_production', 'avg_housing_starts', 'avg_retail_sales']
        
        #calculate gdp growth 
        macro_yearly=macro_yearly.sort_values('year')
        macro_yearly['previous_gdp']=macro_yearly['avg_gdp'].shift(1)
        macro_yearly['gdp_growth']=np.where(
            (macro_yearly['previous_gdp'].notna()) & (macro_yearly['previous_gdp']!=0),
            ((macro_yearly['avg_gdp']-macro_yearly['previous_gdp'])/macro_yearly['previous_gdp'])*100,
            0
        )
        macro_yearly['gdp_growth']=macro_yearly['gdp_growth'].replace([np.inf, -np.inf], 0)
        macro_yearly['gdp_growth']=macro_yearly['gdp_growth'].fillna(0)
        
        #calculating inflation trend
        macro_yearly['previous_inflation']=macro_yearly['avg_inflation'].shift(1)
        macro_yearly['inflation_trend']=macro_yearly['avg_inflation']-macro_yearly['previous_inflation']
        macro_yearly['inflation_trend']=macro_yearly['inflation_trend'].replace([np.inf, -np.inf], 0)
        macro_yearly['inflation_trend']=macro_yearly['inflation_trend'].fillna(0)
        
        #recession years
        macro_yearly['is_recession']=np.where(macro_yearly['gdp_growth']<0, 1, 0)
        macro_facts=macro_df.merge(macro_yearly[['year', 'gdp_growth', 'inflation_trend', 'is_recession']], on='year', how='left')
        
        # numeric columns
        numeric_cols=['GDP', 'inflation', 'unemployment', 'interest_rate', 'consumer_spending',
                      'industrial_production', 'housing_starts', 'retail_sales', 'gdp_growth', 'inflation_trend']
        for col in numeric_cols:
            if col in macro_facts.columns:
                macro_facts[col]=macro_facts[col].replace([np.inf, -np.inf], 0)
                macro_facts[col]=macro_facts[col].fillna(0)
        
        macro_facts.to_csv(f"{exec_dir}/macro_facts.csv", index=False)
        logger.info(f"Macro facts calculated: {len(macro_facts)} rows")
    except Exception as e:
        logger.error(f"failed to calculate macro_facts: {e}")
        return None
    
    #create sector_lookup
    logger.info("Creating sector_lookup...")
    try:
        unique_sectors=company_df['industry'].dropna().unique()
        sector_lookup=pd.DataFrame({
            'sector_id': range(1, len(unique_sectors)+1),
            'sector_name': unique_sectors
        })
        
        #creating sector codes
        sector_lookup['sector_code']=sector_lookup['sector_name'].str[:4].str.upper()       
        sector_lookup.to_csv(f"{exec_dir}/sector_lookup.csv",index=False)
        logger.info(f"Sector lookup created: {len(sector_lookup)} sectors")
    except Exception as e:
        logger.error(f"failed to create sector_lookup: {e}")
        return None
    
    logger.info("Calculating analytics_summary metrics...")
    try:
        company_sector=company_df.merge(sector_lookup,left_on='industry',right_on='sector_name',how='left')
        stock_agg=stock_facts.groupby(['ticker','company','year','quarter']).agg({
            'daily_return':'mean',
            'volatility_30d':'mean',
            'close':'mean',
            'volume':'sum'
        }).reset_index()

        stock_agg.columns=['ticker','company','year','quarter','avg_return','avg_volatility','avg_close','total_volume']
        stock_agg['avg_return']=stock_agg['avg_return'].replace([np.inf,-np.inf],np.nan)
        stock_agg['avg_volatility']=stock_agg['avg_volatility'].replace([np.inf,-np.inf],np.nan)
        stock_agg['avg_close']=stock_agg['avg_close'].replace([np.inf,-np.inf],np.nan)
        logger.info(f"avg_return stats - Mean: {stock_agg['avg_return'].mean():.4f}, Min: {stock_agg['avg_return'].min():.4f}, Max: {stock_agg['avg_return'].max():.4f}, NaN count: {stock_agg['avg_return'].isna().sum()}")

        stock_agg['avg_return']=stock_agg['avg_return'].fillna(0)
        stock_agg['avg_volatility']=stock_agg['avg_volatility'].fillna(0)
        stock_agg['avg_close']=stock_agg['avg_close'].fillna(0)
        analytics=stock_agg.merge(company_sector[['ticker','sector_name','sector_code']],on='ticker',how='left')
        macro_yearly_subset=macro_yearly[['year','avg_gdp','avg_inflation','is_recession']].copy()
        analytics=analytics.merge(macro_yearly_subset,on='year',how='left')

        if 'year' not in exchange_df.columns:
            exchange_df['year']=exchange_df['date'].dt.year
        exchange_yearly=exchange_df.groupby('year')['usd_inr_rate'].mean().reset_index()
        exchange_yearly.columns=['year','avg_exchange_rate']
        exchange_yearly['avg_exchange_rate']=exchange_yearly['avg_exchange_rate'].replace([np.inf,-np.inf],np.nan).fillna(0)

        analytics=analytics.merge(exchange_yearly,on='year',how='left')
        sector_avg=analytics.groupby(['sector_name','year','quarter'])['avg_return'].mean().reset_index()
        sector_avg.columns=['sector_name','year','quarter','sector_avg_return']
        sector_avg['sector_avg_return']=sector_avg['sector_avg_return'].replace([np.inf,-np.inf],np.nan).fillna(0)
        analytics=analytics.merge(sector_avg,on=['sector_name','year','quarter'],how='left')

        logger.info("Calculating correlations for each ticker...")
        correlation_results=[]
        stock_facts_enriched=stock_facts.copy()
        stock_facts_enriched=stock_facts_enriched.merge(macro_yearly[['year','avg_gdp','avg_inflation']],on='year',how='left')
        stock_facts_enriched=stock_facts_enriched.merge(exchange_yearly,on='year',how='left')
        tickers=analytics['ticker'].unique()

        logger.info(f"Computing correlations for {len(tickers)} tickers...")
        for idx,ticker in enumerate(tickers):
            if idx%10==0:
                logger.info(f"Processing ticker {idx+1}/{len(tickers)}: {ticker}")
            ticker_data=stock_facts_enriched[stock_facts_enriched['ticker']==ticker].copy()
            corr_gdp=0
            corr_inflation=0
            corr_currency=0
            if len(ticker_data)>1:
                try:
                    valid_gdp=ticker_data[['daily_return','avg_gdp']].dropna()
                    if len(valid_gdp)>2:
                        corr_gdp=valid_gdp['daily_return'].corr(valid_gdp['avg_gdp'])
                        if np.isnan(corr_gdp) or np.isinf(corr_gdp):
                            corr_gdp=0
                    valid_inflation=ticker_data[['daily_return','avg_inflation']].dropna()
                    if len(valid_inflation)>2:
                        corr_inflation=valid_inflation['daily_return'].corr(valid_inflation['avg_inflation'])
                        if np.isnan(corr_inflation) or np.isinf(corr_inflation):
                            corr_inflation=0
                    valid_currency=ticker_data[['daily_return','avg_exchange_rate']].dropna()
                    if len(valid_currency)>2:
                        corr_currency=valid_currency['daily_return'].corr(valid_currency['avg_exchange_rate'])
                        if np.isnan(corr_currency) or np.isinf(corr_currency):
                            corr_currency=0
                except Exception as e:
                    logger.warning(f"Correlation calculation failed for {ticker}: {e}")
                    corr_gdp=0
                    corr_inflation=0
                    corr_currency=0
            correlation_results.append({'ticker':ticker,'correlation_gdp':corr_gdp,'correlation_inflation':corr_inflation,'currency_impact':corr_currency})

        corr_df=pd.DataFrame(correlation_results)
        corr_df['correlation_gdp']=corr_df['correlation_gdp'].replace([np.inf,-np.inf],0).fillna(0)
        corr_df['correlation_inflation']=corr_df['correlation_inflation'].replace([np.inf,-np.inf],0).fillna(0)
        corr_df['currency_impact']=corr_df['currency_impact'].replace([np.inf,-np.inf],0).fillna(0)
        logger.info(f"Correlation stats - GDP: {corr_df['correlation_gdp'].mean():.4f}, Inflation: {corr_df['correlation_inflation'].mean():.4f}, Currency: {corr_df['currency_impact'].mean():.4f}")

        analytics=analytics.merge(corr_df,on='ticker',how='left')
        analytics['sector_rank']=analytics.groupby(['sector_name','year','quarter'])['avg_return'].rank(ascending=False,method='dense')
        analytics['sector_rank']=analytics['sector_rank'].fillna(0)
        analytics['outperforming_sector']=np.where((analytics['avg_return']>analytics['sector_avg_return'])&(analytics['sector_avg_return'].notna()),1,0)
        recession_performance=stock_facts.merge(macro_yearly[['year','is_recession']],on='year',how='left')
        recession_data=recession_performance[recession_performance['is_recession']==1]

        if len(recession_data)>0:
            recession_scores=recession_data.groupby('ticker')['daily_return'].mean().reset_index()
            recession_scores.columns=['ticker','recession_resilience_score']
            recession_scores['recession_resilience_score']=recession_scores['recession_resilience_score'].replace([np.inf,-np.inf],np.nan).fillna(0)
            analytics=analytics.merge(recession_scores,on='ticker',how='left')
            logger.info(f"Calculated recession resilience for {len(recession_scores)} tickers")
        else:
            logger.warning("No recession periods found in data - setting all recession_resilience_score to 0")
            analytics['recession_resilience_score']=0
        analytics['recession_resilience_score']=analytics['recession_resilience_score'].fillna(0)
        analytics_summary=analytics[['ticker','company','sector_name','sector_code','year','quarter','avg_return','avg_volatility','correlation_gdp','correlation_inflation','currency_impact','sector_rank','sector_avg_return','outperforming_sector','recession_resilience_score']].copy()
        initial_count=len(analytics_summary)
        analytics_summary=analytics_summary.drop_duplicates(subset=['ticker','year','quarter'],keep='last')
        duplicate_count=initial_count-len(analytics_summary)

        if duplicate_count>0:
            logger.info(f"Removed {duplicate_count} duplicate analytics_summary records based on ticker-year-quarter")
        numeric_cols=['avg_return','avg_volatility','correlation_gdp','correlation_inflation','currency_impact','sector_rank','sector_avg_return','recession_resilience_score']
        logger.info("\n"+"="*60)
        logger.info("ANALYTICS SUMMARY FINAL STATISTICS:")

        for col in numeric_cols:
            col_data=analytics_summary[col]
            logger.info(f"{col:30s} - Mean: {col_data.mean():8.4f}, Min: {col_data.min():8.4f}, Max: {col_data.max():8.4f}, Zero%: {(col_data==0).sum()/len(col_data)*100:5.1f}%")
        logger.info("="*60+"\n")
        for col in numeric_cols:
            analytics_summary[col]=analytics_summary[col].replace([np.inf,-np.inf],0)
            analytics_summary[col]=analytics_summary[col].fillna(0)
        analytics_summary.to_csv(f"{exec_dir}/analytics_summary.csv",index=False)
        logger.info(f"Analytics summary calculated: {len(analytics_summary)} rows")
        
    except Exception as e:
        logger.error(f"Failed to calculate analytics_summary: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

      
    logger.info("\n"+"="*60)
    logger.info("KPI calculations completed successfully")
    logger.info(f"Generated files: stock_facts.csv, macro_facts.csv, sector_lookup.csv, analytics_summary.csv, pipeline_logs.csv")
    
    return {
        'stock_facts': stock_facts,
        'macro_facts': macro_facts,
        'sector_lookup': sector_lookup,
        'analytics_summary': analytics_summary
    }

if __name__=="__main__":
    exec_dir="/path/to/Execution/2024/11/24/20241124_120000"
    result=calculate_all_kpis(exec_dir)
    if result:
        logger.info("KPI calculation completed successfully")