"""
Title: Unified Report Generation Script
Author: Shalini Tata
Created: 25-11-2024
Description: Generate all required reports by querying database and creating PDF/Excel outputs
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
import mysql.connector
from logger import logger
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG={
    'host': os.getenv("DB_HOST", "localhost"),
    'port': int(os.getenv("DB_PORT",3306)),
    'user': os.getenv("DB_USER","root"),
    'password': os.getenv("DB_PASSWORD","shalu 025"),
    'database': 'stocks_new'
}

def get_db_connection():
    """connect to db"""
    try:
        connection=mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def ensure_dir(path):
    """ if dir doesnt exists its ctreated"""
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
        logger.info(f"Created directory: {path}")

def header(c,report_name,created_for,start_date,end_date,page_width=210*mm):
    # center logo
    logo_path="mhk_logo.png"
    try:
        logo_w,logo_h=30*mm,30*mm
        c.drawImage(
            logo_path,
            (page_width-logo_w)/2,
            265*mm,
            width=logo_w,
            height=logo_h,
            preserveAspectRatio=True,
            mask='auto'
        )
    except:
        pass

    # title under logo
    c.setFont("Helvetica-Bold",16)
    text_width=c.stringWidth(report_name,"Helvetica-Bold",16)
    c.drawString((page_width-text_width)/2,255*mm,report_name)

    # metadata
    c.setFont("Helvetica",9)
    y=248*mm
    c.drawString(15*mm,y,f"Report Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    y-=5*mm
    c.drawString(15*mm,y,f"Created For: {start_date} - {end_date}")
    y-=5*mm
    c.drawString(15*mm,y,"Created By: Shalini Tata")
    y-=5*mm
    c.drawString(15*mm,y,"Owner: MHK Software Pvt Ltd")

    # underline
    c.line(15*mm,y-3*mm,195*mm,y-3*mm)
    return y-8*mm



def aggr_rows(data,headers,numeric_cols=None):
    if not data or not headers:
        return []
    table=[headers]
    table.extend(data)
    if numeric_cols and len(data)>0:
        # sum row
        sum_row=["SUM"]
        for col in range(1,len(headers)):
            if col in numeric_cols:
                try:
                    vals=[float(str(r[col]).replace(",","")) for r in data if r[col] not in (None,"","N/A")]
                    sum_row.append(f"{sum(vals):,.2f}" if vals else "0")
                except:
                    sum_row.append("0")
            else:
                sum_row.append("")
        table.append(sum_row)
        # average row
        avg_row=["AVERAGE"]
        for col in range(1,len(headers)):
            if col in numeric_cols:
                try:
                    vals=[float(str(r[col]).replace(",","")) for r in data if r[col] not in (None,"","N/A")]
                    avg_row.append(f"{(sum(vals)/len(vals)):.2f}" if vals else "0")
                except:
                    avg_row.append("0")
            else:
                avg_row.append("")
        table.append(avg_row)
        #record count
        count_row=[f"Records:{len(data)}"]
        table.append(count_row)
    return table

def report_eof(c,y,page_width=210*mm):
    c.setFont("Helvetica-Bold",12)
    msg="***** END OF REPORT *****"
    w=c.stringWidth(msg,"Helvetica-Bold",12)
    c.drawString((page_width-w)/2,y,msg)


def draw_table(c, table_data, x=15*mm, y=240*mm, col_widths=None):
    if not table_data:
        return y
    
    if col_widths is None:
        available_width=180*mm
        col_widths=[available_width/len(table_data[0])]*len(table_data[0])
    
    table=Table(table_data, colWidths=col_widths)
    
    style=TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('BACKGROUND', (0, 1), (-1, -2), colors.beige),
        ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
        ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.lightgrey]),
        ('BACKGROUND', (0, -1), (-1, -1), colors.lightblue),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, -1), (-1, -1), 9),
    ])
    
    table.setStyle(style)
    table.wrapOn(c, 180*mm, 250*mm)
    table_height=table._height
    table.drawOn(c, x, y-table_height)
    
    return y-table_height-10*mm

def market_analys(exec_dir, reports_dir,start_date, end_date):
    """Annual Market Intelligence Report - Yearly sector and company performance"""
    logger.info("\n"+"="*60)
    logger.info("Generating Report 1: Annual Market Intelligence Report")
    
    connection=get_db_connection()
    if not connection:
        logger.error("Cannot generate report without database connection")
        return None
    
    cursor=None
    try:
        cursor=connection.cursor()
        
        cursor.execute("SELECT MIN(year), MAX(year) FROM stock_facts")
        year_range=cursor.fetchone()
        created_for=f"Years {year_range[0]}-{year_range[1]}" if year_range[0] else "All Years"
        
        query_yearly_performance="""
        SELECT 
            sf.year,
            AVG(sf.daily_return) as avg_daily_return,
            AVG(sf.volatility_30d) as avg_volatility,
            SUM(sf.volume) as total_volume,
            COUNT(DISTINCT sf.ticker) as company_count,
            AVG(mf.GDP) as avg_gdp,
            AVG(mf.inflation) as avg_inflation
        FROM stock_facts sf
        LEFT JOIN macro_facts mf ON sf.year=mf.year
        WHERE sf.date BETWEEN %s AND %s
        GROUP BY sf.year
        ORDER BY sf.year
        """
        cursor.execute(query_yearly_performance, (start_date, end_date))
        yearly_data=cursor.fetchall()
        logger.info(f"Fetched {len(yearly_data)} rows for yearly performance")
        
        query_company_performance="""
        SELECT 
            sf.ticker,
            sf.company,
            cm.industry,
            AVG(sf.daily_return) as avg_return,
            AVG(sf.volatility_30d) as avg_volatility
        FROM stock_facts sf
        LEFT JOIN company_metadata cm ON sf.ticker=cm.ticker
        WHERE sf.date BETWEEN %s AND %s
        GROUP BY sf.ticker, sf.company, cm.industry
        ORDER BY avg_return DESC
        LIMIT 20
        """
        cursor.execute(query_company_performance, (start_date, end_date))
        company_data=cursor.fetchall()
        logger.info(f"Fetched {len(company_data)} rows for company performance")
        
        df_yearly=pd.DataFrame(yearly_data,columns=['Year', 'AvgDailyReturn', 'AvgVolatility', 'TotalVolume', 'CompanyCount', 'AvgGDP', 'AvgInflation'])
        df_company=pd.DataFrame(company_data,columns=['Ticker', 'Company', 'Industry', 'AvgReturn', 'AvgVolatility'])
        
        csv1_path=os.path.join(exec_dir,"r1_yearly_performance.csv")
        df_yearly.to_csv(csv1_path,index=False)
        logger.info(f"Saved intermediate CSV: {csv1_path}")
        
        csv2_path=os.path.join(exec_dir,"r1_company_performance.csv")
        df_company.to_csv(csv2_path, index=False)
        logger.info(f"Saved intermediate CSV: {csv2_path}")
        ensure_dir(reports_dir)
        pdf_path=os.path.join(reports_dir, f"report1_annual_market_intelligence.pdf")
        c=canvas.Canvas(pdf_path, pagesize=A4)
        y=header(c,"Annual Market Intelligence Report",created_for,start_date,end_date)
        
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm,y,"Yearly Performance Summary")
        y-=8*mm
        
        headers=["Year","Avg Return(%)","Avg Volatility","Total Volume","Companies","GDP","Inflation"]
        data=[]
        for row in yearly_data:
            data.append([
                str(row[0]) if row[0] else 'N/A',
                f"{row[1]:.4f}" if row[1] is not None else 'N/A',
                f"{row[2]:.2f}" if row[2] is not None else 'N/A',
                f"{row[3]:,.0f}" if row[3] else 'N/A',
                str(row[4]) if row[4] else 'N/A',
                f"{row[5]:.2f}" if row[5] is not None else 'N/A',
                f"{row[6]:.2f}" if row[6] is not None else 'N/A'
            ])
        
        table_data=aggr_rows(data, headers, numeric_cols=[1,2,3,4,5,6])
        y=draw_table(c,table_data,y=y,col_widths=[20*mm,25*mm,28*mm,32*mm,25*mm,20*mm,25*mm])
        
        if y<80*mm:
            c.showPage()
            y=270*mm
        
        y-=5*mm
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm,y,"Top 20 Companies by Average Return")
        y-=8*mm
        
        # Remove "Days" from headers and remove row[5] from data
        headers=["Ticker","Company","Industry","Avg Return(%)","Volatility"]
        data=[]
        for row in company_data:
            data.append([
                str(row[0]) if row[0] else 'N/A',
                str(row[1])[:20] if row[1] else 'N/A',
                str(row[2])[:15] if row[2] else 'N/A',
                f"{row[3]:.4f}" if row[3] is not None else 'N/A',
                f"{row[4]:.2f}" if row[4] is not None else 'N/A'
            ])

        table_data=aggr_rows(data,headers,numeric_cols=[4,5])
        y=draw_table(c,table_data,y=y,col_widths=[20*mm,40*mm,35*mm,30*mm,25*mm]) 
        if y>20*mm:
            y-=10*mm
            report_eof(c, y)    
        c.save()
        logger.info(f"saved PDF: {pdf_path}")
        return {'csv': [csv1_path,csv2_path],'pdf': pdf_path}    
    except Exception as e:
        logger.error(f"failed to generate annual market intelligence report: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def sector_stability(exec_dir, reports_dir,start_date, end_date):
    """sector stability & volatility report - sector-wise risk and resilience analysis"""
    logger.info("\n"+"="*60)
    logger.info("Generating Report 2: Sector Stability & Volatility Report")
    connection=get_db_connection()
    if not connection:
        logger.error("cannot generate report without database connection")
        return None
    
    cursor=None
    try:
        cursor=connection.cursor() 
       #years from start_date and end_date
        start_year=datetime.strptime(start_date, "%Y-%m-%d").year if start_date else None
        end_year=datetime.strptime(end_date, "%Y-%m-%d").year if end_date else None

        query_sector_metrics="""
        SELECT 
            ans.sector_name,
            sl.sector_code,
            AVG(ans.avg_volatility) as avg_volatility,
            AVG(ans.avg_return) as avg_return,
            AVG(ans.recession_resilience_score) as resilience,
            AVG(ans.sector_rank) as avg_rank
        FROM analytics_summary ans
        LEFT JOIN sector_lookup sl ON ans.sector_name=sl.sector_name
        WHERE ans.sector_name IS NOT NULL AND ans.year BETWEEN %s AND %s
        GROUP BY ans.sector_name, sl.sector_code
        ORDER BY avg_volatility DESC
        """
        cursor.execute(query_sector_metrics, (start_year, end_year))
        sector_data=cursor.fetchall()
        logger.info(f"fetched {len(sector_data)} rows for sector metrics")
        #make df nd intermediate csv
        df_sector=pd.DataFrame(sector_data,columns=['SectorName','SectorCode','AvgVolatility','AvgReturn','Resilience','AvgRank'])
        csv_path=os.path.join(exec_dir,"r2_sector_stability.csv")
        df_sector.to_csv(csv_path, index=False)
        logger.info(f"saved intermediate CSV: {csv_path}")
        ensure_dir(reports_dir)
        
        excel_path=os.path.join(reports_dir, f"r2_sector_stability.xlsx")
        try:
            df_sector.to_excel(excel_path, index=False, engine='openpyxl')
            logger.info(f"saved Excel: {excel_path}")
        except Exception as e:
            logger.warning(f"Excel save failed: {e}")
            excel_path=None
        
        cursor.execute("SELECT COUNT(DISTINCT sector_name) FROM analytics_summary WHERE sector_name IS NOT NULL")
        sector_count=cursor.fetchone()[0]
        created_for=f"All Sectors ({sector_count} total)"
        
        pdf_path=os.path.join(reports_dir, f"report2_sector_stability.pdf")
        c=canvas.Canvas(pdf_path, pagesize=A4)
        y=header(c,"Sector Stability & Volatility Report", created_for,start_date,end_date)
        
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm, y, "Sector Performance and Risk Metrics")
        y-=8*mm
        
        headers=["Sector", "Code", "Volatility", "Return(%)", "Resilience", "Rank"]
        data=[]
        for row in sector_data[:25]:
            data.append([
                str(row[0])[:20] if row[0] else 'N/A',
                str(row[1]) if row[1] else 'N/A',
                f"{row[2]:.2f}" if row[2] is not None else 'N/A',
                f"{row[3]:.4f}" if row[3] is not None else 'N/A',
                f"{row[4]:.4f}" if row[4] is not None else 'N/A',
                f"{row[5]:.1f}" if row[5] is not None else 'N/A'
            ])
        
        table_data=aggr_rows(data, headers, numeric_cols=[2, 3, 4, 5])
        y=draw_table(c, table_data, y=y, col_widths=[40*mm, 20*mm, 25*mm, 25*mm, 25*mm, 20*mm])
        
        if y>20*mm:
            y-=10*mm
            report_eof(c, y)
        
        c.save()
        logger.info(f"Saved PDF: {pdf_path}")
        return {'csv': csv_path, 'excel': excel_path, 'pdf': pdf_path}
        
    except Exception as e:
        logger.error(f"Failed to generate sector stability report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def cross_market(exec_dir, reports_dir,start_date, end_date):
    """cross-market analysis report - currency impact on stock performance"""
    logger.info("\n"+"="*60)
    logger.info("Generating Report 3: Cross-Market Analysis Report")
    connection=get_db_connection()
    if not connection:
        logger.error("Cannot generate report without database connection")
        return None
    
    cursor=None
    try:
        cursor=connection.cursor()
        query_currency_impact="""
        SELECT 
            sf.year,
            AVG(sf.daily_return) as avg_return,
            AVG(er.usd_inr_rate) as avg_usd_inr,
            AVG(sf.volatility_30d) as avg_volatility,
            COUNT(DISTINCT sf.ticker) as company_count
        FROM stock_facts sf
        LEFT JOIN exchange_rates er ON sf.date=er.date
        WHERE sf.date BETWEEN %s AND %s
        GROUP BY sf.year
        ORDER BY sf.year
        """
        cursor.execute(query_currency_impact,(start_date,end_date))
        yearly_currency=cursor.fetchall()
        logger.info(f"Fetched {len(yearly_currency)} rows for yearly currency data")
        
        start_year=datetime.strptime(start_date,"%Y-%m-%d").year if start_date else None
        end_year=datetime.strptime(end_date,"%Y-%m-%d").year if end_date else None

        query_company_currency="""
        SELECT 
            ans.ticker,
            ans.company,
            ans.sector_name,
            AVG(ans.currency_impact) as avg_currency_correlation,
            AVG(ans.avg_return) as avg_return
        FROM analytics_summary ans
        WHERE ans.year BETWEEN %s AND %s
        GROUP BY ans.ticker, ans.company, ans.sector_name
        ORDER BY avg_currency_correlation DESC
        LIMIT 20
        """
        cursor.execute(query_company_currency, (start_year, end_year))
        company_currency=cursor.fetchall()
        logger.info(f"Fetched {len(company_currency)} rows for company currency data")
        
        df_yearly=pd.DataFrame(yearly_currency, columns=['Year', 'AvgReturn', 'AvgUSD_INR', 'AvgVolatility', 'CompanyCount'])
        df_company=pd.DataFrame(company_currency, columns=['Ticker', 'Company', 'Sector', 'CurrencyCorrelation', 'AvgReturn'])
        
        returns=[row[1] for row in yearly_currency if row[1] is not None]
        exchange=[row[2] for row in yearly_currency if row[2] is not None]
        if len(returns)>1 and len(exchange)>1 and len(returns)==len(exchange):
            correlation=np.corrcoef(returns, exchange)[0, 1]
        else:
            correlation=0
        
        df_yearly['Correlation']=correlation
        csv1_path=os.path.join(exec_dir,"r3_currency_yearly.csv")
        df_yearly.to_csv(csv1_path, index=False)
        logger.info(f"saved intermediate CSV: {csv1_path}")
        
        csv2_path=os.path.join(exec_dir,"r3_company_currency.csv")
        df_company.to_csv(csv2_path, index=False)
        logger.info(f"saved intermediate CSV: {csv2_path}")
        
        ensure_dir(reports_dir)
        
        cursor.execute("SELECT MIN(year), MAX(year) FROM stock_facts")
        year_range=cursor.fetchone()
        created_for=f"Years {year_range[0]}-{year_range[1]}" if year_range[0] else "All Years"
        
        pdf_path=os.path.join(reports_dir,f"report3_cross_market.pdf")
        c=canvas.Canvas(pdf_path, pagesize=A4)
        y=header(c,"Cross-Market Analysis Report",created_for,start_date,end_date)
        
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm,y,"Currency vs Stock Return Analysis")
        y-=8*mm
        
        headers=["Year", "Avg Return(%)", "Avg USD/INR", "Avg Volatility", "Companies"]
        data=[]
        for row in yearly_currency:
            data.append([
                str(row[0]) if row[0] else 'N/A',
                f"{row[1]:.4f}" if row[1] is not None else 'N/A',
                f"{row[2]:.4f}" if row[2] is not None else 'N/A',
                f"{row[3]:.2f}" if row[3] is not None else 'N/A',
                str(row[4]) if row[4] else 'N/A'
            ])
        
        table_data=aggr_rows(data, headers, numeric_cols=[1, 2, 3, 4])
        y=draw_table(c, table_data, y=y, col_widths=[25*mm, 40*mm, 40*mm, 40*mm, 35*mm])
        
        y-=10*mm
        c.setFont("Helvetica-Bold", 10)
        if y<100*mm:
            c.showPage()
            y=270*mm
        
        y-=10*mm
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm, y, "Top 20 Companies by Currency Correlation")
        y-=8*mm
        
        headers=["Ticker", "Company", "Sector", "Currency Corr", "Avg Return(%)"]
        data=[]
        for row in company_currency:
            data.append([
                str(row[0]) if row[0] else 'N/A',
                str(row[1])[:20] if row[1] else 'N/A',
                str(row[2])[:15] if row[2] else 'N/A',
                f"{row[3]:.4f}" if row[3] is not None else 'N/A',
                f"{row[4]:.4f}" if row[4] is not None else 'N/A'
            ])
        
        table_data=aggr_rows(data, headers, numeric_cols=[3, 4])
        y=draw_table(c, table_data, y=y, col_widths=[20*mm, 40*mm, 35*mm, 35*mm, 35*mm])
        
        if y>20*mm:
            y-=10*mm
            report_eof(c, y) 
        c.save()
        logger.info(f"saved PDF: {pdf_path}")
        return {'csv': [csv1_path, csv2_path], 'pdf': pdf_path}
        
    except Exception as e:
        logger.error(f"failed to generate cross-market analysis report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def prediction_sum(exec_dir, reports_dir, start_date, end_date):
    """Forecasting & Prediction Summary - Real predictions with confidence intervals"""
    logger.info("\n"+"="*60)
    logger.info("Generating Report 4: Forecasting & Prediction Summary")
    
    connection=get_db_connection()
    if not connection:
        logger.error("Cannot generate report without database connection")
        return None
    
    cursor=None
    try:
        cursor=connection.cursor()
        start_year=datetime.strptime(start_date, "%Y-%m-%d").year if start_date else None
        end_year=datetime.strptime(end_date, "%Y-%m-%d").year if end_date else None

        # Fetch historical quarterly data for forecasting
        query_historical="""
        SELECT 
            ans.year,
            ans.quarter,
            AVG(ans.avg_return) as avg_return,
            AVG(ans.avg_volatility) as avg_volatility,
            AVG(mf.GDP) as gdp,
            AVG(mf.inflation) as inflation,
            COUNT(DISTINCT ans.ticker) as company_count
        FROM analytics_summary ans
        LEFT JOIN macro_facts mf ON CONCAT('Q', ans.quarter)=mf.quarter AND ans.year=mf.year
        WHERE ans.year BETWEEN %s AND %s
        GROUP BY ans.year, ans.quarter
        ORDER BY ans.year ASC, ans.quarter ASC
        """
        cursor.execute(query_historical, (start_year, end_year))
        historical_data=cursor.fetchall()
        logger.info(f"Fetched {len(historical_data)} rows for historical quarterly data")
        
        if len(historical_data)<4:
            logger.warning("Insufficient data for forecasting (<4 quarters)")
            return None

        # Fetch sector-wise data for forecasting
        query_sector="""
        SELECT 
            ans.sector_name,
            ans.year,
            ans.quarter,
            AVG(ans.avg_return) as avg_return,
            AVG(ans.avg_volatility) as avg_volatility,
            AVG(ans.sector_avg_return) as sector_benchmark
        FROM analytics_summary ans
        WHERE ans.year BETWEEN %s AND %s AND ans.sector_name IS NOT NULL
        GROUP BY ans.sector_name, ans.year, ans.quarter
        ORDER BY ans.sector_name, ans.year, ans.quarter
        """
        cursor.execute(query_sector, (start_year, end_year))
        sector_data=cursor.fetchall()
        logger.info(f"Fetched {len(sector_data)} rows for sector data")
        
        # Convert to DataFrames
        df_historical=pd.DataFrame(historical_data, 
                                    columns=['year', 'quarter', 'avg_return', 'avg_volatility', 
                                            'gdp', 'inflation', 'company_count'])
        
        df_sector=pd.DataFrame(sector_data,
                              columns=['sector_name', 'year', 'quarter', 'avg_return', 
                                      'avg_volatility', 'sector_benchmark'])
        
        # Create time series index
        df_historical['period']=df_historical['year'].astype(str)+'-Q'+df_historical['quarter'].astype(str)
        df_historical=df_historical.sort_values(['year', 'quarter'])
        
        # SIMPLE MOVING AVERAGE FORECASTING (3-period)
        logger.info("Calculating forecasts using moving average method...")
        
        # Forecast next 4 quarters for market returns
        market_returns=df_historical['avg_return'].values
        market_volatility=df_historical['avg_volatility'].values
        
        # Simple 3-period moving average
        forecast_periods=4
        forecasted_returns=[]
        forecasted_volatility=[]
        confidence_intervals=[]
        
        for i in range(forecast_periods):
            if len(market_returns)>=3:
                # Take last 3 periods for moving average
                ma_return=np.mean(market_returns[-3:])
                ma_volatility=np.mean(market_volatility[-3:])
                
                # Calculate standard deviation for confidence interval
                std_return=np.std(market_returns[-6:]) if len(market_returns)>=6 else np.std(market_returns)
                
                # 95% confidence interval (±1.96 * std)
                ci_lower=ma_return-(1.96*std_return)
                ci_upper=ma_return+(1.96*std_return)
                
                forecasted_returns.append(ma_return)
                forecasted_volatility.append(ma_volatility)
                confidence_intervals.append((ci_lower, ci_upper))
                
                # Add forecast to historical for next iteration
                market_returns=np.append(market_returns, ma_return)
                market_volatility=np.append(market_volatility, ma_volatility)
            else:
                break
        
        # Calculate trend direction
        if len(market_returns)>=2:
            recent_trend='Upward' if market_returns[-1]>market_returns[-2] else 'Downward'
        else:
            recent_trend='Stable'
        
        # Generate forecast periods (next 4 quarters)
        last_year=df_historical['year'].max()
        last_quarter=df_historical[df_historical['year']==last_year]['quarter'].max()
        
        forecast_periods_list=[]
        for i in range(1, forecast_periods+1):
            next_quarter=last_quarter+i
            next_year=last_year
            if next_quarter>4:
                next_year+=next_quarter//4
                next_quarter=next_quarter%4
                if next_quarter==0:
                    next_quarter=4
                    next_year-=1
            forecast_periods_list.append((next_year, next_quarter))
        
        # Create forecast dataframe
        forecast_data=[]
        for idx, (f_year, f_quarter) in enumerate(forecast_periods_list):
            if idx<len(forecasted_returns):
                forecast_data.append({
                    'year': f_year,
                    'quarter': f_quarter,
                    'forecasted_return': forecasted_returns[idx],
                    'forecasted_volatility': forecasted_volatility[idx],
                    'ci_lower': confidence_intervals[idx][0],
                    'ci_upper': confidence_intervals[idx][1],
                    'confidence': '95%',
                    'trend': recent_trend
                })
        
        df_forecast=pd.DataFrame(forecast_data)
        
        # Sector-wise forecasting (top 10 sectors)
        sector_forecasts=[]
        top_sectors=df_sector.groupby('sector_name')['avg_return'].mean().sort_values(ascending=False).head(10).index
        
        for sector in top_sectors:
            sector_df=df_sector[df_sector['sector_name']==sector].sort_values(['year', 'quarter'])
            if len(sector_df)>=3:
                sector_returns=sector_df['avg_return'].values
                
                # 3-period moving average
                ma_sector=np.mean(sector_returns[-3:])
                std_sector=np.std(sector_returns) if len(sector_returns)>1 else 0
                
                # Next quarter forecast
                next_year, next_quarter=forecast_periods_list[0]
                
                sector_forecasts.append({
                    'sector': sector,
                    'next_year': next_year,
                    'next_quarter': next_quarter,
                    'predicted_return': ma_sector,
                    'volatility': sector_df['avg_volatility'].mean(),
                    'ci_lower': ma_sector-(1.96*std_sector),
                    'ci_upper': ma_sector+(1.96*std_sector)
                })
        
        df_sector_forecast=pd.DataFrame(sector_forecasts)
        
        # Save intermediate CSVs
        csv1_path=os.path.join(exec_dir, "r4_market_forecast.csv")
        df_forecast.to_csv(csv1_path, index=False)
        logger.info(f"Saved market forecast CSV: {csv1_path}")
        
        csv2_path=os.path.join(exec_dir, "r4_sector_forecast.csv")
        df_sector_forecast.to_csv(csv2_path, index=False)
        logger.info(f"Saved sector forecast CSV: {csv2_path}")
        
        csv3_path=os.path.join(exec_dir, "r4_historical_data.csv")
        df_historical.to_csv(csv3_path, index=False)
        logger.info(f"Saved historical data CSV: {csv3_path}")
        
        # Generate PDF report
        ensure_dir(reports_dir)
        
        cursor.execute("SELECT MIN(year), MAX(year) FROM analytics_summary")
        year_range=cursor.fetchone()
        created_for=f"Historical: {year_range[0]}-{year_range[1]}, Forecast: Next 4 Quarters"
        
        pdf_path=os.path.join(reports_dir, "report4_forecasting.pdf")
        c=canvas.Canvas(pdf_path, pagesize=A4)
        y=header(c, "Forecasting & Prediction Summary", created_for, start_date, end_date)
        
        # Section 1: Market Forecast
        c.setFont("Helvetica-Bold", 12)
        c.drawString(15*mm, y, f"Market Return Forecast (Next 4 Quarters) - Trend: {recent_trend}")
        y-=8*mm
        
        c.setFont("Helvetica", 9)
        c.drawString(15*mm, y, "Method: 3-Period Moving Average with 95% Confidence Intervals")
        y-=8*mm
        
        headers=["Period", "Year", "Quarter", "Predicted Return(%)", "CI Lower(%)", "CI Upper(%)", "Volatility"]
        data=[]
        for idx, row in df_forecast.iterrows():
            data.append([
                f"Q{idx+1}",
                str(int(row['year'])),
                f"Q{int(row['quarter'])}",
                f"{row['forecasted_return']:.4f}",
                f"{row['ci_lower']:.4f}",
                f"{row['ci_upper']:.4f}",
                f"{row['forecasted_volatility']:.2f}"
            ])
        
        table_data=[headers]+data
        y=draw_table(c, table_data, y=y, col_widths=[20*mm, 20*mm, 20*mm, 35*mm, 30*mm, 30*mm, 25*mm])
        
        if y<120*mm:
            c.showPage()
            y=270*mm
        
        # Section 2: Historical vs Forecast Comparison
        y-=10*mm
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm, y, "Recent Historical Performance (Last 8 Quarters)")
        y-=8*mm
        
        headers=["Period", "Year", "Q", "Actual Return(%)", "Volatility", "GDP", "Inflation", "Companies"]
        data=[]
        for idx, row in df_historical.tail(8).iterrows():
            data.append([
                row['period'],
                str(int(row['year'])),
                f"Q{int(row['quarter'])}",
                f"{row['avg_return']:.4f}",
                f"{row['avg_volatility']:.2f}",
                f"{row['gdp']:.2f}" if pd.notna(row['gdp']) else 'N/A',
                f"{row['inflation']:.2f}" if pd.notna(row['inflation']) else 'N/A',
                str(int(row['company_count']))
            ])
        
        table_data=aggr_rows(data, headers, numeric_cols=[3, 4, 5, 6, 7])
        y=draw_table(c, table_data, y=y, col_widths=[25*mm, 18*mm, 15*mm, 32*mm, 25*mm, 20*mm, 25*mm, 25*mm])
        
        if y<100*mm:
            c.showPage()
            y=270*mm
        
        # Section 3: Top Sector Forecasts
        y-=10*mm
        c.setFont("Helvetica-Bold", 11)
        c.drawString(15*mm, y, f"Top 10 Sector Forecasts (Q{forecast_periods_list[0][1]} {forecast_periods_list[0][0]})")
        y-=8*mm
        
        headers=["Sector", "Predicted Return(%)", "Volatility", "CI Lower(%)", "CI Upper(%)", "Confidence"]
        data=[]
        for idx, row in df_sector_forecast.iterrows():
            data.append([
                str(row['sector'])[:30],
                f"{row['predicted_return']:.4f}",
                f"{row['volatility']:.2f}",
                f"{row['ci_lower']:.4f}",
                f"{row['ci_upper']:.4f}",
                "95%"
            ])
        
        table_data=aggr_rows(data, headers, numeric_cols=[1, 2, 3, 4])
        y=draw_table(c, table_data, y=y, col_widths=[50*mm, 35*mm, 25*mm, 30*mm, 30*mm, 20*mm])
        
        # Forecast summary box
        if y<80*mm:
            c.showPage()
            y=270*mm
        
        y-=15*mm
        c.setFont("Helvetica-Bold", 10)
        c.drawString(15*mm, y, "Forecast Summary:")
        y-=6*mm
        c.setFont("Helvetica", 9)
        
        avg_forecast=df_forecast['forecasted_return'].mean()
        avg_ci_range=df_forecast['ci_upper'].mean()-df_forecast['ci_lower'].mean()
        
        c.drawString(20*mm, y, f"• Average Predicted Return (Next 4Q): {avg_forecast:.4f}%")
        y-=5*mm
        c.drawString(20*mm, y, f"• Average Confidence Interval Range: ±{avg_ci_range/2:.4f}%")
        y-=5*mm
        c.drawString(20*mm, y, f"• Market Trend Direction: {recent_trend}")
        y-=5*mm
        c.drawString(20*mm, y, f"• Forecast Method: 3-Period Moving Average")
        y-=5*mm
        c.drawString(20*mm, y, f"• Historical Data Points Used: {len(df_historical)} quarters")
        
        if y>20*mm:
            y-=10*mm
            report_eof(c, y)
        
        c.save()
        logger.info(f"Saved PDF: {pdf_path}")
        
        return {
            'csv': [csv1_path, csv2_path, csv3_path], 
            'pdf': pdf_path,
            'forecast_summary': {
                'avg_predicted_return': float(avg_forecast),
                'trend': recent_trend,
                'quarters_forecasted': len(forecast_data)
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to generate forecasting summary: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def generate_all_reports(exec_dir, reports_dir, start_date=None, end_date=None):
    """Main function to generate all 4 required reports"""
    logger.info("\n"+"="*80)
    logger.info("REPORT GENERATION STARTED")
    
    ensure_dir(exec_dir)
    ensure_dir(reports_dir)
    
    results={}
    
    try:
        r1=market_analys(exec_dir,reports_dir,start_date,end_date)
        results['annual_market_intelligence']=r1
        if r1:
            logger.info(f"Report 1 completed: {r1}")
        r2=sector_stability(exec_dir,reports_dir,start_date,end_date)
        results['sector_stability_volatility']=r2
        if r2:
            logger.info(f"Report 2 completed: {r2}")        
        r3=cross_market(exec_dir,reports_dir,start_date,end_date)
        results['cross_market_analysis']=r3
        if r3:
            logger.info(f"Report 3 completed: {r3}")        
        r4=prediction_sum(exec_dir,reports_dir,start_date,end_date)
        results['forecasting_prediction']=r4
        if r4:
            logger.info(f"Report 4 completed: {r4}")     
        logger.info("\n"+"="*80)
        logger.info("REPORT GENERATION COMPLETED SUCCESSFULLY")
        logger.info("\n"+"="*80)
        
    except Exception as e:
        logger.error(f"Report generation error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return results

if __name__=="__main__":
    exec_dir="./Execution/test"
    reports_dir="./reports_output"
    ensure_dir(exec_dir)
    ensure_dir(reports_dir)
    results=generate_all_reports(exec_dir, reports_dir)
    logger.info(f"Completed. Results: {results}")