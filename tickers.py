"""
Title: Map tickers with company names
Author: Shalini Tata
Created: 22-11-2024
Description: Map tickers with company names with Finnhub and Yahoo Finance 
"""


import re
import time
import requests
from difflib import SequenceMatcher
import yfinance as yf
import pandas as pd
from pathlib import Path

FINN_KEY="d4fc94pr01qkcvvhi5a0d4fc94pr01qkcvvhi5ag"
headers={"User-Agent": "Mozilla/5.0"}

def clean_name(comp):
    comp=re.sub(r'co+rp?o?r?ation', 'corporation', str(comp), flags=re.I)
    clean=re.sub(r'\b(?:inc|incorporated|corp|corporation|co|company|ltd|llc|plc)\b(?:\.)?', '', comp, flags=re.I)
    clean=re.sub(r'[^\w\s]', '', clean)
    clean=re.sub(r'[^\x20-\x7E]', '', clean)
    clean=re.sub(r'\s+', ' ', clean).strip()
    return clean

def similarity(a, b):
    return SequenceMatcher(None, (a or "").lower(), (b or "").lower()).ratio()

def search_finnhub(company):
    base=clean_name(company)
    if not base:
        return None
    
    tokens=base.split()
    queries=[]
    queries.append(" ".join(tokens[:4]))
    if len(tokens)>=3:
        queries.append(" ".join(tokens[:3]))
    if len(tokens)>=2:
        queries.append(" ".join(tokens[:2]))
    if tokens:
        queries.append(tokens[0])
    
    acronym="".join(t[0] for t in tokens if t)
    if len(acronym)>=2:
        queries.append(acronym)
    
    queries=list(dict.fromkeys([q for q in queries if q]))
    
    best={"symbol": None, "score": 0.0}
    url="https://finnhub.io/api/v1/search"
    
    for q in queries:
        try:
            r=requests.get(url, params={"q": q, "token": FINN_KEY}, headers=headers, timeout=8)
        except:
            continue  
        if r.status_code!=200:
            continue
        try:
            data=r.json()
        except:
            continue
        if data.get("count", 0)==0:
            continue
        for item in data.get("result", []):
            for field in ("description", "displaySymbol", "symbol"):
                val=item.get(field)
                if val:
                    score=similarity(base, val)
                    if score>best["score"]:
                        best["symbol"]=item.get("symbol")
                        best["score"]=score
        
        if best["score"]>=0.85:
            return best["symbol"]
        time.sleep(0.08)
    
    return best["symbol"] if best["score"]>=0.6 else None

def search_yfinance(company):
    base=clean_name(company)
    if not base:
        return None
    
    tokens=base.split()
    guesses=[]
    if tokens:
        guesses.append(tokens[0])
    if len(tokens)>=3:
        guesses.append("".join(t[0] for t in tokens[:3]))
    
    for guess in guesses:
        try:
            ticker=yf.Ticker(guess)
            info=ticker.info
            if info:
                for field in ("longName", "shortName"):
                    val=info.get(field)
                    if val and base.lower() in str(val).lower():
                        return guess
        except:
            continue
    return None

def search_yahoo(company):
    base=clean_name(company) or company
    try:
        r=requests.get("https://query2.finance.yahoo.com/v1/finance/search", 
                     params={"q": base}, headers=headers, timeout=8)
    except:
        return None  
    if r.status_code!=200:
        return None  
    try:
        data=r.json()
    except:
        return None   
    quotes=data.get("quotes") or []
    if not quotes:
        return None  
    tokens=[t for t in base.split() if len(t)>=3]
    for q in quotes:
        name=(q.get("shortname") or q.get("longname") or "")
        if any(tok.lower() in str(name).lower() for tok in tokens):
            return q.get("symbol")
    
    return quotes[0].get("symbol")

def get_ticker(company):
    # try finnhub
    ticker=search_finnhub(company)
    if ticker:
        print(f"  Found: {ticker} (finnhub)")
        return ticker
    
    # try yfinance
    ticker=search_yfinance(company)
    if ticker:
        print(f"  Found: {ticker} (yfinance)")
        return ticker
    
    # try yahoo
    ticker=search_yahoo(company)
    if ticker:
        print(f"  Found: {ticker} (yahoo)")
        return ticker
    
    print(f"  Not found")
    return None

def process_csvs(start_year=1996,end_year=2025):
    base=Path("top_companies")
    
    for year in range(start_year, end_year+1):
        file=base / f"fortune_{year}.csv"
        
        if not file.exists():
            continue
        
        print(f"\n{'='*50}")
        print(f"Processing {year}")
        print('='*50)
        
        df=pd.read_csv(file)
        
        if 'company' not in df.columns:
            continue
        
        df=df.head(100).copy()
        
        if 'ticker' not in df.columns:
            df['ticker']=None
        
        for idx, row in df.iterrows():
            company=row['company']
            print(f"\n[{idx+1}/100] {company}")
            
            if pd.notna(row.get('ticker')):
                print(f"  Skip (has ticker: {row['ticker']})")
                continue
            
            ticker=get_ticker(company)
            df.at[idx, 'ticker']=ticker
            time.sleep(0.3)
        
        df.to_csv(file, index=False)
        found=df['ticker'].notna().sum()
        print(f"\nSaved: {found}/100 tickers")

if __name__=="__main__":
    process_csvs(start_year=2024,end_year=2025)