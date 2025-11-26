"""
Title: Data Quality Rules for Financial Data
Author: Shalini Tata
Created: 23-11-2024
Purpose: Validate data quality and separate valid/invalid rows
"""

import pandas as pd
import json
import re
from datetime import datetime
from pathlib import Path
from logger import logger

#load config
with open("config.json") as f:
    config=json.load(f)

tables_config=config.get("tables",{})

def validate_table(df,table_name,refs=None):
    """validate dataframe over rules for given table"""
    if table_name not in tables_config:
        raise ValueError(f"no rules found for table: {table_name}")
    
    rules=tables_config[table_name]
    valid_data=[]
    invalid_data=[]
    unique_trackers={}
    
    #unique column trackers
    for col,conf in rules.items():
        if "unique" in conf.get("rules",[]):
            unique_trackers[col]=set()
    
    #referential lookup sets
    ref_lookup={}
    if refs:
        for col,conf in rules.items():
            for rule in conf.get("rules",[]):
                if rule.startswith("referential:"):
                    ref_table,ref_col=rule.split(":",1)[1].split(".")
                    if ref_table in refs:
                        ref_lookup[col]=set(refs[ref_table][ref_col].astype(str).str.strip().str.upper())
    
    #process each row
    for idx,(_,row) in enumerate(df.iterrows()):
        valid=True
        row_copy=row.copy()
        
        #check each column
        for col,conf in rules.items():
            val=row.get(col)
            col_type=conf.get("type")
            col_rules=conf.get("rules",[])
            
            #apply uppercase_trim if needed
            if "uppercase_trim" in col_rules and pd.notna(val):
                row_copy[col]=str(val).strip().upper()
                val=row_copy[col]
            
            #type conversions
            if pd.notna(val):
                try:
                    if col_type=="string":
                        row_copy[col]=str(val)
                        val=row_copy[col]
                    elif col_type=="float":
                        row_copy[col]=float(val)
                        val=row_copy[col]
                    elif col_type=="integer":
                        #numeric type
                        if isinstance(val, (int,float)):
                            if isinstance(val,float) and not float(val).is_integer():
                                valid=False
                                logger.error(f"Row {idx} col {col}: not an integer ({val})")
                                break
                            row_copy[col]=int(val)
                            val=row_copy[col]
                            continue
                        #string type
                        if isinstance(val,str):
                            s=val.strip()
                            #check using regex
                            if re.fullmatch(r"-?\d+", s):
                                row_copy[col]=int(s)
                                val=row_copy[col]
                                continue
                            else:
                                valid=False
                                logger.error(f"Row {idx} col {col}: string not integer ({val})")
                                break
                        #anything else
                        valid=False
                        logger.error(f"Row {idx} col {col}: invalid type {type(val)} for integer")
                        break
                    elif col_type=="date":
                        row_copy[col]=pd.to_datetime(val)
                        val=row_copy[col]
                except Exception:
                    valid=False
                    logger.error(f"Row {idx} col {col}: type conversion failed for {val}")
                    continue
            
            #apply rules
            for rule in col_rules:
                if rule=="not_null":
                    if pd.isna(val):
                        valid=False
                        logger.error(f"Row {idx} col {col}: null value")
                        break               
                elif rule=="nullable":
                    continue            
                elif rule=="positive":
                    if pd.notna(val) and float(val)<=0:
                        valid=False
                        logger.error(f"Row {idx} col {col}: not positive ({val})")
                        break                
                elif rule=="non_negative":
                    if pd.notna(val) and float(val)<0:
                        valid=False
                        logger.error(f"Row {idx} col {col}: negative value ({val})")
                        break                
                elif rule=="unique":
                    if pd.isna(val):
                        valid=False
                        logger.error(f"Row {idx} col {col}: null in unique column")
                        break
                    if val in unique_trackers[col]:
                        valid=False
                        logger.error(f"Row {idx} col {col}: duplicate value {val}")
                        break
                    unique_trackers[col].add(val)                
                elif rule=="unique_trimmed":
                    check_val=str(val).strip() if pd.notna(val) else None
                    if check_val is None:
                        valid=False
                        logger.error(f"Row {idx} col {col}: null in unique column")
                        break
                    if check_val in unique_trackers.get(col,set()):
                        valid=False
                        logger.error(f"Row {idx} col {col}: duplicate value {check_val}")
                        break
                    unique_trackers.setdefault(col,set()).add(check_val)                
                elif rule=="iso_date":
                    if pd.notna(val):
                        try:
                            pd.to_datetime(val)
                        except:
                            valid=False
                            logger.error(f"Row {idx} col {col}: invalid date {val}")
                            break                
                elif rule.startswith("pattern:"):
                    pattern=rule.split(":",1)[1]
                    if pd.notna(val) and not re.match(pattern,str(val)):
                        valid=False
                        logger.error(f"Row {idx} col {col}: pattern mismatch ({val})")
                        break                
                elif rule.startswith("in_range:"):
                    if pd.notna(val):
                        _,min_val,max_val=rule.split(":")
                        if not (float(min_val)<=float(val)<=float(max_val)):
                            valid=False
                            logger.error(f"Row {idx} col {col}: out of range ({val})")
                            break               
                elif rule.startswith("referential:"):
                    if col in ref_lookup:
                        check_val=str(val).strip().upper() if pd.notna(val) else None
                        if check_val not in ref_lookup[col]:
                            valid=False
                            logger.error(f"Row {idx} col {col}: referential check failed ({val})")
                            break                
                elif rule.startswith("match_company_for_ticker:"):
                    ref_table=rule.split(":",1)[1]
                    if refs and ref_table in refs:
                        ticker_val=str(row.get("ticker","")).strip().upper()
                        company_val=str(row.get("company","")).strip()
                        ref_df=refs[ref_table]
                        ref_map=ref_df.set_index(ref_df["ticker"].str.strip().str.upper())["company"].to_dict()
                        expected=ref_map.get(ticker_val)
                        if expected and company_val.lower()!=expected.strip().lower():
                            valid=False
                            logger.error(f"Row {idx}: company mismatch for ticker {ticker_val}")
                            break
        
        #append to valid or invalid
        if valid:
            valid_data.append(row_copy)
        else:
            invalid_data.append(row_copy)
    
    logger.info(f"validation complete for {table_name}: valid={len(valid_data)}, invalid={len(invalid_data)}")
    return pd.DataFrame(valid_data),pd.DataFrame(invalid_data)


if __name__=="__main__":

    table='stocks_raw'
    csv_file='/Users/shalinitata/Desktop/stocks/Execution/2025/11/25/20:31/merged_stocks_raw.csv'   
    df=pd.read_csv(csv_file)
    valid_df,invalid_df=validate_table(df,table)    
    valid_df.to_csv(f"valid_{table}.csv",index=False)
    invalid_df.to_csv(f"invalid_{table}.csv",index=False)    
    logger.info(f"saved valid_{table}.csv and invalid_{table}.csv")