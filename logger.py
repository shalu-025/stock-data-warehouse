"""
Title: LOGGING CONFIGURATION
Author: Shalini Tata
Created Date: 23-11-2025
Last Updated date: 23-11-2025
Purpose: Logging with colored console output and structured log files using UUID
"""

import os
import logging
from colorama import Fore, Style, init
import uuid
from new import create_folder
init(autoreset=True)
class ColorFormatter(logging.Formatter):
    """ assign colors for logs on console """
    def format(self, record):
        msg=super().format(record)
        if record.levelno==logging.ERROR:
            msg=f"{Fore.RED}{msg}{Style.RESET_ALL}"
        elif record.levelno==logging.WARNING:
            msg=f"{Fore.YELLOW}{msg}{Style.RESET_ALL}"
        elif record.levelno==logging.INFO:
            msg=f"{Fore.GREEN}{msg}{Style.RESET_ALL}"
        elif record.levelno==logging.DEBUG:
            msg=f"{Fore.BLUE}{msg}{Style.RESET_ALL}"
        return msg

def create_logger(program_name="PROGRAM", filename="FILE", user_id="USERID", username="USERNAME", file_path="."):
    """funtion to create logger with desired requirements"""
    log_dir=create_folder("Logging")
    log_uuid=str(uuid.uuid4())
    log_file_path=os.path.join(log_dir, f"{log_uuid}.log")
    print(f"{Fore.CYAN}Log UUID: {log_uuid}{Style.RESET_ALL}")
    logger=logging.getLogger("main_logger")
    logger.setLevel(logging.DEBUG)
    logger.propagate=False
    
    if not logger.handlers:
        fh=logging.FileHandler(log_file_path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(fh)
        ch=logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(ColorFormatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(ch)
        header=f"\n{'*'*10} {program_name} {'*'*10}\nFilename: {filename}\nUser ID: {user_id}\nUsername: {username}\nFile Path: {file_path}\n{'*'*10} {filename} {'*'*10}\n"
        with open(log_file_path, "a") as f:
            f.write(header)
        footer=f"\n{'*'*10} END OF LOG FILE {'*'*10}\n"
        def log_footer():
            with open(log_file_path, "a") as f:
                f.write(footer)
        logger.log_footer=log_footer

    return logger

logger=create_logger(program_name="Local Data Warehouse and Analytical Reporting System", filename="Stock Market and Macroeconomic Intelligence", user_id="800338", username="Shalini Tata", file_path="/Users/shalinitata/Desktop/stocks/watcher.py")
logger.info("===== started running the process ======")

