"""
__title__: Watcher Script for Stock Data Warehouse Project
__employee__id: 800338
__author__: Shalini Tata
__created__: 25-11-2024
__purpose__: Watch for config files and trigger historical data processing while maintaining live schedule
version :v1.0
"""

import time
import os
import sys
import json
import threading
from subprocess import Popen
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from logger import logger
from datetime import datetime

watch_fold="watch_folder/"
live=None

os.makedirs(watch_fold,exist_ok=True)


class StockConfigHandler(FileSystemEventHandler):
    """handles events for json config files in watch folder"""
    
    def __init__(self):
        self.processing_lock=threading.Lock()
        self.processing_files=set()
    
    def on_modified(self, event):
        """triggered when existing json config is modified"""
        if event.src_path.endswith(".json"):
            logger.info(f"Modified config detected: {event.src_path}")
            self.process_config(event.src_path)
    
    def on_created(self, event):
        """triggered when new json config is created"""
        if event.src_path.endswith(".json"):
            logger.info(f"New config detected: {event.src_path}")
            self.process_config(event.src_path)
    
    def process_config(self, config_path):
        """validate and process config file"""
        if config_path in self.processing_files:
            logger.info(f"Config {config_path} already being processed, skipping")
            return
        
        with self.processing_lock:
            self.processing_files.add(config_path)
        
        try:
            with open(config_path, 'r') as f:
                config=json.load(f)
            
            if "start_date" in config and "end_date" in config:
                start_date=config["start_date"]
                end_date=config["end_date"]
                
                try:
                    datetime.strptime(start_date, "%Y-%m-%d")
                    datetime.strptime(end_date, "%Y-%m-%d")
                    logger.info(f"Processing historical range: {start_date} to {end_date}")
                    self.trigger_historical(config_path)
                except ValueError:
                    logger.error(f"Invalid date format in {config_path}")
            else:
                logger.warning(f"Config missing required fields: {config_path}")
                
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in {config_path}")
        except Exception as e:
            logger.error(f"Error processing config: {e}")
        finally:
            with self.processing_lock:
                self.processing_files.discard(config_path)
    
    def trigger_historical(self, config_path):
        """trigger historical data processing"""
        try:
            logger.info(f"Triggering check.py for historical processing")
            Popen([sys.executable, "check.py", "--config", config_path])
        except Exception as e:
            logger.error(f"Failed to trigger check.py: {e}")


def start_live_scheduler():
    """start the live data collection scheduler in background"""
    global live
    try:
        logger.info("Starting live data scheduler...")
        live=Popen([sys.executable, "check.py", "--mode", "live"])
        logger.info("Live scheduler started successfully")
    except Exception as e:
        logger.error(f"Failed to start live scheduler: {e}")


if __name__=="__main__":
    logger.info("="*60)
    logger.info("Stock Data Watcher Started")
    logger.info("="*60)
    
    start_live_scheduler()
    
    event_handler=StockConfigHandler()
    observer=Observer()
    observer.schedule(event_handler, path=watch_fold, recursive=False)
    observer.start()
    
    logger.info(f"Watching '{watch_fold}' for config files...")
    logger.info("Drop JSON configs with start_date and end_date for historical processing")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down watcher...")
        observer.stop()
        if live:
            live.terminate()
            logger.info("Live scheduler stopped")
        logger.info("Watcher stopped")
    
    observer.join()