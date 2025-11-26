"""
Title: Backend Server for Stock Data Warehouse UI
Author: shalini Tata
Created: 25-11-2024
Purpose: FastAPI backend to connect UI with watcher.py processing system
"""

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pathlib import Path
import json
import uuid
from datetime import datetime
import pandas as pd
import os
from logger import logger

app = FastAPI(title="Stock Data Warehouse API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static HTML
UI_FOLDER = Path("UI")
WATCH_FOLDER = Path("watch_folder")
EXECUTIONS_FOLDER = Path("Execution")  # Changed to match your actual folder

# Ensure folders exist
WATCH_FOLDER.mkdir(exist_ok=True)
UI_FOLDER.mkdir(exist_ok=True)

class DateRangeRequest(BaseModel):
    start_date: str
    end_date: str

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    """Serve the main dashboard HTML"""
    html_file = UI_FOLDER / "index.html"
    if not html_file.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    
    with open(html_file, 'r') as f:
        return HTMLResponse(content=f.read())

@app.post("/trigger_processing")
async def trigger_processing(request: DateRangeRequest):
    """Create config JSON in watch_folder to trigger processing"""
    try:
        # Validate dates
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d")
        
        if start_date > end_date:
            raise HTTPException(status_code=400, detail="Start date must be before end date")
        
        # Create unique config file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        config_filename = f"config_{timestamp}_{uuid.uuid4().hex[:8]}.json"
        config_path = WATCH_FOLDER / config_filename
        
        # Create execution directory name matching your actual structure
        now = datetime.now()
        execution_dir = f"Execution/{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H:%M')}"
        
        # Create config JSON
        config_data = {
            "start_date": request.start_date,
            "end_date": request.end_date,
            "timestamp": timestamp,
            "execution_dir": execution_dir
        }
        
        # Write config to watch folder
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        logger.info(f"Created config file: {config_path}")
        logger.info(f"Date range: {request.start_date} to {request.end_date}")
        
        return {
            "success": True,
            "message": "Processing triggered successfully",
            "config_file": str(config_path),
            "execution_dir": execution_dir
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")
    except Exception as e:
        logger.error(f"Error triggering processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/check_reports")
async def check_reports(execution_dir: str):
    """Check if reports are ready in the execution directory"""
    try:
        reports_dir = Path(execution_dir) / "reports"
        
        if not reports_dir.exists():
            logger.info(f"Reports directory does not exist yet: {reports_dir}")
            return {
                "reports_ready": False,
                "reports": [],
                "message": "Reports directory not found"
            }
        
        # Find all PDF report files
        report_files = list(reports_dir.glob("report*.pdf"))
        
        if not report_files:
            logger.info(f"No PDF reports found in {reports_dir}")
            return {
                "reports_ready": False,
                "reports": [],
                "message": "No reports generated yet"
            }
        
        # Extract report information
        reports_info = []
        for report_file in report_files:
            report_name = report_file.stem  # filename without extension
            reports_info.append({
                "name": report_name,
                "filename": report_file.name,
                "path": str(report_file)
            })
        
        logger.info(f"Found {len(reports_info)} reports in {reports_dir}")
        
        return {
            "reports_ready": True,
            "reports": reports_info,
            "count": len(reports_info)
        }
        
    except Exception as e:
        logger.error(f"Error checking reports: {e}")
        return {
            "reports_ready": False,
            "reports": [],
            "error": str(e)
        }

@app.get("/get_report_preview")
async def get_report_preview(path: str):
    """Get report metadata for preview"""
    try:
        report_path = Path(path)
        
        if not report_path.exists():
            raise HTTPException(status_code=404, detail="Report not found")
        
        # Get file info
        file_stat = report_path.stat()
        file_size_mb = file_stat.st_size / (1024 * 1024)
        modified_time = datetime.fromtimestamp(file_stat.st_mtime)
        
        return {
            "success": True,
            "filename": report_path.name,
            "size_mb": round(file_size_mb, 2),
            "modified": modified_time.strftime("%Y-%m-%d %H:%M:%S"),
            "path": str(report_path)
        }
        
    except Exception as e:
        logger.error(f"Error getting report preview: {e}")
        return {
            "success": False,
            "message": str(e)
        }

@app.get("/download_report")
async def download_report(path: str):
    """Download full report PDF"""
    try:
        report_path = Path(path)
        
        if not report_path.exists():
            raise HTTPException(status_code=404, detail="Report not found")
        
        return FileResponse(
            path=report_path,
            filename=report_path.name,
            media_type="application/pdf"
        )
        
    except Exception as e:
        logger.error(f"Error downloading report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/view_report")
async def view_report(path: str):
    """View report PDF in browser"""
    try:
        report_path = Path(path)
        
        if not report_path.exists():
            raise HTTPException(status_code=404, detail="Report not found")
        
        return FileResponse(
            path=report_path,
            media_type="application/pdf",
            headers={
                "Content-Disposition": f"inline; filename={report_path.name}"
            }
        )
        
    except Exception as e:
        logger.error(f"Error viewing report: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/list_executions")
async def list_executions():
    """List all execution directories"""
    try:
        if not EXECUTIONS_FOLDER.exists():
            return {"executions": []}
        
        executions = []
        
        # Walk through year/month/day/time structure
        for year_dir in EXECUTIONS_FOLDER.iterdir():
            if not year_dir.is_dir():
                continue
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                for day_dir in month_dir.iterdir():
                    if not day_dir.is_dir():
                        continue
                    for time_dir in day_dir.iterdir():
                        if not time_dir.is_dir():
                            continue
                        
                        reports_dir = time_dir / "reports"
                        report_count = len(list(reports_dir.glob("*.pdf"))) if reports_dir.exists() else 0
                        
                        executions.append({
                            "name": f"{year_dir.name}/{month_dir.name}/{day_dir.name}/{time_dir.name}",
                            "path": str(time_dir),
                            "report_count": report_count
                        })
        
        # Sort by name (newest first)
        executions.sort(key=lambda x: x['name'], reverse=True)
        
        return {"executions": executions}
        
    except Exception as e:
        logger.error(f"Error listing executions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "watch_folder": str(WATCH_FOLDER.absolute()),
        "watch_folder_exists": WATCH_FOLDER.exists(),
        "execution_folder": str(EXECUTIONS_FOLDER.absolute()),
        "execution_folder_exists": EXECUTIONS_FOLDER.exists()
    }


# Run server when script is executed directly
if __name__ == "__main__":
    import uvicorn
    
    print("="*60)
    print("Starting Stock Data Warehouse Backend Server")
    print(f"Watch folder: {WATCH_FOLDER.absolute()}")
    print(f"UI folder: {UI_FOLDER.absolute()}")
    print(f"Execution folder: {EXECUTIONS_FOLDER.absolute()}")
    print("Server running at: http://localhost:8000")
    print("="*60)
    
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)