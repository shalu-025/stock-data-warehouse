"""
Title: API Server for Stock Reports Dashboard
Author: Shalini Tata
Created: 25-11-2024
Purpose: REST API to handle report generation requests from frontend
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import json
import os
import time
from datetime import datetime
from pathlib import Path
import uuid
from logger import logger

app = Flask(__name__)
CORS(app)

WATCH_FOLDER = "watch_folder/"
EXECUTION_FOLDER = "Execution/"
JOBS = {}  # Store job status in memory

os.makedirs(WATCH_FOLDER, exist_ok=True)

@app.route('/api/generate-reports', methods=['POST'])
def generate_reports():
    """Handle report generation request"""
    try:
        data = request.json
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({'success': False, 'message': 'Missing required fields'}), 400
        
        # Validate dates
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({'success': False, 'message': 'Invalid date format'}), 400
        
        # Generate unique job ID
        job_id = str(uuid.uuid4())
        
        # Create config file in watch_folder
        config = {
            'start_date': start_date,
            'end_date': end_date,
            'job_id': job_id
        }
        
        config_path = os.path.join(WATCH_FOLDER, f"{job_id}.json")
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        # Initialize job status
        JOBS[job_id] = {
            'status': 'processing',
            'start_date': start_date,
            'end_date': end_date,
            'created_at': datetime.now().isoformat(),
            'reports': []
        }
        
        logger.info(f"Created config file: {config_path}")
        logger.info(f"Job {job_id} initiated for {start_date} to {end_date}")
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': 'Report generation started'
        })
        
    except Exception as e:
        logger.error(f"Error in generate_reports: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    """Check job status and return reports if completed"""
    try:
        if job_id not in JOBS:
            return jsonify({'success': False, 'message': 'Job not found'}), 404
        
        job = JOBS[job_id]
        
        # Check if reports are generated
        if job['status'] == 'processing':
            # Look for generated reports in Execution folder
            reports = find_reports(job_id, job['start_date'], job['end_date'])
            
            if reports:
                JOBS[job_id]['status'] = 'completed'
                JOBS[job_id]['reports'] = reports
                JOBS[job_id]['completed_at'] = datetime.now().isoformat()
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'status': JOBS[job_id]['status'],
            'reports': JOBS[job_id].get('reports', [])
        })
        
    except Exception as e:
        logger.error(f"Error in job_status: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

def find_reports(job_id, start_date, end_date):
    """Find generated reports in Execution folder"""
    reports = []
    
    try:
        # Parse dates to find execution folder
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Look in today's execution folder (since reports are generated today)
        today = datetime.now()
        exec_path = Path(EXECUTION_FOLDER) / str(today.year) / f"{today.month:02d}" / f"{today.day:02d}"
        
        if not exec_path.exists():
            return reports
        
        # Find the most recent timestamped folder
        timestamped_dirs = sorted([d for d in exec_path.iterdir() if d.is_dir()], 
                                 key=lambda x: x.name, reverse=True)
        
        if not timestamped_dirs:
            return reports
        
        latest_dir = timestamped_dirs[0]
        reports_dir = latest_dir / "reports"
        
        if not reports_dir.exists():
            return reports
        
        # Map report files
        report_mapping = {
            'annual_market_intelligence': {
                'pattern': 'report1_annual_market_intelligence_',
                'csv_patterns': ['report1_yearly_performance.csv', 'report1_company_performance.csv']
            },
            'sector_stability_volatility': {
                'pattern': 'report2_sector_stability_',
                'csv_patterns': ['report2_sector_stability.csv']
            },
            'cross_market_analysis': {
                'pattern': 'report3_cross_market_',
                'csv_patterns': ['report3_currency_yearly.csv', 'report3_company_currency.csv']
            },
            'forecasting_prediction': {
                'pattern': 'report4_forecasting_',
                'csv_patterns': ['report4_quarterly_trends.csv', 'report4_sector_forecast.csv']
            }
        }
        
        for report_type, config in report_mapping.items():
            report_data = {'type': report_type, 'csv': []}
            
            # Find PDF
            pdf_files = list(reports_dir.glob(f"{config['pattern']}*.pdf"))
            if pdf_files:
                report_data['pdf'] = f"/files/{pdf_files[0].relative_to(Path('.'))}"
            
            # Find Excel (for report 2)
            excel_files = list(reports_dir.glob(f"{config['pattern']}*.xlsx"))
            if excel_files:
                report_data['excel'] = f"/files/{excel_files[0].relative_to(Path('.'))}"
            
            # Find CSVs
            for csv_pattern in config['csv_patterns']:
                csv_path = latest_dir / csv_pattern
                if csv_path.exists():
                    report_data['csv'].append(f"/files/{csv_path.relative_to(Path('.'))}")
            
            if report_data.get('pdf') or report_data.get('csv'):
                reports.append(report_data)
        
        return reports
        
    except Exception as e:
        logger.error(f"Error finding reports: {e}")
        return reports

@app.route('/files/<path:filepath>')
def serve_file(filepath):
    """Serve generated report files"""
    try:
        file_path = Path(filepath)
        if file_path.exists():
            return send_file(file_path, as_attachment=False)
        return jsonify({'success': False, 'message': 'File not found'}), 404
    except Exception as e:
        logger.error(f"Error serving file: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    logger.info("="*60)
    logger.info("Starting API Server on http://localhost:5174")
    logger.info("="*60)
    app.run(host='0.0.0.0', port=5000, debug=True)