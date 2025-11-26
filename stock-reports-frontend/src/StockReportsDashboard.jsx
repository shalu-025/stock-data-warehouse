import React, { useState, useEffect } from 'react';
import { Calendar, FileText, TrendingUp, BarChart3, Globe, Activity, Download, AlertCircle, CheckCircle, Clock, Loader2 } from 'lucide-react';
const API = "http://localhost:5174";

const StockReportsDashboard = () => {
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [status, setStatus] = useState('idle');
  const [reports, setReports] = useState([]);
  const [selectedReport, setSelectedReport] = useState(null);
  const [error, setError] = useState('');
  const [jobId, setJobId] = useState(null);

  const reportTypes = [
    {
      id: 'annual_market_intelligence',
      name: 'Annual Market Intelligence',
      icon: TrendingUp,
      description: 'Yearly sector and company performance analysis',
      color: 'from-blue-500 to-cyan-500'
    },
    {
      id: 'sector_stability_volatility',
      name: 'Sector Stability & Volatility',
      icon: BarChart3,
      description: 'Sector-wise risk and resilience metrics',
      color: 'from-purple-500 to-pink-500'
    },
    {
      id: 'cross_market_analysis',
      name: 'Cross-Market Analysis',
      icon: Globe,
      description: 'Currency impact on stock performance',
      color: 'from-green-500 to-emerald-500'
    },
    {
      id: 'forecasting_prediction',
      name: 'Forecasting & Prediction',
      icon: Activity,
      description: 'Future trends and confidence analysis',
      color: 'from-orange-500 to-red-500'
    }
  ];

  const handleFetch = async () => {
    if (!startDate || !endDate) {
      setError('Please select both start and end dates');
      return;
    }

    if (new Date(startDate) > new Date(endDate)) {
      setError('Start date must be before end date');
      return;
    }

    setError('');
    setStatus('submitting');
    setReports([]);
    setSelectedReport(null);

    try {
      const response = await fetch(`${API}/api/generate-reports`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start_date: startDate, end_date: endDate })
      });

      const data = await response.json();
      
      if (data.success) {
        setJobId(data.job_id);
        setStatus('processing');
        pollJobStatus(data.job_id);
      } else {
        setError(data.message || 'Failed to submit request');
        setStatus('idle');
      }
    } catch (err) {
      setError('Failed to connect to server. Make sure the backend is running.');
      setStatus('idle');
    }
  };

  const pollJobStatus = async (id) => {
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`${API}/api/job-status/${id}`);
        const data = await response.json();

        if (data.status === 'completed') {
          clearInterval(interval);
          setStatus('completed');
          setReports(data.reports || []);
        } else if (data.status === 'failed') {
          clearInterval(interval);
          setStatus('failed');
          setError(data.error || 'Report generation failed');
        }
      } catch (err) {
        console.error('Polling error:', err);
      }
    }, 3000);
  };

  const handleDownload = (reportUrl) => {
    window.open(`${API}${reportUrl}`, '_blank');
  };

  const handleViewReport = (report) => {
    setSelectedReport(report);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900">
      {/* Header */}
      <div className="bg-black/30 backdrop-blur-sm border-b border-white/10">
        <div className="max-w-7xl mx-auto px-6 py-6">
          <div className="flex items-center gap-4">
            <div className="p-3 bg-gradient-to-br from-blue-500 to-purple-600 rounded-2xl shadow-lg shadow-purple-500/50">
              <TrendingUp className="w-8 h-8 text-white" />
            </div>
            <div>
              <h1 className="text-3xl font-bold text-white">Stock Market Intelligence</h1>
              <p className="text-purple-200 mt-1">Comprehensive Financial Analysis & Reporting System</p>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-6 py-8">
        {/* Date Selection Card */}
        <div className="bg-white/10 backdrop-blur-md rounded-3xl p-8 mb-8 border border-white/20 shadow-2xl">
          <h2 className="text-2xl font-bold text-white mb-6 flex items-center gap-3">
            <Calendar className="w-7 h-7 text-purple-400" />
            Select Report Period
          </h2>
          
          <div className="grid md:grid-cols-2 gap-6 mb-6">
            <div>
              <label className="block text-purple-200 mb-2 font-medium">Start Date</label>
              <input
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="w-full px-4 py-3 rounded-xl bg-white/10 border border-white/20 text-white placeholder-purple-300 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent"
              />
            </div>
            
            <div>
              <label className="block text-purple-200 mb-2 font-medium">End Date</label>
              <input
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="w-full px-4 py-3 rounded-xl bg-white/10 border border-white/20 text-white placeholder-purple-300 focus:outline-none focus:ring-2 focus:ring-purple-500 focus:border-transparent"
              />
            </div>
          </div>

          {error && (
            <div className="mb-6 p-4 bg-red-500/20 border border-red-500/50 rounded-xl flex items-center gap-3 text-red-200">
              <AlertCircle className="w-5 h-5 flex-shrink-0" />
              <span>{error}</span>
            </div>
          )}

          <button
            onClick={handleFetch}
            disabled={status === 'processing' || status === 'submitting'}
            className="w-full py-4 bg-gradient-to-r from-purple-600 to-blue-600 hover:from-purple-700 hover:to-blue-700 disabled:from-gray-600 disabled:to-gray-700 text-white font-bold rounded-xl transition-all duration-300 shadow-lg shadow-purple-500/50 hover:shadow-purple-500/70 disabled:shadow-none flex items-center justify-center gap-3"
          >
            {status === 'processing' || status === 'submitting' ? (
              <>
                <Loader2 className="w-5 h-5 animate-spin" />
                Generating Reports...
              </>
            ) : (
              <>
                <FileText className="w-5 h-5" />
                Generate Reports
              </>
            )}
          </button>
        </div>

        {/* Status Messages */}
        {status === 'processing' && (
          <div className="mb-8 p-6 bg-blue-500/20 border border-blue-500/50 rounded-2xl">
            <div className="flex items-center gap-4">
              <Loader2 className="w-6 h-6 text-blue-400 animate-spin" />
              <div>
                <h3 className="text-xl font-bold text-white mb-1">Processing Your Request</h3>
                <p className="text-blue-200">Fetching data and generating comprehensive reports. This may take a few minutes...</p>
              </div>
            </div>
          </div>
        )}

        {status === 'completed' && reports.length > 0 && (
          <div className="mb-8 p-6 bg-green-500/20 border border-green-500/50 rounded-2xl">
            <div className="flex items-center gap-4">
              <CheckCircle className="w-6 h-6 text-green-400" />
              <div>
                <h3 className="text-xl font-bold text-white mb-1">Reports Generated Successfully!</h3>
                <p className="text-green-200">All reports are ready. Click on any report below to view or download.</p>
              </div>
            </div>
          </div>
        )}

        {/* Reports Grid */}
        {status === 'completed' && reports.length > 0 && (
          <div>
            <h2 className="text-2xl font-bold text-white mb-6">Available Reports</h2>
            <div className="grid md:grid-cols-2 gap-6">
              {reportTypes.map((reportType) => {
                const report = reports.find(r => r.type === reportType.id);
                const Icon = reportType.icon;
                
                return (
                  <div
                    key={reportType.id}
                    className="bg-white/10 backdrop-blur-md rounded-2xl p-6 border border-white/20 hover:border-white/40 transition-all duration-300 shadow-xl hover:shadow-2xl cursor-pointer group"
                    onClick={() => report && handleViewReport(report)}
                  >
                    <div className="flex items-start gap-4">
                      <div className={`p-4 bg-gradient-to-br ${reportType.color} rounded-xl shadow-lg group-hover:scale-110 transition-transform duration-300`}>
                        <Icon className="w-6 h-6 text-white" />
                      </div>
                      
                      <div className="flex-1">
                        <h3 className="text-lg font-bold text-white mb-2 group-hover:text-purple-300 transition-colors">
                          {reportType.name}
                        </h3>
                        <p className="text-purple-200 text-sm mb-4">{reportType.description}</p>
                        
                        {report ? (
                          <div className="flex gap-2">
                            {report.pdf && (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDownload(report.pdf);
                                }}
                                className="px-4 py-2 bg-purple-600 hover:bg-purple-700 text-white text-sm rounded-lg flex items-center gap-2 transition-colors"
                              >
                                <Download className="w-4 h-4" />
                                PDF
                              </button>
                            )}
                            {report.excel && (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDownload(report.excel);
                                }}
                                className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white text-sm rounded-lg flex items-center gap-2 transition-colors"
                              >
                                <Download className="w-4 h-4" />
                                Excel
                              </button>
                            )}
                            {report.csv && report.csv.map((csvFile, idx) => (
                              <button
                                key={idx}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleDownload(csvFile);
                                }}
                                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-lg flex items-center gap-2 transition-colors"
                              >
                                <Download className="w-4 h-4" />
                                CSV {report.csv.length > 1 ? idx + 1 : ''}
                              </button>
                            ))}
                          </div>
                        ) : (
                          <span className="text-purple-300 text-sm">Not available</span>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* Report Preview Modal */}
        {selectedReport && selectedReport.pdf && (
          <div className="fixed inset-0 bg-black/80 backdrop-blur-sm flex items-center justify-center p-4 z-50">
            <div className="bg-slate-900 rounded-3xl max-w-6xl w-full max-h-[90vh] flex flex-col border border-white/20">
              <div className="p-6 border-b border-white/10 flex items-center justify-between">
                <h3 className="text-2xl font-bold text-white">Report Preview</h3>
                <button
                  onClick={() => setSelectedReport(null)}
                  className="p-2 hover:bg-white/10 rounded-lg transition-colors text-white"
                >
                  ✕
                </button>
              </div>
              <div className="flex-1 overflow-hidden p-6">
                <iframe
                  ssrc={`${API}${selectedReport.pdf}`}
  className="w-full h-full rounded-xl"
  title="Report Preview"
                />
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default StockReportsDashboard;