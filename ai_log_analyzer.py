#!/usr/bin/env python3
"""
AI Log Analyzer for PostgreSQL Statistics Estimator

This module provides advanced log analysis and streaming capabilities specifically
designed for AI models to diagnose issues, track experiment progress, and identify
patterns in failures. It integrates with the existing logging system to provide
real-time analysis and historical pattern detection.

Usage:
    python ai_log_analyzer.py [options]

Author: AI Assistant
Created: 2025
"""

import re
import json
import time
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import subprocess
import requests
from collections import defaultdict

@dataclass
class LogEntry:
    """Structured log entry"""
    timestamp: datetime
    level: str
    logger: str
    message: str
    raw_line: str
    experiment_id: Optional[int] = None
    trial_number: Optional[int] = None
    error_type: Optional[str] = None

@dataclass
class ErrorPattern:
    """Error pattern for classification"""
    pattern: str
    error_type: str
    severity: str
    description: str
    suggested_fix: str

class AILogAnalyzer:
    """Advanced log analyzer for AI debugging"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.log_dir = self.project_root / "app" / "app" / "logs"
        self.base_url = "http://localhost:8000"
        
        # Error patterns for classification
        self.error_patterns = [
            ErrorPattern(
                pattern=r"Failed to apply statistics.*OpenAI.*API",
                error_type="OPENAI_API_ERROR",
                severity="HIGH",
                description="OpenAI API call failed during statistics generation",
                suggested_fix="Check OpenAI API key, rate limits, and network connectivity"
            ),
            ErrorPattern(
                pattern=r"Query execution timed out",
                error_type="QUERY_TIMEOUT",
                severity="MEDIUM",
                description="Database query exceeded timeout limit",
                suggested_fix="Increase timeout or optimize query/statistics"
            ),
            ErrorPattern(
                pattern=r"Failed to create temporary database",
                error_type="DATABASE_CREATION_ERROR",
                severity="HIGH",
                description="Cannot create temporary experiment database",
                suggested_fix="Check PostgreSQL connection and permissions"
            ),
            ErrorPattern(
                pattern=r"ValidationError.*iterations",
                error_type="PARAMETER_VALIDATION_ERROR",
                severity="LOW",
                description="Invalid experiment parameters",
                suggested_fix="Check experiment configuration parameters"
            ),
            ErrorPattern(
                pattern=r"StatsApplicationError",
                error_type="STATS_APPLICATION_ERROR",
                severity="HIGH",
                description="Failed to apply statistics to database",
                suggested_fix="Check statistics source configuration and database state"
            ),
            ErrorPattern(
                pattern=r"Connection refused.*5432",
                error_type="DATABASE_CONNECTION_ERROR",
                severity="HIGH",
                description="Cannot connect to PostgreSQL database",
                suggested_fix="Ensure PostgreSQL container is running and healthy"
            ),
            ErrorPattern(
                pattern=r"YAML.*parsing.*error",
                error_type="CONFIG_PARSING_ERROR",
                severity="MEDIUM",
                description="Configuration file parsing failed",
                suggested_fix="Check YAML syntax in configuration files"
            )
        ]
        
        # Tracking for analysis
        self.error_counts = defaultdict(int)
        self.experiment_timeline = []
        self.performance_metrics = []
    
    def parse_log_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single log line into structured data"""
        # Pattern for main application logs
        app_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) - ([^-]+) - ([^-]+) - (.+)'
        
        match = re.match(app_pattern, line.strip())
        if not match:
            return None
        
        timestamp_str, logger, level, message = match.groups()
        
        try:
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
        except ValueError:
            return None
        
        entry = LogEntry(
            timestamp=timestamp,
            level=level.strip(),
            logger=logger.strip(),
            message=message.strip(),
            raw_line=line
        )
        
        # Extract experiment ID if present
        exp_match = re.search(r'experiment[^0-9]*(\d+)', message.lower())
        if exp_match:
            entry.experiment_id = int(exp_match.group(1))
        
        # Extract trial number if present
        trial_match = re.search(r'trial[^0-9]*(\d+)', message.lower())
        if trial_match:
            entry.trial_number = int(trial_match.group(1))
        
        # Classify error type
        if level in ['ERROR', 'CRITICAL']:
            entry.error_type = self.classify_error(message)
        
        return entry
    
    def classify_error(self, message: str) -> str:
        """Classify error message using patterns"""
        for pattern in self.error_patterns:
            if re.search(pattern.pattern, message, re.IGNORECASE):
                self.error_counts[pattern.error_type] += 1
                return pattern.error_type
        return "UNKNOWN_ERROR"
    
    def get_error_description(self, error_type: str) -> Optional[ErrorPattern]:
        """Get error description and suggested fix"""
        for pattern in self.error_patterns:
            if pattern.error_type == error_type:
                return pattern
        return None
    
    def analyze_log_file(self, log_file: Path, hours_back: int = 24) -> Dict[str, Any]:
        """Analyze a log file and return structured analysis"""
        if not log_file.exists():
            return {"error": f"Log file not found: {log_file}"}
        
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        entries = []
        errors = []
        warnings = []
        experiments = defaultdict(list)
        
        try:
            with open(log_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    entry = self.parse_log_line(line)
                    if not entry or entry.timestamp < cutoff_time:
                        continue
                    
                    entries.append(entry)
                    
                    if entry.level == 'ERROR':
                        errors.append(entry)
                    elif entry.level == 'WARNING':
                        warnings.append(entry)
                    
                    if entry.experiment_id:
                        experiments[entry.experiment_id].append(entry)
        
        except Exception as e:
            return {"error": f"Failed to read log file: {e}"}
        
        # Generate analysis
        analysis = {
            "file": str(log_file),
            "analysis_time": datetime.now().isoformat(),
            "time_range": f"Last {hours_back} hours",
            "total_entries": len(entries),
            "summary": {
                "errors": len(errors),
                "warnings": len(warnings),
                "experiments": len(experiments),
                "error_types": dict(self.error_counts)
            },
            "recent_errors": [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "message": e.message,
                    "error_type": e.error_type,
                    "experiment_id": e.experiment_id
                }
                for e in sorted(errors, key=lambda x: x.timestamp, reverse=True)[:10]
            ],
            "experiment_status": self._analyze_experiments(experiments),
            "recommendations": self._generate_recommendations()
        }
        
        return analysis
    
    def _analyze_experiments(self, experiments: Dict[int, List[LogEntry]]) -> List[Dict]:
        """Analyze experiment status and performance"""
        experiment_analysis = []
        
        for exp_id, entries in experiments.items():
            errors = [e for e in entries if e.level == 'ERROR']
            start_time = min(e.timestamp for e in entries)
            end_time = max(e.timestamp for e in entries)
            duration = (end_time - start_time).total_seconds()
            
            # Determine status
            status = "UNKNOWN"
            if any("completed successfully" in e.message.lower() for e in entries):
                status = "SUCCESS"
            elif errors:
                status = "FAILED"
            elif any("running" in e.message.lower() for e in entries):
                status = "RUNNING"
            
            experiment_analysis.append({
                "experiment_id": exp_id,
                "status": status,
                "start_time": start_time.isoformat(),
                "duration_seconds": duration,
                "error_count": len(errors),
                "total_log_entries": len(entries),
                "main_errors": [e.error_type for e in errors if e.error_type]
            })
        
        return sorted(experiment_analysis, key=lambda x: x["start_time"], reverse=True)
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on error patterns"""
        recommendations = []
        
        # Check most common errors
        if self.error_counts:
            most_common = max(self.error_counts.items(), key=lambda x: x[1])
            error_type, count = most_common
            
            pattern = self.get_error_description(error_type)
            if pattern and count > 2:
                recommendations.append(
                    f"🔴 Frequent {error_type}: {pattern.description}. "
                    f"Occurred {count} times. Fix: {pattern.suggested_fix}"
                )
        
        # Database connection issues
        if self.error_counts.get("DATABASE_CONNECTION_ERROR", 0) > 0:
            recommendations.append(
                "🔴 Database connectivity issues detected. "
                "Run: docker-compose ps to check container status"
            )
        
        # API-related issues
        if self.error_counts.get("OPENAI_API_ERROR", 0) > 0:
            recommendations.append(
                "🔴 OpenAI API issues detected. "
                "Check API key in environment variables and rate limits"
            )
        
        return recommendations
    
    def stream_logs_realtime(self, follow_experiments: bool = True):
        """Stream logs in real-time with analysis"""
        print("🔄 Starting real-time log streaming...")
        print("Press Ctrl+C to stop")
        
        # Track file positions
        file_positions = {}
        
        try:
            while True:
                today = datetime.now().strftime('%Y-%m-%d')
                log_files = [
                    self.log_dir / f"experiment_{today}.log",
                    self.log_dir / f"stats_source_{today}.log"
                ]
                
                for log_file in log_files:
                    if not log_file.exists():
                        continue
                    
                    # Get file position
                    pos = file_positions.get(str(log_file), 0)
                    
                    try:
                        with open(log_file, 'r') as f:
                            f.seek(pos)
                            new_lines = f.readlines()
                            file_positions[str(log_file)] = f.tell()
                        
                        # Process new lines
                        for line in new_lines:
                            entry = self.parse_log_line(line)
                            if entry:
                                self._print_log_entry(entry)
                                
                                # Track experiment progress
                                if follow_experiments and entry.experiment_id:
                                    self._track_experiment_progress(entry)
                    
                    except Exception as e:
                        print(f"⚠️  Error reading {log_file}: {e}")
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n🛑 Real-time streaming stopped")
    
    def _print_log_entry(self, entry: LogEntry):
        """Print formatted log entry"""
        # Color coding
        colors = {
            'ERROR': '\033[91m',    # Red
            'WARNING': '\033[93m',  # Yellow
            'INFO': '\033[92m',     # Green
            'DEBUG': '\033[94m'     # Blue
        }
        reset = '\033[0m'
        
        color = colors.get(entry.level, '')
        timestamp_str = entry.timestamp.strftime('%H:%M:%S')
        
        prefix = f"[{timestamp_str}] {color}{entry.level}{reset}"
        
        if entry.experiment_id:
            prefix += f" [EXP:{entry.experiment_id}]"
        if entry.trial_number:
            prefix += f" [T:{entry.trial_number}]"
        if entry.error_type and entry.error_type != "UNKNOWN_ERROR":
            prefix += f" [{entry.error_type}]"
        
        print(f"{prefix} {entry.message}")
        
        # Show suggestions for errors
        if entry.level == 'ERROR' and entry.error_type:
            pattern = self.get_error_description(entry.error_type)
            if pattern:
                print(f"    💡 {pattern.suggested_fix}")
    
    def _track_experiment_progress(self, entry: LogEntry):
        """Track experiment progress for summary"""
        if "completed successfully" in entry.message.lower():
            print(f"✅ Experiment {entry.experiment_id} completed successfully")
        elif "failed" in entry.message.lower() and entry.level == 'ERROR':
            print(f"❌ Experiment {entry.experiment_id} failed")
    
    def quick_health_check(self) -> Dict[str, Any]:
        """Quick health check of the system"""
        print("🏥 Running quick health check...")
        
        health = {
            "timestamp": datetime.now().isoformat(),
            "containers": {},
            "web_service": {},
            "recent_errors": [],
            "log_files": {}
        }
        
        # Check Docker containers
        try:
            result = subprocess.run(
                ["docker-compose", "ps", "--format", "json"],
                capture_output=True, text=True, cwd=self.project_root
            )
            if result.returncode == 0:
                health["containers"]["status"] = "running"
                health["containers"]["details"] = result.stdout
            else:
                health["containers"]["status"] = "error"
                health["containers"]["error"] = result.stderr
        except Exception as e:
            health["containers"]["status"] = "error"
            health["containers"]["error"] = str(e)
        
        # Check web service
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            health["web_service"]["status"] = "ok"
            health["web_service"]["http_code"] = response.status_code
        except Exception as e:
            health["web_service"]["status"] = "error"
            health["web_service"]["error"] = str(e)
        
        # Check log files
        for log_type in ["experiment", "stats_source"]:
            today = datetime.now().strftime('%Y-%m-%d')
            log_file = self.log_dir / f"{log_type}_{today}.log"
            
            if log_file.exists():
                stat = log_file.stat()
                health["log_files"][log_type] = {
                    "exists": True,
                    "size_mb": round(stat.st_size / 1024 / 1024, 2),
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
                
                # Get recent errors
                analysis = self.analyze_log_file(log_file, hours_back=1)
                if "recent_errors" in analysis:
                    health["recent_errors"].extend(analysis["recent_errors"])
            else:
                health["log_files"][log_type] = {"exists": False}
        
        # Print summary
        print(f"Containers: {health['containers']['status']}")
        print(f"Web Service: {health['web_service']['status']}")
        print(f"Recent Errors: {len(health['recent_errors'])}")
        
        return health
    
    def generate_debug_report(self, hours_back: int = 6) -> str:
        """Generate comprehensive debug report"""
        print(f"📊 Generating debug report for last {hours_back} hours...")
        
        report_data = {
            "generated_at": datetime.now().isoformat(),
            "time_range_hours": hours_back,
            "health_check": self.quick_health_check(),
            "log_analysis": {}
        }
        
        # Analyze all log files
        today = datetime.now().strftime('%Y-%m-%d')
        for log_type in ["experiment", "stats_source"]:
            log_file = self.log_dir / f"{log_type}_{today}.log"
            analysis = self.analyze_log_file(log_file, hours_back)
            report_data["log_analysis"][log_type] = analysis
        
        # Generate report file
        report_file = self.project_root / f"debug_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        print(f"📄 Debug report saved to: {report_file}")
        
        # Print summary
        total_errors = sum(
            analysis.get("summary", {}).get("errors", 0)
            for analysis in report_data["log_analysis"].values()
            if isinstance(analysis, dict)
        )
        
        print(f"📈 Summary: {total_errors} errors found in last {hours_back} hours")
        
        return str(report_file)

def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="AI Log Analyzer")
    parser.add_argument("command", 
                       choices=["analyze", "stream", "health", "report"],
                       help="Command to execute")
    parser.add_argument("--hours", type=int, default=24,
                       help="Hours back to analyze")
    parser.add_argument("--log-type", default="experiment",
                       choices=["experiment", "stats_source"],
                       help="Log type to analyze")
    
    args = parser.parse_args()
    
    analyzer = AILogAnalyzer()
    
    if args.command == "analyze":
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = analyzer.log_dir / f"{args.log_type}_{today}.log"
        analysis = analyzer.analyze_log_file(log_file, args.hours)
        print(json.dumps(analysis, indent=2))
    
    elif args.command == "stream":
        analyzer.stream_logs_realtime()
    
    elif args.command == "health":
        health = analyzer.quick_health_check()
        print(json.dumps(health, indent=2))
    
    elif args.command == "report":
        analyzer.generate_debug_report(args.hours)

if __name__ == "__main__":
    main()