#!/usr/bin/env python3
"""
AI Test Interface for PostgreSQL Statistics Estimator

This script provides a command-line interface for AI models to run tests,
view logs, and iterate on the experiment code. It integrates with the Docker
containerized application and provides extensive error reporting.

Usage:
    python ai_test_interface.py <command> [options]

Commands:
    start           - Start the Docker environment
    stop            - Stop the Docker environment  
    restart         - Restart the Docker environment
    logs            - View logs (experiment, stats, web, postgres)
    test            - Run a quick test experiment
    debug           - Debug mode with detailed logging
    shell           - Open shell in web container
    psql            - Open PostgreSQL shell
    status          - Check container status
    clean           - Clean restart (removes all data)

Author: AI Assistant
Created: 2025
"""

import sys
import subprocess
import json
import time
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
import threading
from datetime import datetime

class Colors:
    """ANSI color codes for terminal output"""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class AITestInterface:
    """Main interface for AI testing and debugging"""
    
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.project_root = Path(__file__).parent
        self.log_dir = self.project_root / "app" / "app" / "logs"
        
    def run_command(self, cmd: List[str], capture_output: bool = True, 
                   cwd: Optional[Path] = None) -> Tuple[int, str, str]:
        """Run a shell command and return exit code, stdout, stderr"""
        try:
            if cwd is None:
                cwd = self.project_root
                
            print(f"{Colors.CYAN}[CMD]{Colors.END} {' '.join(cmd)}")
            
            if capture_output:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, cwd=cwd
                )
                return result.returncode, result.stdout, result.stderr
            else:
                result = subprocess.run(cmd, cwd=cwd)
                return result.returncode, "", ""
                
        except Exception as e:
            return 1, "", str(e)
    
    def print_header(self, title: str):
        """Print a formatted header"""
        print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.CYAN}{title.center(60)}{Colors.END}")
        print(f"{Colors.BOLD}{Colors.CYAN}{'='*60}{Colors.END}\n")
    
    def print_success(self, message: str):
        """Print success message"""
        print(f"{Colors.GREEN}✓ {message}{Colors.END}")
    
    def print_error(self, message: str):
        """Print error message"""
        print(f"{Colors.RED}✗ {message}{Colors.END}")
    
    def print_warning(self, message: str):
        """Print warning message"""
        print(f"{Colors.YELLOW}⚠ {message}{Colors.END}")
    
    def print_info(self, message: str):
        """Print info message"""
        print(f"{Colors.BLUE}ℹ {message}{Colors.END}")

    def start_environment(self) -> bool:
        """Start the Docker environment"""
        self.print_header("Starting Docker Environment")
        
        code, stdout, stderr = self.run_command(
            ["docker-compose", "up", "--build", "-d"]
        )
        
        if code != 0:
            self.print_error(f"Failed to start Docker environment")
            print(f"STDOUT: {stdout}")
            print(f"STDERR: {stderr}")
            return False
        
        self.print_success("Docker environment started")
        
        # Wait for services to be ready
        self.print_info("Waiting for services to be ready...")
        time.sleep(10)
        
        # Check if web service is responding
        max_retries = 30
        for i in range(max_retries):
            try:
                response = requests.get(f"{self.base_url}/", timeout=5)
                if response.status_code == 200:
                    self.print_success("Web service is ready")
                    return True
            except requests.exceptions.RequestException:
                pass
            
            if i < max_retries - 1:
                print(f"Waiting for web service... ({i+1}/{max_retries})")
                time.sleep(2)
        
        self.print_warning("Web service may not be fully ready")
        return True
    
    def stop_environment(self) -> bool:
        """Stop the Docker environment"""
        self.print_header("Stopping Docker Environment")
        
        code, stdout, stderr = self.run_command(
            ["docker-compose", "down"]
        )
        
        if code != 0:
            self.print_error("Failed to stop Docker environment")
            print(f"STDERR: {stderr}")
            return False
        
        self.print_success("Docker environment stopped")
        return True
    
    def restart_environment(self) -> bool:
        """Restart the Docker environment"""
        self.print_header("Restarting Docker Environment")
        
        if not self.stop_environment():
            return False
        
        time.sleep(2)
        return self.start_environment()
    
    def clean_restart(self) -> bool:
        """Clean restart - removes all data"""
        self.print_header("Clean Restart (Removing All Data)")
        
        # Stop containers
        self.run_command(["docker-compose", "down"])
        
        # Remove volumes
        code, stdout, stderr = self.run_command(
            ["docker-compose", "down", "-v"]
        )
        
        # Clean images
        self.run_command(["docker", "system", "prune", "-f"])
        
        time.sleep(2)
        return self.start_environment()
    
    def get_status(self) -> Dict:
        """Get container status"""
        self.print_header("Container Status")
        
        code, stdout, stderr = self.run_command(
            ["docker-compose", "ps"]
        )
        
        print(stdout)
        
        # Check web service health
        try:
            response = requests.get(f"{self.base_url}/", timeout=5)
            self.print_success(f"Web service: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            self.print_error(f"Web service: {e}")
        
        # Check database connectivity
        try:
            response = requests.get(f"{self.base_url}/experiment/debug/status", timeout=5)
            if response.status_code == 200:
                self.print_success("Database connectivity: OK")
            else:
                self.print_warning(f"Database connectivity: HTTP {response.status_code}")
        except requests.exceptions.RequestException as e:
            self.print_error(f"Database connectivity: {e}")
        
        return {"containers": stdout}
    
    def view_logs(self, service: str = "all", follow: bool = False, tail: int = 100):
        """View logs from containers"""
        self.print_header(f"Viewing Logs: {service}")
        
        if service == "all":
            services = ["web", "postgres"]
        else:
            services = [service]
        
        for svc in services:
            print(f"\n{Colors.BOLD}{Colors.MAGENTA}=== {svc.upper()} LOGS ==={Colors.END}")
            
            cmd = ["docker-compose", "logs"]
            if follow:
                cmd.append("-f")
            if tail > 0:
                cmd.extend(["--tail", str(tail)])
            cmd.append(svc)
            
            if follow:
                self.run_command(cmd, capture_output=False)
            else:
                code, stdout, stderr = self.run_command(cmd)
                print(stdout)
                if stderr:
                    print(f"{Colors.RED}STDERR: {stderr}{Colors.END}")
    
    def view_app_logs(self, log_type: str = "experiment", tail: int = 50):
        """View application logs from log files"""
        self.print_header(f"Application Logs: {log_type}")
        
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = self.log_dir / f"{log_type}_{today}.log"
        
        if not log_file.exists():
            self.print_warning(f"Log file not found: {log_file}")
            # Show available log files
            if self.log_dir.exists():
                logs = list(self.log_dir.glob("*.log"))
                if logs:
                    self.print_info("Available log files:")
                    for log in sorted(logs):
                        print(f"  - {log.name}")
            return
        
        self.print_info(f"Reading from: {log_file}")
        
        # Use tail command to get last N lines
        code, stdout, stderr = self.run_command(
            ["tail", "-n", str(tail), str(log_file)]
        )
        
        if code == 0:
            print(stdout)
        else:
            self.print_error(f"Failed to read log file: {stderr}")
    
    def run_test_experiment(self) -> bool:
        """Run a quick test experiment to verify functionality"""
        self.print_header("Running Test Experiment")
        
        # Check if web service is running
        try:
            response = requests.get(f"{self.base_url}/")
            if response.status_code != 200:
                self.print_error("Web service not responding")
                return False
        except requests.exceptions.RequestException:
            self.print_error("Cannot connect to web service")
            return False
        
        self.print_success("Web service is accessible")
        
        # Test database connectivity
        try:
            response = requests.get(f"{self.base_url}/experiment/debug/status")
            data = response.json()
            self.print_success(f"Database status: {len(data.get('experiments', {}))} active experiments")
        except Exception as e:
            self.print_error(f"Database test failed: {e}")
            return False
        
        # Test statistics sources endpoint
        try:
            response = requests.get(f"{self.base_url}/experiment/sources")
            if response.status_code == 200:
                sources = response.json()
                self.print_success(f"Found {len(sources)} statistics sources")
                for source in sources:
                    print(f"  - {source}")
            else:
                self.print_warning(f"Statistics sources endpoint returned {response.status_code}")
        except Exception as e:
            self.print_error(f"Statistics sources test failed: {e}")
        
        return True
    
    def open_shell(self, container: str = "web"):
        """Open shell in container"""
        self.print_info(f"Opening shell in {container} container...")
        self.run_command(
            ["docker-compose", "exec", container, "bash"], 
            capture_output=False
        )
    
    def open_psql(self):
        """Open PostgreSQL shell"""
        self.print_info("Opening PostgreSQL shell...")
        self.run_command(
            ["docker-compose", "exec", "postgres", "psql", "-U", "postgres", "-d", "EXPERIMENT"],
            capture_output=False
        )
    
    def debug_mode(self):
        """Enhanced debug mode with continuous monitoring"""
        self.print_header("Debug Mode - Continuous Monitoring")
        
        self.print_info("Starting debug monitoring...")
        self.print_info("Press Ctrl+C to exit")
        
        try:
            while True:
                print(f"\n{Colors.CYAN}=== {datetime.now().strftime('%H:%M:%S')} ==={Colors.END}")
                
                # Check service status
                try:
                    response = requests.get(f"{self.base_url}/", timeout=2)
                    print(f"Web: {Colors.GREEN}UP{Colors.END} (HTTP {response.status_code})")
                except Exception:
                    print(f"Web: {Colors.RED}DOWN{Colors.END}")
                
                # Check recent logs
                self.view_app_logs("experiment", tail=5)
                
                time.sleep(10)
                
        except KeyboardInterrupt:
            self.print_info("Debug mode stopped")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="AI Test Interface for PostgreSQL Statistics Estimator"
    )
    parser.add_argument("command", 
                       choices=["start", "stop", "restart", "logs", "test", 
                               "debug", "shell", "psql", "status", "clean", "app-logs"],
                       help="Command to execute")
    parser.add_argument("--service", default="all", 
                       help="Service for logs command (web, postgres, all)")
    parser.add_argument("--follow", "-f", action="store_true",
                       help="Follow logs (for logs command)")
    parser.add_argument("--tail", type=int, default=100,
                       help="Number of log lines to show")
    parser.add_argument("--log-type", default="experiment",
                       choices=["experiment", "stats_source"],
                       help="Type of application logs to view")
    
    args = parser.parse_args()
    
    interface = AITestInterface()
    
    # Execute command
    try:
        if args.command == "start":
            success = interface.start_environment()
            sys.exit(0 if success else 1)
            
        elif args.command == "stop":
            success = interface.stop_environment()
            sys.exit(0 if success else 1)
            
        elif args.command == "restart":
            success = interface.restart_environment()
            sys.exit(0 if success else 1)
            
        elif args.command == "clean":
            success = interface.clean_restart()
            sys.exit(0 if success else 1)
            
        elif args.command == "logs":
            interface.view_logs(args.service, args.follow, args.tail)
            
        elif args.command == "app-logs":
            interface.view_app_logs(args.log_type, args.tail)
            
        elif args.command == "test":
            success = interface.run_test_experiment()
            sys.exit(0 if success else 1)
            
        elif args.command == "debug":
            interface.debug_mode()
            
        elif args.command == "shell":
            interface.open_shell()
            
        elif args.command == "psql":
            interface.open_psql()
            
        elif args.command == "status":
            interface.get_status()
            
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted by user{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")
        sys.exit(1)

if __name__ == "__main__":
    main()