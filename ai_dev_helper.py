#!/usr/bin/env python3
"""
AI Development Helper for PostgreSQL Statistics Estimator

This is the main entry point for AI models to develop, test, and debug the
experiment platform. It combines all AI tools and provides a unified interface
for iterative development.

Usage:
    python ai_dev_helper.py <action> [options]

Actions:
    setup          - Initial setup and environment check
    dev-cycle      - Complete development cycle (build, test, analyze)
    quick-test     - Quick functionality test
    deep-debug     - Comprehensive debugging session
    monitor        - Continuous monitoring mode
    experiment     - Run a test experiment
    fix-common     - Auto-fix common issues

Author: AI Assistant
Created: 2025
"""

import sys
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
# Import our AI tools - using fallback if modules not available
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    print("Warning: requests module not available, some features disabled")

try:
    from ai_test_interface import AITestInterface
    from ai_log_analyzer import AILogAnalyzer
    AI_TOOLS_AVAILABLE = True
except ImportError:
    AI_TOOLS_AVAILABLE = False
    print("Warning: AI tools not available, using basic mode")

class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    END = '\033[0m'

class AIDevHelper:
    """Main AI development helper"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        
        if AI_TOOLS_AVAILABLE:
            self.test_interface = AITestInterface()
            self.log_analyzer = AILogAnalyzer()
        else:
            self.test_interface = None
            self.log_analyzer = None
        
    def print_banner(self):
        """Print welcome banner"""
        banner = f"""
{Colors.CYAN}{Colors.BOLD}
╔══════════════════════════════════════════════════════════════╗
║                   AI Development Helper                      ║
║            PostgreSQL Statistics Estimator                  ║
╚══════════════════════════════════════════════════════════════╝
{Colors.END}

Welcome, AI! This tool helps you develop and debug the experiment platform.
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        print(banner)
    
    def setup(self) -> bool:
        """Initial setup and environment verification"""
        self.print_banner()
        print(f"{Colors.BOLD}🚀 Initial Setup{Colors.END}")
        
        # Check dependencies
        print("\n1. Checking dependencies...")
        deps = ["docker", "docker-compose", "python", "curl"]
        
        for dep in deps:
            try:
                result = subprocess.run([dep, "--version"], 
                                     capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"   ✓ {dep}: Available")
                else:
                    print(f"   ✗ {dep}: Not found")
                    return False
            except FileNotFoundError:
                print(f"   ✗ {dep}: Not found")
                return False
        
        # Check project structure
        print("\n2. Verifying project structure...")
        required_paths = [
            "docker-compose.yml",
            "app/Dockerfile",
            "app/app/main.py",
            "app/app/services/experiment_runner.py"
        ]
        
        for path in required_paths:
            if (self.project_root / path).exists():
                print(f"   ✓ {path}")
            else:
                print(f"   ✗ {path}: Missing")
                return False
        
        print(f"\n{Colors.GREEN}✓ Setup complete! Ready for development.{Colors.END}")
        return True
    
    def dev_cycle(self) -> bool:
        """Complete development cycle"""
        print(f"{Colors.BOLD}🔄 Development Cycle{Colors.END}")
        
        stages = [
            ("Environment Start", self.test_interface.start_environment),
            ("Health Check", self._health_check),
            ("Quick Test", self.test_interface.run_test_experiment),
            ("Log Analysis", self._analyze_recent_logs),
            ("Status Summary", self._print_status_summary)
        ]
        
        for stage_name, stage_func in stages:
            print(f"\n{Colors.CYAN}► {stage_name}{Colors.END}")
            
            try:
                success = stage_func()
                if success:
                    print(f"  {Colors.GREEN}✓ {stage_name} completed{Colors.END}")
                else:
                    print(f"  {Colors.RED}✗ {stage_name} failed{Colors.END}")
                    return False
            except Exception as e:
                print(f"  {Colors.RED}✗ {stage_name} error: {e}{Colors.END}")
                return False
        
        print(f"\n{Colors.GREEN}🎉 Development cycle completed successfully!{Colors.END}")
        return True
    
    def quick_test(self) -> bool:
        """Quick functionality test"""
        print(f"{Colors.BOLD}⚡ Quick Test{Colors.END}")
        
        # Start if not running
        if not self._is_running():
            print("Starting environment...")
            if not self.test_interface.start_environment():
                return False
        
        # Run basic tests
        return self.test_interface.run_test_experiment()
    
    def deep_debug(self):
        """Comprehensive debugging session"""
        print(f"{Colors.BOLD}🔍 Deep Debug Session{Colors.END}")
        
        # Generate comprehensive report
        print("\n1. Generating debug report...")
        report_file = self.log_analyzer.generate_debug_report(hours_back=6)
        
        # Health check
        print("\n2. Health check...")
        health = self.log_analyzer.quick_health_check()
        
        # Container logs
        print("\n3. Recent container logs...")
        self.test_interface.view_logs("all", follow=False, tail=20)
        
        # Application logs
        print("\n4. Recent application logs...")
        self.test_interface.view_app_logs("experiment", tail=20)
        
        # Common issues check
        print("\n5. Checking for common issues...")
        self._check_common_issues()
        
        print(f"\n{Colors.GREEN}🔍 Debug session complete. Report: {report_file}{Colors.END}")
    
    def monitor_mode(self):
        """Continuous monitoring mode"""
        print(f"{Colors.BOLD}📊 Monitor Mode{Colors.END}")
        print("Entering continuous monitoring. Press Ctrl+C to exit.")
        
        try:
            self.log_analyzer.stream_logs_realtime(follow_experiments=True)
        except KeyboardInterrupt:
            print(f"\n{Colors.YELLOW}Monitor mode stopped{Colors.END}")
    
    def run_experiment(self) -> bool:
        """Run a complete test experiment"""
        print(f"{Colors.BOLD}🧪 Running Test Experiment{Colors.END}")
        
        if not self._is_running():
            print("Starting environment...")
            if not self.test_interface.start_environment():
                return False
        
        # Use simple query for testing
        test_query = "SELECT COUNT(*) FROM pg_stat_user_tables;"
        
        # Submit experiment via API (simplified)
        try:
            # This would need to be implemented based on actual API
            print("Setting up test experiment...")
            print(f"Query: {test_query}")
            print("This would submit an actual experiment through the API")
            
            # For now, just verify the API is accessible
            response = requests.get(f"{self.test_interface.base_url}/experiment/sources")
            if response.status_code == 200:
                sources = response.json()
                print(f"Available sources: {sources}")
                return True
            else:
                print(f"API error: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Experiment failed: {e}")
            return False
    
    def fix_common_issues(self) -> bool:
        """Auto-fix common issues"""
        print(f"{Colors.BOLD}🔧 Auto-fixing Common Issues{Colors.END}")
        
        fixed_count = 0
        
        # Check if containers are stopped
        if not self._is_running():
            print("Issue: Containers not running")
            print("Fix: Starting containers...")
            if self.test_interface.start_environment():
                print("  ✓ Containers started")
                fixed_count += 1
            else:
                print("  ✗ Failed to start containers")
        
        # Check for stale containers
        print("Checking for stale containers...")
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", "status=exited"],
            capture_output=True, text=True
        )
        
        if "postgres" in result.stdout or "web" in result.stdout:
            print("Issue: Stale containers found")
            print("Fix: Cleaning up...")
            subprocess.run(["docker-compose", "down"], 
                         cwd=self.project_root, capture_output=True)
            time.sleep(2)
            if self.test_interface.start_environment():
                print("  ✓ Containers restarted")
                fixed_count += 1
        
        # Check log file permissions
        log_dir = self.project_root / "app" / "app" / "logs"
        if log_dir.exists():
            try:
                test_file = log_dir / "test_write.tmp"
                test_file.write_text("test")
                test_file.unlink()
                print("  ✓ Log directory writable")
            except Exception:
                print("Issue: Log directory not writable")
                print("Fix: Adjusting permissions...")
                subprocess.run(["chmod", "-R", "755", str(log_dir)])
                fixed_count += 1
        
        print(f"\n{Colors.GREEN}🔧 Fixed {fixed_count} issues{Colors.END}")
        return fixed_count > 0
    
    def _is_running(self) -> bool:
        """Check if services are running"""
        try:
            response = requests.get(f"{self.test_interface.base_url}/", timeout=2)
            return response.status_code == 200
        except:
            return False
    
    def _health_check(self) -> bool:
        """Simple health check"""
        health = self.log_analyzer.quick_health_check()
        
        web_ok = health.get("web_service", {}).get("status") == "ok"
        containers_ok = health.get("containers", {}).get("status") == "running"
        
        return web_ok and containers_ok
    
    def _analyze_recent_logs(self) -> bool:
        """Analyze recent logs for issues"""
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = self.log_analyzer.log_dir / f"experiment_{today}.log"
        
        if not log_file.exists():
            print("  No recent logs found")
            return True
        
        analysis = self.log_analyzer.analyze_log_file(log_file, hours_back=1)
        
        if "error" in analysis:
            print(f"  Log analysis error: {analysis['error']}")
            return False
        
        error_count = analysis.get("summary", {}).get("errors", 0)
        if error_count > 0:
            print(f"  ⚠️  Found {error_count} recent errors")
            
            # Show recent errors
            for error in analysis.get("recent_errors", [])[:3]:
                print(f"    - {error['timestamp']}: {error['message'][:100]}...")
        else:
            print("  ✓ No recent errors")
        
        return error_count == 0
    
    def _print_status_summary(self) -> bool:
        """Print overall status summary"""
        print(f"\n{Colors.BOLD}📋 Status Summary{Colors.END}")
        
        # Services
        web_status = "🟢 UP" if self._is_running() else "🔴 DOWN"
        print(f"  Web Service: {web_status}")
        
        # Recent activity
        health = self.log_analyzer.quick_health_check()
        error_count = len(health.get("recent_errors", []))
        error_status = "🟢 CLEAN" if error_count == 0 else f"🟡 {error_count} ERRORS"
        print(f"  Recent Errors: {error_status}")
        
        # Log files
        log_count = sum(1 for info in health.get("log_files", {}).values() 
                       if info.get("exists", False))
        print(f"  Log Files: 📄 {log_count} active")
        
        return True
    
    def _check_common_issues(self):
        """Check for common development issues"""
        issues = []
        
        # Check Docker
        try:
            result = subprocess.run(["docker", "ps"], capture_output=True)
            if result.returncode != 0:
                issues.append("Docker daemon not running")
        except FileNotFoundError:
            issues.append("Docker not installed")
        
        # Check ports
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 8000))
            if result != 0:
                issues.append("Port 8000 not accessible")
            sock.close()
        except Exception:
            issues.append("Network connectivity issue")
        
        # Check disk space
        import shutil
        total, used, free = shutil.disk_usage(self.project_root)
        free_gb = free / (1024**3)
        if free_gb < 1:
            issues.append(f"Low disk space: {free_gb:.1f}GB free")
        
        if issues:
            print(f"  {Colors.YELLOW}Found issues:{Colors.END}")
            for issue in issues:
                print(f"    - {issue}")
        else:
            print(f"  {Colors.GREEN}No common issues detected{Colors.END}")

def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description="AI Development Helper")
    parser.add_argument("action", 
                       choices=["setup", "dev-cycle", "quick-test", "deep-debug", 
                               "monitor", "experiment", "fix-common"],
                       help="Action to perform")
    
    args = parser.parse_args()
    
    helper = AIDevHelper()
    
    try:
        if args.action == "setup":
            success = helper.setup()
            sys.exit(0 if success else 1)
            
        elif args.action == "dev-cycle":
            success = helper.dev_cycle()
            sys.exit(0 if success else 1)
            
        elif args.action == "quick-test":
            success = helper.quick_test()
            sys.exit(0 if success else 1)
            
        elif args.action == "deep-debug":
            helper.deep_debug()
            
        elif args.action == "monitor":
            helper.monitor_mode()
            
        elif args.action == "experiment":
            success = helper.run_experiment()
            sys.exit(0 if success else 1)
            
        elif args.action == "fix-common":
            success = helper.fix_common_issues()
            sys.exit(0 if success else 1)
            
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted by user{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")
        sys.exit(1)

if __name__ == "__main__":
    main()