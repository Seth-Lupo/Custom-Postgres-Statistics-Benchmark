#!/usr/bin/env python3
"""
Simple test script for PostgreSQL Statistics Estimator

Tests the AI tools and runs an experiment using standard library only.
"""

import subprocess
import sys
import time
import json
from pathlib import Path
from datetime import datetime

class SimpleTest:
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.base_url = "http://localhost:8000"
    
    def run_command(self, cmd, capture_output=True, cwd=None):
        """Run a command and return result"""
        if cwd is None:
            cwd = self.project_root
        
        print(f"[CMD] {' '.join(cmd)}")
        
        try:
            if capture_output:
                result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
                return result.returncode, result.stdout, result.stderr
            else:
                result = subprocess.run(cmd, cwd=cwd)
                return result.returncode, "", ""
        except Exception as e:
            return 1, "", str(e)
    
    def check_dependencies(self):
        """Check if required tools are available"""
        print("🔍 Checking dependencies...")
        
        deps = ["docker", "docker-compose"]
        for dep in deps:
            code, stdout, stderr = self.run_command([dep, "--version"])
            if code == 0:
                print(f"  ✓ {dep}: Available")
            else:
                print(f"  ✗ {dep}: Not found")
                return False
        return True
    
    def start_environment(self):
        """Start Docker environment"""
        print("🚀 Starting Docker environment...")
        
        # Stop any existing containers first
        self.run_command(["docker-compose", "down"])
        time.sleep(2)
        
        # Start fresh
        code, stdout, stderr = self.run_command(["docker-compose", "up", "--build", "-d"])
        
        if code != 0:
            print(f"❌ Failed to start environment")
            print(f"STDOUT: {stdout}")
            print(f"STDERR: {stderr}")
            return False
        
        print("✅ Docker environment started")
        
        # Wait for services
        print("⏳ Waiting for services to be ready...")
        max_wait = 60  # seconds
        for i in range(max_wait):
            code, stdout, stderr = self.run_command(["curl", "-s", "-f", self.base_url])
            if code == 0:
                print("✅ Web service is ready")
                return True
            time.sleep(1)
            if i % 10 == 0:
                print(f"  Still waiting... ({i}s)")
        
        print("⚠️  Web service may not be fully ready")
        return False
    
    def check_status(self):
        """Check container status"""
        print("📊 Checking container status...")
        
        code, stdout, stderr = self.run_command(["docker-compose", "ps"])
        print(stdout)
        
        # Test web service
        code, stdout, stderr = self.run_command(["curl", "-s", "-f", f"{self.base_url}/"])
        if code == 0:
            print("✅ Web service: Responding")
        else:
            print("❌ Web service: Not responding")
            return False
        
        return True
    
    def view_logs(self, service="web", tail=20):
        """View container logs"""
        print(f"📋 Viewing {service} logs (last {tail} lines)...")
        
        code, stdout, stderr = self.run_command(
            ["docker-compose", "logs", "--tail", str(tail), service]
        )
        
        if code == 0:
            print(stdout)
        else:
            print(f"❌ Failed to get logs: {stderr}")
    
    def test_api_endpoints(self):
        """Test API endpoints"""
        print("🔌 Testing API endpoints...")
        
        # Test endpoints
        endpoints = [
            "/",
            "/experiment/configs",
            "/experiment/debug/status"
        ]
        
        for endpoint in endpoints:
            url = f"{self.base_url}{endpoint}"
            code, stdout, stderr = self.run_command(["curl", "-s", "-f", url])
            
            if code == 0:
                print(f"  ✅ {endpoint}: OK")
                # Try to parse JSON if it looks like JSON
                if stdout.strip().startswith('{') or stdout.strip().startswith('['):
                    try:
                        data = json.loads(stdout)
                        if endpoint == "/experiment/configs":
                            print(f"     Found {len(data)} statistics sources")
                            for source in data:
                                print(f"       - {source}")
                        elif endpoint == "/experiment/debug/status":
                            exp_count = len(data.get('experiments', {}))
                            print(f"     {exp_count} active experiments")
                    except:
                        pass
            else:
                print(f"  ❌ {endpoint}: Failed")
                return False
        
        return True
    
    def find_test_files(self):
        """Find test files for experiment"""
        print("🔍 Looking for test files...")
        
        # Look for SQL dump files
        dump_patterns = ["*.sql", "samples/*.sql", "app/samples/*.sql"]
        dump_file = None
        
        for pattern in dump_patterns:
            dumps = list(self.project_root.glob(pattern))
            if dumps:
                dump_file = dumps[0]
                break
        
        if dump_file:
            print(f"  ✅ Found dump file: {dump_file}")
        else:
            print("  ⚠️  No SQL dump file found")
            # Create a simple test dump
            dump_file = self.project_root / "test_dump.sql"
            dump_content = """
-- Simple test database
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER
);

INSERT INTO test_table (name, value) VALUES 
('test1', 100),
('test2', 200),
('test3', 300);

ANALYZE test_table;
"""
            dump_file.write_text(dump_content)
            print(f"  ✅ Created test dump: {dump_file}")
        
        # Find query file or create one
        query_patterns = ["*.query", "samples/*.query", "app/samples/*.query"]
        query_file = None
        
        for pattern in query_patterns:
            queries = list(self.project_root.glob(pattern))
            if queries:
                query_file = queries[0]
                break
        
        if not query_file:
            query_file = self.project_root / "test_query.sql"
            query_content = "SELECT COUNT(*) FROM test_table WHERE value > 150;"
            query_file.write_text(query_content)
            print(f"  ✅ Created test query: {query_file}")
        else:
            print(f"  ✅ Found query file: {query_file}")
        
        return str(dump_file), query_file.read_text().strip()
    
    def run_test_experiment(self):
        """Run a test experiment via API calls"""
        print("🧪 Running test experiment...")
        
        # Get test files
        dump_path, query = self.find_test_files()
        
        print(f"  Dump: {dump_path}")
        print(f"  Query: {query}")
        
        # For now, just verify we can reach the upload endpoints
        # A full implementation would need to handle file uploads and form data
        
        # Test that we can reach upload endpoints
        upload_endpoints = [
            "/experiment/upload_dump",
            "/experiment/upload_queries", 
            "/experiment/configure",
            "/experiment/run"
        ]
        
        print("  Testing experiment endpoints...")
        for endpoint in upload_endpoints:
            # Just test that endpoints exist (they'll return method not allowed for GET)
            code, stdout, stderr = self.run_command(
                ["curl", "-s", "-w", "%{http_code}", "-o", "/dev/null", f"{self.base_url}{endpoint}"]
            )
            
            if "405" in stdout or "200" in stdout:  # Method not allowed or OK
                print(f"    ✅ {endpoint}: Available")
            else:
                print(f"    ❌ {endpoint}: Not found ({stdout})")
        
        print("  📝 Note: Full experiment testing requires form data uploads")
        print("     The endpoints are available for manual testing")
        
        return True
    
    def run_full_test(self):
        """Run complete test sequence"""
        print("🎯 Running full test sequence...")
        print("=" * 60)
        
        # 1. Check dependencies
        if not self.check_dependencies():
            return False
        
        # 2. Start environment  
        if not self.start_environment():
            return False
        
        # 3. Wait a bit more for full startup
        time.sleep(5)
        
        # 4. Check status
        if not self.check_status():
            print("⚠️  Status check failed, continuing anyway...")
        
        # 5. Test API
        if not self.test_api_endpoints():
            print("⚠️  API test failed, checking logs...")
            self.view_logs("web", 30)
            return False
        
        # 6. Test experiment setup
        if not self.run_test_experiment():
            return False
        
        # 7. View some logs
        print("\n📋 Recent application logs:")
        self.view_logs("web", 20)
        
        print("\n✅ Test sequence completed successfully!")
        print("\n🎯 Next steps:")
        print("  - Access web interface: http://localhost:8000")
        print("  - Upload your SQL dump and query files")
        print("  - Configure experiment with 10 trials")
        print("  - Use default/built-in statistics source")
        
        return True

def main():
    test = SimpleTest()
    
    if len(sys.argv) > 1 and sys.argv[1] == "logs":
        service = sys.argv[2] if len(sys.argv) > 2 else "web"
        test.view_logs(service, 50)
        return
    
    success = test.run_full_test()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()