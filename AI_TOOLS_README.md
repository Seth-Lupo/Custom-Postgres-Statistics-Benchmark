# AI Development Tools for PostgreSQL Statistics Estimator

This directory contains specialized tools designed for AI models to develop, test, and debug the experiment platform efficiently. These tools provide comprehensive logging, error analysis, and iterative development capabilities.

## 🚀 Quick Start for AI Models

```bash
# Initial setup and verification
python ai_dev_helper.py setup

# Complete development cycle (recommended)
python ai_dev_helper.py dev-cycle

# Quick functionality test
python ai_dev_helper.py quick-test
```

## 🛠️ Tools Overview

### 1. `ai_dev_helper.py` - Main AI Interface
The primary entry point for AI development. Combines all tools into a unified workflow.

**Key Commands:**
- `setup` - Initial environment verification
- `dev-cycle` - Complete build-test-analyze cycle
- `quick-test` - Fast functionality verification
- `deep-debug` - Comprehensive debugging session
- `monitor` - Real-time log monitoring
- `experiment` - Run test experiment
- `fix-common` - Auto-fix common issues

### 2. `ai_test_interface.py` - Docker & Service Management
Manages Docker environment and provides testing capabilities.

**Key Commands:**
- `start` - Start Docker environment
- `stop/restart` - Manage containers
- `logs` - View container logs
- `status` - Check service health
- `shell` - Access container shells
- `clean` - Clean restart (removes data)

### 3. `ai_log_analyzer.py` - Advanced Log Analysis
Provides intelligent log parsing, error classification, and pattern detection.

**Key Commands:**
- `analyze` - Analyze log files
- `stream` - Real-time log streaming
- `health` - Quick health check
- `report` - Generate debug reports

## 📋 Common Development Workflows

### Initial Development Setup
```bash
# 1. Verify environment
python ai_dev_helper.py setup

# 2. Start services
python ai_test_interface.py start

# 3. Verify functionality
python ai_test_interface.py test
```

### Debugging Failed Experiments
```bash
# 1. Generate debug report
python ai_log_analyzer.py report --hours 6

# 2. View real-time logs
python ai_log_analyzer.py stream

# 3. Check specific container logs
python ai_test_interface.py logs --service web --tail 50
```

### Continuous Development
```bash
# Monitor logs while developing
python ai_dev_helper.py monitor

# Or use the complete cycle
python ai_dev_helper.py dev-cycle
```

## 🔍 Error Analysis & Debugging

### Automated Error Classification
The log analyzer automatically classifies errors into categories:

- **OPENAI_API_ERROR** - API authentication/rate limit issues
- **QUERY_TIMEOUT** - Database query timeouts
- **DATABASE_CONNECTION_ERROR** - PostgreSQL connectivity issues
- **STATS_APPLICATION_ERROR** - Statistics generation failures
- **CONFIG_PARSING_ERROR** - YAML/configuration issues

### Real-time Error Monitoring
```bash
# Stream logs with error classification
python ai_log_analyzer.py stream

# Health check with error summary
python ai_log_analyzer.py health
```

### Debug Reports
Comprehensive JSON reports include:
- Error patterns and frequencies
- Experiment timelines
- Performance metrics
- Container health status
- Recommendations for fixes

## 📊 Log Files & Locations

The system generates several types of logs:

### Application Logs (in `app/app/logs/`)
- `experiment_YYYY-MM-DD.log` - Main experiment execution
- `stats_source_YYYY-MM-DD.log` - Statistics source operations

### Container Logs
- `docker-compose logs web` - Web application
- `docker-compose logs postgres` - Database

### Access Methods
```bash
# Application logs (parsed)
python ai_test_interface.py app-logs --log-type experiment

# Container logs (raw)
python ai_test_interface.py logs --service web

# Real-time streaming
python ai_log_analyzer.py stream
```

## 🔧 Common Issues & Solutions

### Environment Issues
| Issue | Command | Description |
|-------|---------|-------------|
| Containers not starting | `python ai_dev_helper.py fix-common` | Auto-fixes container issues |
| Port conflicts | `python ai_test_interface.py clean` | Clean restart |
| Permission errors | `chmod -R 755 app/app/logs` | Fix log permissions |

### API Issues
| Issue | Check | Solution |
|-------|-------|----------|
| OpenAI API errors | Environment variables | Set `OPENAI_API_KEY` |
| Database connection | `python ai_test_interface.py status` | Restart PostgreSQL container |
| Web service down | Container logs | Check application errors |

### Development Issues
| Issue | Tool | Command |
|-------|------|---------|
| Code changes not reflected | Hot reload | `python ai_test_interface.py restart` |
| Experiment failures | Debug analysis | `python ai_dev_helper.py deep-debug` |
| Performance issues | Log analysis | `python ai_log_analyzer.py analyze --hours 24` |

## 🎯 AI Development Best Practices

### 1. Always Start with Setup
```bash
python ai_dev_helper.py setup
```

### 2. Use the Development Cycle
For comprehensive testing:
```bash
python ai_dev_helper.py dev-cycle
```

### 3. Monitor Logs During Development
```bash
# Terminal 1: Code changes
# Terminal 2: Log monitoring
python ai_dev_helper.py monitor
```

### 4. Generate Reports for Complex Issues
```bash
python ai_log_analyzer.py report --hours 6
```

### 5. Clean Restart When Needed
```bash
python ai_test_interface.py clean
```

## 📈 Performance Monitoring

### Real-time Metrics
- Container resource usage
- Database connection status
- API response times
- Error rates and patterns

### Historical Analysis
- Experiment success/failure rates
- Performance trends
- Error pattern evolution
- Resource utilization over time

## 🔄 Iterative Development Process

1. **Make Code Changes**
2. **Run Quick Test**: `python ai_dev_helper.py quick-test`
3. **Check Logs**: `python ai_log_analyzer.py stream`
4. **Debug Issues**: `python ai_dev_helper.py deep-debug`
5. **Fix and Repeat**

## 🆘 Emergency Procedures

### Complete System Reset
```bash
python ai_test_interface.py stop
docker system prune -f
python ai_dev_helper.py setup
python ai_test_interface.py start
```

### Data Recovery
```bash
# Backup logs before cleanup
cp -r app/app/logs/ logs_backup_$(date +%Y%m%d_%H%M%S)/

# Generate final report
python ai_log_analyzer.py report --hours 48
```

## 📞 Getting Help

### Debug Information Collection
```bash
# Comprehensive debug report
python ai_dev_helper.py deep-debug

# System status
python ai_test_interface.py status
python ai_log_analyzer.py health
```

### Log Analysis
```bash
# Recent errors
python ai_log_analyzer.py analyze --hours 6

# Full system health
python ai_dev_helper.py dev-cycle
```

---

**Note**: These tools are specifically designed for AI models to efficiently develop and debug the experiment platform. They provide comprehensive error reporting, automated issue detection, and streamlined development workflows.