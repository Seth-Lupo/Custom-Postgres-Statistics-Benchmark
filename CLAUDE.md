# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a PostgreSQL query performance benchmarking platform that compares different methods of generating table statistics and their impact on query execution plans. It uses a plugin architecture to support multiple statistics sources including built-in PostgreSQL statistics, empty statistics, and AI-based statistics generation.

## Development Commands

### Starting the Development Environment
```bash
# Start the application (builds and runs in foreground)
./start.sh

# Start in detached mode
docker-compose up --build -d

# View logs
docker-compose logs -f web       # Web application logs
docker-compose logs -f postgres  # Database logs

# Access containers
docker-compose exec web bash
docker-compose exec postgres psql -U postgres -d EXPERIMENT

# Reset everything (clean start - removes all data)
./reset.sh
```

### Common Development Tasks
```bash
# Install new Python dependencies
# 1. Add to app/requirements.txt
# 2. Rebuild: docker-compose build web

# Access the application
# Browser: http://localhost:8000
# Database: localhost:5432 (user: postgres, password: postgres)

# View experiment logs
docker-compose exec web tail -f app/logs/experiment.log
```

## Architecture

### Plugin System for Statistics Sources
The core extensibility mechanism uses an abstract base class pattern:

1. **Base Class**: `app/app/src/base.py` defines `StatsSource` ABC
2. **Implementations**: Each source in `app/app/src/{source_name}/`
   - `default/` - Built-in PostgreSQL statistics
   - `empty_pg_stats/` - Empty statistics for testing
   - `schneider_ai/` - AI-based statistics generation using OpenAI

### Service Layer Architecture
- **ExperimentRunner**: Main orchestration (`app/app/services/experiment_runner.py`)
- **TrialExecutor**: Individual trial execution
- **StatisticsCapture**: Collects query statistics
- **ProgressTracker**: Real-time progress updates
- **ExperimentValidator**: Input validation

### Configuration System
Two-tier configuration:
- **Settings**: PostgreSQL runtime parameters (`app/app/src/settings/`)
- **Config**: Plugin-specific configuration (`app/app/src/{source}/config/`)

### Key Directories
- `app/app/routers/` - FastAPI route handlers
- `app/app/services/` - Business logic services
- `app/app/models/` - SQLModel database models
- `app/app/templates/` - Jinja2 HTML templates
- `app/app/static/` - JavaScript and generated visualizations
- `app/uploads/` - User uploaded dumps and queries
- `init-scripts/` - PostgreSQL initialization SQL

### Database Access Pattern
The project uses SQLModel (SQLAlchemy + Pydantic). Database sessions are managed via dependency injection:
```python
from app.database import get_db
from sqlmodel import Session

async def endpoint(db: Session = Depends(get_db)):
    # Use db session
```

### Experiment Workflow
1. Upload database dump → `POST /experiment/upload_dump`
2. Upload queries → `POST /experiment/upload_queries`
3. Configure experiment → `POST /experiment/configure`
4. Run trials → `POST /experiment/run` (background task)
5. Monitor progress → WebSocket streaming
6. View results → Generated Plotly visualizations

### Logging System
- Separate loggers for different components
- Custom streaming handler for frontend integration
- Logs stored in `app/app/logs/` with 30-day retention
- Real-time log streaming via WebSocket

### Error Handling
Custom exception hierarchy in `app/app/exceptions.py`:
- `ExperimentError` - Base exception
- `StatsApplicationError`, `QueryExecutionError`, etc.

## Code Conventions

- **Type Hints**: Use throughout the codebase
- **Docstrings**: Document all functions and classes
- **Imports**: Group by stdlib → third-party → local
- **Naming**: snake_case for functions/variables, PascalCase for classes
- **Line Length**: Keep under 120 characters
- **FastAPI Dependencies**: Use dependency injection pattern

## Adding New Statistics Sources

1. Create directory: `app/app/src/new_source/`
2. Implement `StatsSource` abstract class
3. Add configuration in `config/` subdirectory
4. Import in `app/app/src/__init__.py`
5. The source will auto-register and appear in the UI

## Important Notes

- Hot reload is enabled in development (code changes apply immediately)
- PostgreSQL 15 is used with custom initialization scripts
- No formal test suite exists - testing is manual
- Environment variables are set in `docker-compose.yml`
- The `EXPERIMENT` database is created on startup
- Each experiment run creates a temporary database