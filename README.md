# PostgreSQL Statistics Benchmarking Platform

This platform reveals how different statistical methods affect PostgreSQL query performance. Upload your database and queries, then compare traditional statistics against AI-generated ones to discover which approach works best for your workload.

## What This Does

PostgreSQL needs statistics to create efficient query plans. Bad statistics mean slow queries.

This platform tests different ways of generating those statistics:
- Traditional PostgreSQL ANALYZE
- AI-powered statistical estimation 
- Custom statistical methods

You get direct performance comparisons showing which method produces the fastest queries for your specific database.

## Quick Start

**Requirements:** Docker and Docker Compose

**Setup:**
1. Copy the environment file:
   ```bash
   cp .env.example .env
   ```
2. Edit `.env` with your API keys (optional for basic functionality)

**Run:**
```bash
./start.sh
```

**Access:** [http://localhost:8000](http://localhost:8000)

**Stop:**
```bash
docker-compose down
```

**Reset everything:**
```bash
./reset.sh
```

## How It Works

**1. Upload Your Database**
Upload a `.sql` or `.dump` file containing your schema and data.

**2. Upload Your Queries**
Upload `.sql` files with the queries you want to benchmark.

**3. Run Experiment**
- Choose which queries to test
- Select which statistical methods to compare
- Run the experiment

**4. See Results**
View detailed performance comparisons and execution time visualizations.

## Adding New Statistical Methods

Create custom statistical methods by building new "Statistics Sources".

**1. Create Directory Structure**
```
app/app/src/my_method/
├── my_method.py
└── config/
    └── default.yaml
```

**2. Implement the Class**
```python
from ..base import StatsSource

class MyMethodStatsSource(StatsSource):
    def name(self) -> str:
        return "my_method"
    
    def apply_statistics(self, session: Session) -> None:
        self.clear_caches(session)
        # Your statistical logic here
        session.execute(text("ANALYZE;"))
        session.commit()
```

**3. Add Configuration**
```yaml
name: default
description: "My custom statistics method"
settings:
    analyze_verbose: true
    analyze_timeout_seconds: 300
```

**4. Register the Source**
Add import to `app/app/src/__init__.py`:
```python
from .my_method.my_method import MyMethodStatsSource
```

Your method will appear in the experiment interface automatically.

## Built-in Statistical Methods

**Default PostgreSQL**: Standard `ANALYZE` command
**AI-Powered**: Uses machine learning to estimate statistics (requires AI API)
**Empty Statistics**: Removes all statistics to test baseline performance
**Custom Methods**: Add your own statistical approaches

## Architecture

- **FastAPI** backend with web interface
- **PostgreSQL 15** database
- **Docker** containerization for consistent environments
- **Extensible** plugin system for new statistical methods 