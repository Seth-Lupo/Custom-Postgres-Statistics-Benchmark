# PostgreSQL Statistics Benchmarking Platform

A reproducible, Docker-based benchmarking platform that uses FastAPI and HTMX to compare PostgreSQL query performance with different statistics sources.

## Features

- **Docker-based**: Fully containerized setup with PostgreSQL and web application
- **HTMX-powered UI**: Modern, responsive web interface with dynamic interactions
- **Multiple Statistics Sources**: Compare built-in vs. random PostgreSQL statistics
- **Comprehensive Benchmarking**: Run multiple iterations with detailed timing and cost analysis
- **Visual Analytics**: Interactive charts (bar, line, histogram) using Matplotlib
- **Real-time Progress**: Live experiment tracking with progress bars
- **Detailed Results**: Per-trial results with statistical analysis

## Architecture

- **Backend**: FastAPI with SQLAlchemy and PostgreSQL
- **Frontend**: HTMX + Bootstrap for declarative, AJAX-style interactions
- **Database**: PostgreSQL 15 with custom statistics manipulation
- **Charts**: Matplotlib-generated PNG images served dynamically
- **Deployment**: Docker Compose for easy setup and reproducibility

## Quick Start

1. **Clone and Start Services**
   ```bash
   git clone <repository-url>
   cd llm-pg-statistics-estimator
   docker-compose up --build
   ```

2. **Access the Platform**
   - Open http://localhost:8000 in your browser
   - Use the sample data or upload your own files

3. **Run Your First Experiment**
   - Navigate to "Upload Files" to add database dump and queries
   - Go to "Run Experiment" to configure and start benchmarking
   - View results with detailed charts and statistics

## Usage Workflow

### 1. Upload Files
- **Database Dump**: PostgreSQL dump file (.sql or .dump)
- **Queries File**: SQL queries separated by semicolons (.sql)

Sample files are included in the `samples/` directory.

### 2. Configure Experiment
- **Statistics Source**: Choose between built-in or random statistics
- **Iterations**: Number of times to execute each query (1-100)

### 3. Monitor Progress
- Real-time progress tracking with HTMX polling
- Background execution without blocking the UI

### 4. Analyze Results
- Summary statistics (mean, standard deviation)
- Individual trial results with relative performance
- Interactive charts (bar, line, histogram)
- Detailed query execution plans and costs

## Statistics Sources

### Built-in PostgreSQL Statistics
- Uses native PostgreSQL ANALYZE command
- Leverages actual data distribution statistics
- Baseline for comparison

### Random PostgreSQL Statistics
- Applies random statistics values to all columns
- Tests query planner behavior with different statistics
- Useful for studying planner sensitivity

## API Endpoints

- `GET /` - Home page with navigation
- `GET /upload` - File upload interface
- `POST /upload/dump` - Database dump upload
- `POST /upload/queries` - Queries file upload
- `GET /experiment` - Experiment configuration
- `POST /experiment` - Start experiment
- `GET /experiment/status/{id}` - Experiment progress
- `GET /results` - All experiment results
- `GET /results/{id}` - Detailed experiment view
- `GET /results/{id}/chart` - Generate chart visualization

## Development

### Local Development
```bash
cd app
poetry install
poetry shell
uvicorn app.main:app --reload
```

### Database Setup
```bash
# Connect to PostgreSQL
docker exec -it <postgres-container> psql -U postgres -d experiment

# Load sample data
\i /docker-entrypoint-initdb.d/dump.sql
```

### Testing
```bash
cd app
poetry run pytest
```

## Configuration

Environment variables:
- `DATABASE_URL`: PostgreSQL connection string
- Default: `postgresql+psycopg2://postgres:postgres@postgres:5432/experiment`

## File Structure

```
├── docker-compose.yml          # Docker services definition
├── samples/                    # Sample data files
│   ├── dump.sql               # Sample database dump
│   └── queries.sql            # Sample SQL queries
├── app/                       # FastAPI application
│   ├── Dockerfile             # Application container
│   ├── pyproject.toml         # Python dependencies
│   └── app/                   # Application code
│       ├── main.py            # FastAPI app with routes
│       ├── database.py        # Database configuration
│       ├── models.py          # SQLAlchemy models
│       ├── schemas.py         # Pydantic schemas
│       ├── experiment.py      # Experiment runner
│       ├── stats_sources/     # Statistics source implementations
│       ├── routers/           # API route handlers
│       ├── templates/         # Jinja2 HTML templates
│       └── static/            # Static files (charts, CSS)
└── data/                      # Persistent database data
```

## Sample Data

The platform includes a sample e-commerce database with:
- 1,000 customers across 5 countries
- 500 products in 4 categories
- 5,000 orders with realistic date distribution
- Order items with product relationships

Sample queries test various scenarios:
- Simple aggregations with GROUP BY
- JOINs with filtering conditions
- Complex multi-table JOINs
- Date range queries
- Subqueries and HAVING clauses

## Technical Details

### HTMX Integration
- Declarative AJAX interactions without JavaScript
- Real-time progress updates via polling
- Dynamic content swapping for charts and results
- Form submissions with file uploads

### Chart Generation
- Matplotlib charts saved as PNG files
- Three chart types: bar, line, histogram
- Statistical annotations (mean, standard deviation)
- Responsive image sizing with Bootstrap

### Background Processing
- FastAPI BackgroundTasks for long-running experiments
- In-memory status tracking (Redis recommended for production)
- Progress callbacks and error handling

## Production Considerations

- Use Redis for experiment status storage
- Configure proper PostgreSQL connection pooling
- Set up monitoring and logging
- Use environment-specific configurations
- Implement authentication and authorization
- Add data validation and sanitization

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

This project is licensed under the MIT License. 