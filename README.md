# PostgreSQL Statistics Estimator

A platform for benchmarking PostgreSQL query performance with different statistics sources. This tool allows you to upload a database schema and a set of queries, and then run experiments to compare how different methods of generating table statistics affect query execution plans and performance.

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Running the Application](#running-the-application)
- [Usage](#usage)
  - [1. Upload Database Dump](#1-upload-database-dump)
  - [2. Upload Queries](#2-upload-queries)
  - [3. Run an Experiment](#3-run-an-experiment)
  - [4. View Results](#4-view-results)
- [Extending the Platform: Creating New Statistics Sources](#extending-the-platform-creating-new-statistics-sources)
- [Advantages](#advantages)

## Overview

The PostgreSQL query planner relies on table and column statistics to choose the most efficient execution plan for a query. Inaccurate or outdated statistics can lead to suboptimal query plans and poor performance.

This platform provides a framework to:
- Easily restore a database from a dump.
- Define multiple "statistics sources" - different ways of generating `ANALYZE` statistics.
- Run experiments that test a given set of SQL queries against each statistics source.
- Compare the `EXPLAIN (ANALYZE, BUFFERS)` output for each query across all experiments.
- Visualize performance differences.

This helps database administrators and developers understand the impact of statistics on their specific workloads and make informed decisions about how to manage them.

## Tech Stack

- **Backend**: [FastAPI](https://fastapi.tiangolo.com/) (Python)
- **Database**: [PostgreSQL](https://www.postgresql.org/) (version 15)
- **Data Handling**: [SQLModel](https://sqlmodel.tiangolo.com/), [Pandas](https://pandas.pydata.org/), [NumPy](https://numpy.org/)
- **Frontend**: [Jinja2](https://jinja.palletsprojects.com/) for templating, with vanilla HTML/CSS/JS.
- **Visualization**: [Plotly](https://plotly.com/python/)
- **Containerization**: [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Running the Application

1.  Clone this repository.
2.  Navigate to the root directory of the project.
3.  Run the application using the provided script:

    ```bash
    ./start.sh
    ```

    This will build the Docker containers and start the application in detached mode.

    Alternatively, you can use `docker-compose` directly:
    ```bash
    docker-compose up --build -d
    ```

4.  The web application will be accessible at [http://localhost:8000](http://localhost:8000).
5.  The PostgreSQL database will be accessible on port `5432`.

To stop the application, run:
```bash
docker-compose down
```

The `reset.sh` script can be used to completely stop, remove, and rebuild the application, including the database volume.
```bash
./reset.sh
```

## Usage

The application workflow is divided into four main steps.

### 1. Upload Database Dump

-   Navigate to the **Upload** page.
-   Under "Upload Database Dump", choose a `.sql` or `.dump` file that contains your database schema and optionally data.
-   Click "Upload". The platform will use this dump to restore the database before each experiment run.

### 2. Upload Queries

-   Navigate to the **Upload** page.
-   Under "Upload Query Files", you can upload one or more `.sql` files.
-   Each file should contain one or more SQL queries that you want to benchmark. The queries in a single file will be treated as a group for analysis.

### 3. Run an Experiment

-   Navigate to the **Run Experiment** page.
-   Give your experiment a descriptive name.
-   Select the query files you want to include in the experiment.
-   Select the "Statistics Sources" you want to compare. These are different methods for generating statistics (e.g., default `ANALYZE`, or a custom method).
-   Click "Run Experiment". The process will be executed in the background. You can monitor the logs from the terminal using `docker-compose logs -f web`.

### 4. View Results

-   Navigate to the **Results** page.
-   You will see a list of completed experiments.
-   Click on an experiment to view the detailed results.
-   The results page provides a comparison of the `EXPLAIN (ANALYZE, BUFFERS)` output for each query across the selected statistics sources, along with visualizations of execution time.

## Extending the Platform: Creating New Statistics Sources

The core of this platform is the concept of a "Statistics Source" (or "src"). This is a pluggable component that defines a method for generating statistics on the database. You can easily create your own.

To create a new statistics source:

1.  **Create a Directory**:
    -   Inside `app/app/src/`, create a new directory. The name should be the `snake_case` version of your new source's class name (e.g., `my_awesome_source` for a class named `MyAwesomeSourceStatsSource`).

2.  **Create the Source Class**:
    -   Inside your new directory, create a Python file (e.g., `my_awesome_source.py`).
    -   In this file, define a class that inherits from `app.src.base.StatsSource`.
    -   You **must** implement the `name()` abstract method, which should return a unique identifier for your source.

    ```python
    # in app/app/src/my_awesome_source/my_awesome_source.py
    from ..base import StatsSource
    from sqlmodel import Session

    class MyAwesomeSourceStatsSource(StatsSource):
        def name(self) -> str:
            return "my_awesome_source"

        # Optionally, override other methods like apply_statistics
        def apply_statistics(self, session: Session) -> None:
            # Your custom logic to apply statistics
            # For example, you could call a custom procedure,
            # or use different ANALYZE parameters.
            self.logger.info(f"Applying stats with {self.name()}")
            
            # Make sure to call clear_caches first
            self.clear_caches(session)

            # Custom logic here...
            session.execute(text("ANALYZE;")) # Example
            session.commit()
            
            self.logger.info("Finished applying stats.")

    ```

3.  **Add Configuration**:
    -   Inside your source's directory (e.g., `app/app/src/my_awesome_source/`), create a `config` subdirectory.
    -   Inside `config`, add one or more `.yaml` files. Each file represents a different configuration for your source.
    -   A `default.yaml` is recommended.

    Example `my_config.yaml`:
    ```yaml
    name: default
    description: "Default PostgreSQL built-in statistics configuration"
    settings:
        analyze_verbose: true
        analyze_timeout_seconds: 300
        clear_caches: true
        reset_counters: true
        work_mem: "16MB"
        maintenance_work_mem: "16MB" 
    data:
        message: "This is a test message. Settings are generally determine the enviroment, but data is for other aspects of the program (epsilon, prompts, etc)"
    ```

4.  **Import the New Source**:
    -   Finally, in `app/app/src/__init__.py`, import your new class so the application can find it.

    ```python
    # in app/app/src/__init__.py
    # ... existing imports
    from .my_awesome_source.my_awesome_source import MyAwesomeSourceStatsSource
    ```

The application will now automatically discover and display your new statistics source as an option when running experiments.

## Advantages

-   **Automated & Repeatable**: Provides a consistent environment for running benchmark tests, eliminating manual setup and inconsistencies.
-   **Comparative Analysis**: Directly compare different statistics generation strategies against the same queries and dataset.
-   **Isolates Impact**: Helps isolate the performance impact of statistics from other database or application variables.
-   **Easy to Use**: A simple web interface allows users to manage experiments without needing deep expertise in the underlying scripts.
-   **Extensible**: The pluggable "Statistics Source" architecture makes it easy to add and test new ideas for statistics management. 