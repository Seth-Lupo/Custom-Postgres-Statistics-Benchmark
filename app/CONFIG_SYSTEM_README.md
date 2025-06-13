# Configuration System

The LLM PostgreSQL Statistics Estimator now includes a comprehensive configuration system that allows you to customize the behavior of different statistics sources.

## Overview

Each statistics source has its own directory with:
- Implementation file (e.g., `direct_pg.py`)
- `config/` directory containing YAML configuration files
- `default.yaml` - the default configuration
- Additional preset configurations (e.g., `fast.yaml`, `aggressive.yaml`)

## Directory Structure

```
app/stats_sources/
├── base.py                    # Base classes
├── direct_pg/                 # Built-in PostgreSQL stats
│   ├── direct_pg.py
│   └── config/
│       ├── default.yaml
│       └── fast.yaml
└── random_pg/                 # Random statistics
    ├── random_pg.py
    └── config/
        ├── default.yaml
        ├── aggressive.yaml
        └── conservative.yaml
```

## Using the Configuration Editor

### 1. Select a Statistics Source
Choose your desired statistics source from the dropdown in the experiment form.

### 2. Choose a Configuration
After selecting a stats source, choose from available configurations. Each has different settings optimized for different scenarios.

### 3. Edit Configuration (Optional)
Click the "Edit" button next to the configuration dropdown to open the visual editor:

- **YAML Editor**: Edit configuration settings in real-time
- **Configuration Info**: View source, original config, and modification status
- **Common Settings**: Reference for available configuration options
- **Reset**: Restore to original configuration
- **Save**: Apply your custom changes

### 4. Run Experiment
Your experiment will use either the selected preset or your custom configuration.

## Configuration Settings

### Common Settings (All Sources)

| Setting | Type | Description | Example |
|---------|------|-------------|---------|
| `analyze_verbose` | boolean | Enable verbose ANALYZE output | `true` |
| `analyze_timeout_seconds` | integer | Timeout for operations | `300` |
| `clear_caches` | boolean | Clear database caches before analysis | `true` |
| `reset_counters` | boolean | Reset PostgreSQL statistics counters | `true` |
| `work_mem` | string | Memory for sort/hash operations | `"16MB"` |
| `maintenance_work_mem` | string | Memory for maintenance operations | `"16MB"` |

### Random PostgreSQL Source Settings

| Setting | Type | Description | Example |
|---------|------|-------------|---------|
| `min_stats_value` | integer | Minimum random statistics value | `1` |
| `max_stats_value` | integer | Maximum random statistics value | `10000` |
| `skip_system_schemas` | boolean | Skip system schemas | `true` |
| `excluded_schemas` | array | List of schemas to exclude | `["information_schema", "pg_catalog"]` |

## Preset Configurations

### Direct PostgreSQL Source

- **default**: Standard PostgreSQL ANALYZE with full caching and statistics
- **fast**: Reduced timeouts, minimal caching for quick experiments

### Random PostgreSQL Source

- **default**: Balanced random stats (1-10,000 range)
- **aggressive**: Wide range (1-50,000), higher memory allocation  
- **conservative**: Narrow range (100-1,000), minimal resources

## Creating Custom Configurations

### 1. Add New YAML File
Create a new `.yaml` file in the appropriate `config/` directory:

```yaml
name: my_custom_config
description: "My custom configuration for specific needs"
settings:
  analyze_verbose: false
  analyze_timeout_seconds: 600
  clear_caches: true
  work_mem: "64MB"
  # ... other settings
```

### 2. Use in Experiments
The new configuration will automatically appear in the dropdown and can be selected for experiments.

## Experiment Results

Configuration information is stored with each experiment:

- **Configuration Name**: The original preset used
- **Configuration Status**: Whether it was modified
- **YAML Configuration**: The exact configuration used (including any edits)

This ensures full reproducibility of experiments.

## Migration

If upgrading from a previous version, run the migration script:

```bash
python migration_add_config_fields.py
```

This adds the necessary database fields to store configuration information.

## Troubleshooting

### Configuration Not Loading
- Check that the YAML file is valid
- Ensure the file is in the correct `config/` directory
- Verify the `name` field matches the filename (without .yaml)

### Editor Shows Error
- Verify YAML syntax is correct
- Ensure required fields (`name`, `settings`) are present
- Check browser console for detailed error messages

### Database Migration Issues
- Ensure database is accessible
- Check DATABASE_URL environment variable
- Verify PostgreSQL user has necessary permissions

## Example YAML Configuration

```yaml
name: high_performance
description: "High performance configuration with increased resources"
settings:
  analyze_verbose: true
  analyze_timeout_seconds: 900
  clear_caches: true
  reset_counters: true
  work_mem: "256MB"
  maintenance_work_mem: "512MB"
  
  # Random source specific
  min_stats_value: 1000
  max_stats_value: 100000
  skip_system_schemas: true
  excluded_schemas:
    - "information_schema"
    - "pg_catalog"
    - "pg_toast"
``` 