# Schneider AI Statistics Source

This module provides AI-powered PostgreSQL statistics estimation using the LLM API proxy from the `ai-method-reference` project.

## Overview

The `SchneiderAIStatsSource` class inherits from the base `StatsSource` and integrates with PostgreSQL to:
1. Gather database schema information
2. Use AI models to estimate statistical values
3. Apply AI-generated estimates to `pg_statistic` table
4. Fall back to standard ANALYZE if AI estimation fails

## Key Features

- **API Integration**: Uses `reference.py` from `ai-method-reference/proxy` instead of direct OpenAI calls
- **Configurable**: All settings (API endpoint, model parameters, prompts) stored in YAML config files
- **PostgreSQL Integration**: Directly modifies `pg_statistic` table with AI estimates
- **Robust Fallback**: Falls back to standard PostgreSQL ANALYZE on failures
- **Extensive Logging**: Detailed logging for debugging and monitoring

## Architecture

```
SchneiderAIStatsSource
├── Inherits from StatsSource (base.py)
├── Uses reference.py API for LLM calls
├── Modifies pg_statistic table directly
└── Follows standard config/settings pattern
```

## Configuration Files

### `config/default.yaml`
- Full-featured configuration using LLM proxy
- 10 iterations for robust estimation
- Temperature 0.3 for balanced creativity/consistency

### `config/openai.yaml`
- Configuration for using OpenAI directly
- Uses GPT-4o-mini model by default
- Optimized prompts for OpenAI models

### `config/fast.yaml`
- Quick testing configuration using LLM proxy
- Simplified prompts for testing

## Core Methods

### `apply_statistics(session: Session)`
Main entry point that:
1. Clears PostgreSQL caches
2. Gathers database schema information
3. Requests AI estimation via API
4. Applies estimates to pg_statistic
5. Falls back to standard ANALYZE if needed

### `estimate_statistics_with_ai(schema_info: Dict)`
Formats prompts and calls the AI API through `reference.generate()`

### `apply_ai_statistics_to_pg(session: Session, estimates: Dict)`
Updates specific columns in pg_statistic table:
- `stanullfrac` (column 3): null fraction
- `stadistinct` (column 5): distinct values count
- `stanumbers1` (column 16): statistical numbers array

## Usage Example

```python
from schneider_ai import SchneiderAIStatsSource

# Create stats source with default config
stats_source = SchneiderAIStatsSource()

# Apply AI statistics to database
with Session(engine) as session:
    stats_source.apply_statistics(session)
```

## Dependencies

- `reference.py` from `ai-method-reference/proxy`
- SQLAlchemy/SQLModel for database operations
- PyYAML for configuration loading
- NumPy for statistical operations

## Configuration Options

| Setting | Description | Default |
|---------|-------------|---------|
| `provider` | AI provider to use ("llmproxy" or "openai") | `"llmproxy"` |
| `model` | LLM model to use | `"us.anthropic.claude-3-haiku-20240307-v1:0"` (llmproxy) or `"gpt-4o-mini"` (openai) |
| `temperature` | Response creativity level | `0.3` |
| `num_iterations` | Estimation attempts | `10` |
| `target_columns` | pg_statistic columns to modify | `{stanullfrac: 3, stadistinct: 5, stanumbers1: 16}` |
| `session_id` | Session identifier for LLM proxy | `"schneider_stats_session"` |

## Environment Variables

The following environment variables must be set in `app/app/.env`:

| Variable | Description | Required For |
|----------|-------------|--------------|
| `LLMPROXY_API_ENDPOINT` | LLM proxy API endpoint | llmproxy provider |
| `LLMPROXY_API_KEY` | LLM proxy API key | llmproxy provider |
| `OPENAI_API_KEY` | OpenAI API key | openai provider |
| `OPENAI_API_URL` | OpenAI API base URL | openai provider (optional) |

## Error Handling

- Graceful degradation to standard PostgreSQL ANALYZE
- Comprehensive logging at all levels
- Transaction rollback on failures
- Import validation for reference module

## Integration

The module is automatically discovered by the application's dynamic import system in `/app/app/src/__init__.py` and will appear as a selectable statistics source in the web interface. 