# Configuration Modification Tracking System

## Overview

This system tracks whether experiment configurations have been modified from their original state, providing complete transparency about configuration changes in the database and UI.

## Database Changes

### New Fields in Experiment Model

Added three new fields to the `Experiment` model in `models.py`:

- `original_config_yaml: Optional[str]` - Stores the original configuration YAML before any modifications
- `config_modified: bool` - Boolean flag indicating whether the configuration was modified from original
- `config_modified_at: Optional[datetime]` - Timestamp when configuration was modified

### Migration Script

Created `migration_add_config_tracking.py` to add the new fields:

```sql
ALTER TABLE experiment ADD COLUMN original_config_yaml TEXT;
ALTER TABLE experiment ADD COLUMN config_modified BOOLEAN DEFAULT FALSE;
ALTER TABLE experiment ADD COLUMN config_modified_at TIMESTAMP;
```

## Backend Logic Changes

### Experiment Creation Logic

Modified `experiment.py` to implement configuration tracking:

1. **Original Configuration Capture**: Always stores the original/default configuration YAML
2. **Modification Detection**: Compares custom YAML against original to detect actual changes
3. **Smart Tracking**: Only marks as modified if YAML content actually differs
4. **Timestamp Recording**: Records modification timestamp when changes are detected

### Configuration States

- **Unmodified**: `config_modified = False`, no custom YAML stored
- **Modified**: `config_modified = True`, custom YAML differs from original
- **Reverted**: Custom YAML provided but identical to original (treated as unmodified)

## Frontend UI Changes

### Experiment Detail Page

Enhanced configuration section with:

1. **Three-Column Layout**:
   - Configuration Name
   - Modification Status (Modified/Unmodified badges)
   - Configuration Type (Standard/Custom/Custom-Edited)

2. **Modification Alert**: Warning alert when configuration was modified with timestamp

3. **Configuration Comparison**:
   - Side-by-side view of original vs current configuration
   - Collapsible panels for both configurations
   - Clear visual indicators for modified configurations

### Experiment Form

Enhanced configuration editor with:

1. **Smart Modification Detection**: Compares edited YAML against original before marking as modified
2. **Visual State Indicators**:
   - **Unchanged**: Green badge with check icon
   - **Modified**: Warning badge with exclamation icon
   - **Edit Button States**: Different colors/icons based on modification status

3. **Automatic State Management**:
   - Clears modification state when switching configurations
   - Resets to original when using "Reset to Original" button
   - Only stores custom YAML when actually modified

## Configuration States & Visual Indicators

### Database States

| State | `config_modified` | `config_yaml` | `original_config_yaml` |
|-------|------------------|---------------|----------------------|
| Standard | `false` | `null` or empty | Contains original |
| Custom Unchanged | `false` | `null` or empty | Contains original |
| Custom Modified | `true` | Contains modified | Contains original |

### UI Visual Indicators

| State | Badge Color | Icon | Button State |
|-------|------------|------|-------------|
| Unmodified | Green (success) | ✓ check-circle | Edit (secondary) |
| Modified | Orange (warning) | ⚠️ exclamation-triangle | Modified (warning) |
| Unchanged Custom | Blue (info) | 📄 file-earmark-code | Unchanged (success) |

## Benefits

1. **Audit Trail**: Complete tracking of configuration modifications
2. **Transparency**: Clear visibility into what was changed and when
3. **Accuracy**: Only marks as modified when actual changes occur
4. **Reversibility**: Can compare against original configuration
5. **Data Integrity**: Prevents false modification flags

## Usage

### For Researchers
- Can see exactly what configuration was used in experiments
- Can identify which experiments used modified vs standard configurations
- Can compare original vs modified configurations side-by-side

### For System Administrators
- Full audit trail of configuration changes
- Database accurately reflects modification state
- Can track when and how configurations were altered

## Implementation Notes

- Uses YAML string comparison to detect actual modifications
- Handles edge cases like whitespace differences
- Maintains backward compatibility with existing experiments
- Provides migration script for database schema updates

## Migration Instructions

1. Run the migration script: `python3 migration_add_config_tracking.py`
2. Restart the application to use updated model
3. New experiments will automatically track configuration modifications
4. Existing experiments will show as unmodified (safe default) 