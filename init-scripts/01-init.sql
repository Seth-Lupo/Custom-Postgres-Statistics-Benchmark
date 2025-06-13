-- Drop any existing databases that might have been left over
DO $$ 
DECLARE
    db_name text;
BEGIN
    FOR db_name IN 
        SELECT datname 
        FROM pg_database 
        WHERE datname NOT IN ('postgres', 'template0', 'template1', 'EXPERIMENT')
    LOOP
        EXECUTE 'DROP DATABASE IF EXISTS ' || quote_ident(db_name);
    END LOOP;
END $$;

-- Clean up any existing tables in EXPERIMENT database
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public; 

-- Create experiment table
CREATE TABLE experiment (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    config_name VARCHAR(100),
    config_yaml TEXT,
    exit_status VARCHAR(50) DEFAULT 'PENDING',
    experiment_logs TEXT,
    stats_reset_strategy VARCHAR(50) DEFAULT 'once',
    transaction_handling VARCHAR(50) DEFAULT 'rollback',
    original_config_yaml TEXT,
    config_modified BOOLEAN DEFAULT FALSE,
    config_modified_at TIMESTAMP
);

-- Create trial table
CREATE TABLE trial (
    id SERIAL PRIMARY KEY,
    experiment_id INTEGER NOT NULL REFERENCES experiment(id) ON DELETE CASCADE,
    trial_number INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    results TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    pg_stats_snapshot TEXT,
    pg_statistic_snapshot TEXT,
    UNIQUE (experiment_id, trial_number)
); 