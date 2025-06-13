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