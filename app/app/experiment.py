import time
import statistics
import traceback
import json
import re
from typing import List, Tuple, Callable, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from .models import Experiment, Trial
from .src.base import StatsSource, StatsSourceConfig
from .logging_config import experiment_logger, query_logger, stats_logger, stats_source_logger
from .database import create_database, drop_database, load_dump, get_db_session
from sqlmodel import Session
from datetime import datetime
    

class ExperimentError(Exception):
    """Base class for experiment-related errors."""
    pass

class QueryExecutionError(ExperimentError):
    """Error during query execution."""
    pass

class StatsApplicationError(ExperimentError):
    """Error during statistics application."""
    pass

class ExperimentRunner:
    """Handles running benchmarking experiments."""
    
    def __init__(self):
        self.src = {}
        # Discover all available StatsSource subclasses
        for subclass in StatsSource.__subclasses__():
            # This logic is from StatsSource._get_config_dir
            class_name = subclass.__name__
            if class_name.endswith('StatsSource'):
                class_name = class_name[:-11]
            
            snake_case = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', class_name)
            key = re.sub('([a-z0-9])([A-Z])', r'\1_\2', snake_case).lower()
            
            self.src[key] = subclass
    
    def get_available_src(self) -> List[Tuple[str, str]]:
        """Get list of available statistics sources as (key, display_name) tuples."""
        return [(key, source().name()) for key, source in self.src.items()]
    
    def get_available_configs(self, stats_source: str) -> List[Tuple[str, str]]:
        """Get list of available configurations for a stats source as (config_name, display_name) tuples."""
        if stats_source not in self.src:
            return []
        
        source_class = self.src[stats_source]
        instance = source_class()
        return instance.get_available_configs()
    
    def run_experiment(self, session: Session, stats_source: str, config_name: str, config_yaml: str, query: str, iterations: int, stats_reset_strategy: str, transaction_handling: str, progress_callback: Callable[[str, int, int], None], dump_path: str, name: str) -> Experiment:
        
        db_name = "test_database"
        experiment_logs = []  # Capture all experiment logs
        
        def log_and_callback(message: str, current: int, total: int):
            """Log message and call the progress callback."""
            timestamped_message = f"[{datetime.utcnow().strftime('%H:%M:%S')}] {message}"
            experiment_logs.append(timestamped_message)
            # Pass the raw message to progress callback (it will add its own timestamp)
            progress_callback(message, current, total)
        
        def stats_source_stream_callback(log_level: str, message: str):
            """Callback to capture stats source logs and stream them to frontend."""
            formatted_msg = f"[Stats] {message}"
            timestamped_message = f"[{datetime.utcnow().strftime('%H:%M:%S')}] {formatted_msg}"
            experiment_logs.append(timestamped_message)
            # Also call the progress callback to stream to frontend (use current iteration count)
            current_iter = len([log for log in experiment_logs if "Trial" in log and "completed" in log])
            progress_callback(formatted_msg, current_iter, iterations)
        
        # Set up stats source logging stream
        stats_source_logger.stream_handler.clear_experiment_logs()
        stats_source_logger.stream_handler.set_stream_callback(stats_source_stream_callback)
        
        """Run a complete experiment and return the result."""
        experiment_logger.info(f"Starting new experiment with {stats_source} stats source")
        experiment_logger.debug(f"Query: {query}")
        experiment_logger.debug(f"Iterations: {iterations}")
        experiment_logger.info(f"Stats reset strategy: {stats_reset_strategy}")
        experiment_logger.info(f"Transaction handling: {transaction_handling}")
        experiment_logger.info(f"Using temporary database: {db_name} from dump: {dump_path}")
        
        if stats_source not in self.src:
            error_msg = f"Unknown stats source: {stats_source}"
            experiment_logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Instantiate stats source with the specified configuration
        source_class = self.src[stats_source]
        
        # Get the original/default configuration YAML for comparison
        original_config_yaml = None
        config_modified = False
        config_modified_at = None
        
        # Determine which configuration to use and track modifications
        if config_yaml:
            # Use the custom YAML configuration provided by the user
            import yaml
            try:
                config_data = yaml.safe_load(config_yaml)
                config = StatsSourceConfig(config_data)
                stats_source_instance = source_class(config)
                experiment_logger.info(f"Using custom configuration: {config.name}")
                
                # Get the original configuration YAML string for comparison
                effective_config_name = config_name or 'default'
                original_config_yaml = source_class().get_config_content(effective_config_name)
                
                # Check if configuration was actually modified
                if original_config_yaml and config_yaml.strip() != original_config_yaml.strip():
                    config_modified = True
                    config_modified_at = datetime.utcnow()
                    experiment_logger.info("Configuration was modified from original")
                else:
                    experiment_logger.info("Configuration unchanged from original")
                    
            except Exception as e:
                error_msg = f"Failed to parse custom configuration YAML: {str(e)}"
                experiment_logger.error(error_msg)
                raise ValueError(error_msg)
        else:
            # Use a named or default configuration
            effective_config_name = config_name or 'default'
            config = source_class().load_config(effective_config_name)
            stats_source_instance = source_class(config)
            experiment_logger.info(f"Using named/default configuration: {effective_config_name}")
            
            # Get the original YAML for the named configuration
            original_config_yaml = source_class().get_config_content(effective_config_name)

        # Store the actual YAML that will be used
        actual_config_yaml = config_yaml if config_yaml else original_config_yaml
        
        # Create experiment record
        try:
            log_and_callback("Creating experiment record...", 0, iterations)
            experiment = Experiment(
                name=name,
                stats_source=stats_source_instance.display_name(),
                config_name=config_name or 'default',
                config_yaml=actual_config_yaml,
                original_config_yaml=original_config_yaml,
                config_modified=config_modified,
                config_modified_at=config_modified_at,
                query=query,
                iterations=iterations,
                stats_reset_strategy=stats_reset_strategy,
                transaction_handling=transaction_handling,
                exit_status="RUNNING"
            )
            session.add(experiment)
            session.commit()
            session.refresh(experiment)
            experiment_logger.info(f"Created experiment record with ID {experiment.id}")
            
        except SQLAlchemyError as e:
            error_msg = f"Failed to create experiment record: {str(e)}"
            experiment_logger.error(error_msg)
            experiment_logger.debug(traceback.format_exc())
            raise ExperimentError(error_msg) from e
        
        experiment_db_session_generator = None
        experiment_db_session = None
        try:
            # Create and setup the temporary database
            log_and_callback(f"Creating temporary database '{db_name}'...", 0, iterations)
            create_database(db_name)
            log_and_callback(f"Loading data from '{dump_path}'...", 0, iterations)
            load_dump(db_name, dump_path)

            # Get a session to the temporary database
            experiment_db_session_generator = get_db_session(db_name)
            experiment_db_session = next(experiment_db_session_generator)

            # Set session parameters for consistent execution environment
            # Maximum time (in ms) a statement can run before being cancelled (0 = no timeout)
            timeout = stats_source_instance.config.get_setting('statement_timeout_ms', 0)
            experiment_db_session.execute(text(f"SET statement_timeout = {timeout}"))
            
            # Memory allocated for internal sort/hash operations
            work_mem = stats_source_instance.config.get_setting('work_mem', '16MB') 
            experiment_db_session.execute(text(f"SET work_mem = '{work_mem}'"))
            
            # Memory for maintenance operations like VACUUM, CREATE INDEX
            maint_work_mem = stats_source_instance.config.get_setting('maintenance_work_mem', '16MB')
            experiment_db_session.execute(text(f"SET maintenance_work_mem = '{maint_work_mem}'"))
            
            # Planner's assumption about size of disk cache
            cache_size = stats_source_instance.config.get_setting('effective_cache_size', '1GB')
            experiment_db_session.execute(text(f"SET effective_cache_size = '{cache_size}'"))
            
            # Relative cost of non-sequential page reads
            random_cost = stats_source_instance.config.get_setting('random_page_cost', 1.0)
            experiment_db_session.execute(text(f"SET random_page_cost = {random_cost}"))
            
            # Relative cost of sequential page reads  
            seq_cost = stats_source_instance.config.get_setting('seq_page_cost', 1.0)
            experiment_db_session.execute(text(f"SET seq_page_cost = {seq_cost}"))
            
            experiment_db_session.commit()

            # Apply statistics based on strategy
            if stats_reset_strategy == "once":
                # Apply statistics once before all trials
                msg = f"Applying {stats_source_instance.name()} statistics to database (once before all trials)..."
                log_and_callback(msg, 0, iterations)
                experiment_logger.info(msg)
                
                try:
                    stats_source_instance.apply_statistics(experiment_db_session)
                    stats_logger.info(f"Successfully applied {stats_source_instance.name()} statistics")
                except Exception as e:
                    error_msg = f"Failed to apply statistics: {str(e)}"
                    stats_logger.error(error_msg)
                    stats_logger.debug(traceback.format_exc())
                    raise StatsApplicationError(error_msg) from e
            
            # Run trials
            execution_times = []
            query_plans = []
            
            for i in range(iterations):
                trial_msg = f"Running trial {i + 1}/{iterations}..."
                log_and_callback(trial_msg, i, iterations)
                experiment_logger.info(trial_msg)
                
                try:
                    # Apply statistics per trial if strategy is per_trial
                    if stats_reset_strategy == "per_trial":
                        trial_stats_msg = f"Resetting and applying {stats_source_instance.name()} statistics for trial {i + 1}..."
                        log_and_callback(trial_stats_msg, i, iterations)
                        experiment_logger.info(trial_stats_msg)
                        
                        try:
                            stats_source_instance.apply_statistics(experiment_db_session)
                            stats_logger.info(f"Successfully applied {stats_source_instance.name()} statistics for trial {i + 1}")
                        except Exception as e:
                            error_msg = f"Failed to apply statistics for trial {i + 1}: {str(e)}"
                            stats_logger.error(error_msg)
                            stats_logger.debug(traceback.format_exc())
                            raise StatsApplicationError(error_msg) from e
                    
                    # Execute the trial with transaction handling
                    execution_time, cost_estimate, query_plan = self._run_single_trial(
                        experiment_db_session, 
                        query, 
                        transaction_handling,
                        stats_source_instance
                    )
                    execution_times.append(execution_time)
                    query_plans.append(query_plan)
                    
                    # Capture statistics snapshots for this trial
                    pg_stats_snapshot, pg_statistic_snapshot = self._capture_statistics_snapshots(experiment_db_session)
                    
                    # Record trial
                    trial = Trial(
                        experiment_id=experiment.id,
                        run_index=i + 1,
                        execution_time=execution_time,
                        cost_estimate=cost_estimate,
                        pg_stats_snapshot=pg_stats_snapshot,
                        pg_statistic_snapshot=pg_statistic_snapshot
                    )
                    session.add(trial)
                    session.commit()
                    
                    # Log detailed trial information
                    query_logger.info(f"Trial {i + 1} completed successfully:")
                    query_logger.debug(f"Execution time: {execution_time:.4f}s")
                    query_logger.debug(f"Cost estimate: {cost_estimate:.2f}")
                    query_logger.debug(f"Query plan: {query_plan}")
                    query_logger.debug(f"Captured statistics snapshots for trial {i + 1}")
                    
                    result_msg = f"Trial {i + 1} completed: Time={execution_time:.4f}s, Cost={cost_estimate:.2f}"
                    log_and_callback(result_msg, i + 1, iterations)
                    
                except Exception as e:
                    error_msg = f"Error in trial {i + 1}: {str(e)}"
                    query_logger.error(error_msg)
                    query_logger.debug(traceback.format_exc())
                    log_and_callback(f"⚠️ {error_msg}", i + 1, iterations)
                    raise QueryExecutionError(error_msg) from e
            
            # Calculate aggregate statistics
            log_and_callback("Calculating final statistics...", iterations, iterations)
            experiment_logger.info("Computing experiment statistics")
            
            experiment.avg_time = statistics.mean(execution_times)
            experiment.stddev_time = statistics.stdev(execution_times) if len(execution_times) > 1 else 0.0
            
            # Log statistical analysis
            experiment_logger.info("Experiment statistics computed:")
            experiment_logger.info(f"Average execution time: {experiment.avg_time:.4f}s")
            experiment_logger.info(f"Standard deviation: {experiment.stddev_time:.4f}s")
            experiment_logger.info(f"Min time: {min(execution_times):.4f}s")
            experiment_logger.info(f"Max time: {max(execution_times):.4f}s")
            
            # Analyze query plan changes
            if len(set(str(p) for p in query_plans)) > 1:
                experiment_logger.warning("Query plans varied between trials!")
                for i, plan in enumerate(query_plans):
                    experiment_logger.debug(f"Plan for trial {i + 1}: {plan}")
            
            session.commit()
            session.refresh(experiment)
            
            final_msg = (
                f"Experiment completed! "
                f"Average time: {experiment.avg_time:.4f}s ± {experiment.stddev_time:.4f}s\n"
                f"Min: {min(execution_times):.4f}s, Max: {max(execution_times):.4f}s"
            )
            log_and_callback(final_msg, iterations, iterations)
            experiment_logger.info("Experiment completed successfully")
            
            # Save logs and mark as successful
            experiment.exit_status = "SUCCESS"
            
            # Capture any remaining stats source logs and add them to experiment logs
            stats_source_logs = stats_source_logger.stream_handler.get_experiment_logs()
            for log in stats_source_logs:
                if log not in experiment_logs:
                    experiment_logs.append(log)
                    
            experiment.experiment_logs = '\n'.join(experiment_logs)
            session.commit()
            session.refresh(experiment)
            
            return experiment
            
        except Exception as e:
            session.rollback()
            error_msg = f"Experiment failed: {str(e)}"
            experiment_logger.error(error_msg)
            experiment_logger.debug(traceback.format_exc())
            log_and_callback(f"❌ {error_msg}", iterations, iterations)
            
            # Update experiment with failure status and logs
            try:
                experiment.exit_status = "FAILURE"
                
                # Capture any remaining stats source logs and add them to experiment logs
                stats_source_logs = stats_source_logger.stream_handler.get_experiment_logs()
                for log in stats_source_logs:
                    if log not in experiment_logs:
                        experiment_logs.append(log)
                        
                experiment.experiment_logs = '\n'.join(experiment_logs)
                session.commit()
            except Exception:
                pass  # If we can't save, that's ok - the main error is more important
            
            raise
        finally:
            # Clean up stats source logging
            stats_source_logger.stream_handler.set_stream_callback(None)
            stats_source_logger.stream_handler.clear_experiment_logs()
            
            if experiment_db_session_generator:
                try:
                    # Ensure the generator is closed, which also closes the session
                    next(experiment_db_session_generator)
                except StopIteration:
                    pass
            # Drop the temporary database
            log_and_callback(f"Cleaning up temporary database '{db_name}'...", iterations, iterations)
            drop_database(db_name)
            experiment_logger.info(f"Dropped temporary database: {db_name}")
    
    def _run_single_trial(self, session: Session, query: str, transaction_handling: str, stats_source_instance: StatsSource) -> Tuple[float, float, Dict[str, Any]]:
        """Run a single trial and return (execution_time, cost_estimate, query_plan)."""
        query_logger.debug(f"Starting new trial with query: {query} (transaction handling: {transaction_handling})")
        
        try:
            # Enhanced cache clearing before trial
            if stats_source_instance.config.get_setting('clear_caches', True):
                session.commit()
                query_logger.debug("Performing comprehensive cache clearing...")
                conn = session.connection().connection
                conn.set_session(autocommit=True)
                try:
                    session.execute(text("DISCARD ALL"))
                finally:
                    conn.set_session(autocommit=False)
                session.commit()
                query_logger.debug("Cache clearing completed.")

            # Reset statistics if configured
            if stats_source_instance.config.get_setting('reset_counters', True):
                query_logger.debug("Resetting statistics counters...")
                session.execute(text("SELECT pg_stat_reset()"))
                try:
                    session.execute(text("SELECT pg_stat_reset_shared('bgwriter')"))
                    session.execute(text("SELECT pg_stat_reset_shared('archiver')"))
                except Exception:
                    query_logger.debug("Some shared statistics reset operations not available")
                
                try:
                    session.execute(text("SELECT pg_stat_statements_reset()"))
                except Exception:
                    query_logger.debug("pg_stat_statements extension not available, skipping reset")
                session.commit()
                query_logger.debug("Statistics counters reset.")

            # Get query plan (EXPLAIN)
            query_logger.debug("Generating query plan using EXPLAIN...")
            explain_query = text(f"EXPLAIN (FORMAT JSON) {query}")
            explain_result = session.execute(explain_query)
            explain_result = explain_result.fetchone()
            
            if not explain_result or not explain_result[0]:
                error_msg = "Failed to get query plan"
                query_logger.error(error_msg)
                raise QueryExecutionError(error_msg)
            
            plan_data = explain_result[0]
            query_plan = plan_data[0] if isinstance(plan_data, list) and len(plan_data) > 0 else {}
            cost_estimate = query_plan.get("Plan", {}).get("Total Cost", 0.0)
            
            query_logger.debug(f"Query plan obtained: {query_plan}")
            query_logger.debug(f"Estimated cost: {cost_estimate}")
            
            # Execute query with appropriate transaction handling
            if transaction_handling == "rollback":
                # Create a savepoint to rollback to after query execution
                savepoint = session.begin_nested()
                query_logger.debug("Created savepoint for rollback transaction handling")
                
                try:
                    # Execute query and measure time
                    query_logger.debug("Executing query with rollback transaction handling...")
                    start_time = time.perf_counter()
                    session.execute(text(query))
                    session.flush()  # Ensure query is executed but don't commit
                    end_time = time.perf_counter()
                    
                    execution_time = end_time - start_time
                    query_logger.debug(f"Query executed in {execution_time:.4f} seconds")
                    
                    # Rollback to savepoint to undo any changes
                    savepoint.rollback()
                    query_logger.debug("Rolled back transaction - database state preserved")
                    
                except Exception as e:
                    savepoint.rollback()
                    raise e
                    
            else:  # transaction_handling == "persist"
                # Execute query and commit changes
                query_logger.debug("Executing query with persistent transaction handling...")
                start_time = time.perf_counter()
                session.execute(text(query))
                session.commit()  # Commit transaction to persist changes
                end_time = time.perf_counter()
                
                execution_time = end_time - start_time
                query_logger.debug(f"Query executed in {execution_time:.4f} seconds (changes persisted)")
            
            return execution_time, cost_estimate, query_plan
            
        except Exception as e:
            query_logger.error(f"Error during trial execution: {str(e)}")
            query_logger.debug(traceback.format_exc())
            raise QueryExecutionError(f"Trial failed: {str(e)}") from e

    def _capture_statistics_snapshots(self, session: Session) -> Tuple[str, str]:
        """Capture snapshots of pg_stats and pg_statistic."""
        try:
            # Capture pg_stats snapshot (public schema only)
            pg_stats_query = text("""
                SELECT schemaname, tablename, attname, inherited, null_frac, avg_width, n_distinct,
                       most_common_vals, most_common_freqs, histogram_bounds, correlation, 
                       most_common_elems, most_common_elem_freqs, elem_count_histogram
                FROM pg_stats 
                WHERE schemaname = 'public'
                ORDER BY schemaname, tablename, attname
            """)
            pg_stats_result = session.execute(pg_stats_query)
            pg_stats_data = []
            for row in pg_stats_result:
                pg_stats_data.append({
                    'schemaname': row.schemaname,
                    'tablename': row.tablename, 
                    'attname': row.attname,
                    'inherited': row.inherited,
                    'null_frac': float(row.null_frac) if row.null_frac is not None else None,
                    'avg_width': int(row.avg_width) if row.avg_width is not None else None,
                    'n_distinct': float(row.n_distinct) if row.n_distinct is not None else None,
                    'most_common_vals': str(row.most_common_vals) if row.most_common_vals is not None else None,
                    'most_common_freqs': str(row.most_common_freqs) if row.most_common_freqs is not None else None,
                    'histogram_bounds': str(row.histogram_bounds) if row.histogram_bounds is not None else None,
                    'correlation': float(row.correlation) if row.correlation is not None else None,
                    'most_common_elems': str(row.most_common_elems) if row.most_common_elems is not None else None,
                    'most_common_elem_freqs': str(row.most_common_elem_freqs) if row.most_common_elem_freqs is not None else None,
                    'elem_count_histogram': str(row.elem_count_histogram) if row.elem_count_histogram is not None else None
                })
            
            # Capture pg_statistic snapshot (public schema tables only)
            pg_statistic_query = text("""
                SELECT starelid::regclass AS table_name, staattnum, stainherit, stanullfrac, 
                       stawidth, stadistinct, stakind1, stakind2, stakind3, stakind4, stakind5,
                       staop1, staop2, staop3, staop4, staop5,
                       stacoll1, stacoll2, stacoll3, stacoll4, stacoll5,
                       stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5,
                       stavalues1, stavalues2, stavalues3, stavalues4, stavalues5
                FROM pg_statistic ps
                JOIN pg_class pc ON ps.starelid = pc.oid
                JOIN pg_namespace pn ON pc.relnamespace = pn.oid
                WHERE pn.nspname = 'public'
                ORDER BY starelid, staattnum
            """)
            pg_statistic_result = session.execute(pg_statistic_query)
            pg_statistic_data = []
            for row in pg_statistic_result:
                pg_statistic_data.append({
                    'table_name': str(row.table_name),
                    'staattnum': int(row.staattnum),
                    'stainherit': bool(row.stainherit),
                    'stanullfrac': float(row.stanullfrac) if row.stanullfrac is not None else None,
                    'stawidth': int(row.stawidth) if row.stawidth is not None else None,
                    'stadistinct': float(row.stadistinct) if row.stadistinct is not None else None,
                    'stakind1': int(row.stakind1) if row.stakind1 is not None else None,
                    'stakind2': int(row.stakind2) if row.stakind2 is not None else None,
                    'stakind3': int(row.stakind3) if row.stakind3 is not None else None,
                    'stakind4': int(row.stakind4) if row.stakind4 is not None else None,
                    'stakind5': int(row.stakind5) if row.stakind5 is not None else None,
                    'staop1': int(row.staop1) if row.staop1 is not None else None,
                    'staop2': int(row.staop2) if row.staop2 is not None else None,
                    'staop3': int(row.staop3) if row.staop3 is not None else None,
                    'staop4': int(row.staop4) if row.staop4 is not None else None,
                    'staop5': int(row.staop5) if row.staop5 is not None else None,
                    'stacoll1': int(row.stacoll1) if row.stacoll1 is not None else None,
                    'stacoll2': int(row.stacoll2) if row.stacoll2 is not None else None,
                    'stacoll3': int(row.stacoll3) if row.stacoll3 is not None else None,
                    'stacoll4': int(row.stacoll4) if row.stacoll4 is not None else None,
                    'stacoll5': int(row.stacoll5) if row.stacoll5 is not None else None,
                    'stanumbers1': str(row.stanumbers1) if row.stanumbers1 is not None else None,
                    'stanumbers2': str(row.stanumbers2) if row.stanumbers2 is not None else None,
                    'stanumbers3': str(row.stanumbers3) if row.stanumbers3 is not None else None,
                    'stanumbers4': str(row.stanumbers4) if row.stanumbers4 is not None else None,
                    'stanumbers5': str(row.stanumbers5) if row.stanumbers5 is not None else None,
                    'stavalues1': str(row.stavalues1) if row.stavalues1 is not None else None,
                    'stavalues2': str(row.stavalues2) if row.stavalues2 is not None else None,
                    'stavalues3': str(row.stavalues3) if row.stavalues3 is not None else None,
                    'stavalues4': str(row.stavalues4) if row.stavalues4 is not None else None,
                    'stavalues5': str(row.stavalues5) if row.stavalues5 is not None else None
                })
            
            # Convert to JSON strings
            pg_stats_json = json.dumps(pg_stats_data, default=str)
            pg_statistic_json = json.dumps(pg_statistic_data, default=str)
            
            stats_logger.debug(f"Captured {len(pg_stats_data)} pg_stats entries and {len(pg_statistic_data)} pg_statistic entries (public schema only)")
            
            return pg_stats_json, pg_statistic_json
            
        except Exception as e:
            stats_logger.error(f"Failed to capture statistics snapshots: {str(e)}")
            stats_logger.debug(traceback.format_exc())
            # Return empty JSON on error
            return json.dumps([]), json.dumps([]) 