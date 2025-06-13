import time
import statistics
import traceback
from typing import List, Tuple, Callable, Dict, Any
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from .models import Experiment, Trial
from .stats_sources.base import StatsSource, StatsSourceConfig
from .stats_sources.direct_pg import DirectPgStatsSource
from .stats_sources.random_pg import RandomPgStatsSource
from .logging_config import experiment_logger, query_logger, stats_logger
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
        self.stats_sources = {
            "direct": DirectPgStatsSource,
            "random": RandomPgStatsSource,
        }
    
    def get_available_stats_sources(self) -> List[Tuple[str, str]]:
        """Get list of available statistics sources as (key, display_name) tuples."""
        return [(key, source().name()) for key, source in self.stats_sources.items()]
    
    def get_available_configs(self, stats_source: str) -> List[Tuple[str, str]]:
        """Get list of available configurations for a stats source as (config_name, display_name) tuples."""
        if stats_source not in self.stats_sources:
            return []
        
        source_class = self.stats_sources[stats_source]
        instance = source_class()
        return instance.get_available_configs()
    
    def run_experiment(self, session: Session, stats_source: str, config_name: str, config_yaml: str, query: str, iterations: int, stats_reset_strategy: str, transaction_handling: str, progress_callback: Callable[[str, int, int], None], dump_path: str, name: str) -> Experiment:
        
        db_name = "test_database"
        experiment_logs = []  # Capture all experiment logs
        
        def log_and_callback(message: str, current: int, total: int):
            """Log message and call the progress callback."""
            experiment_logs.append(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {message}")
            progress_callback(message, current, total)
        
        """Run a complete experiment and return the result."""
        experiment_logger.info(f"Starting new experiment with {stats_source} stats source")
        experiment_logger.debug(f"Query: {query}")
        experiment_logger.debug(f"Iterations: {iterations}")
        experiment_logger.info(f"Stats reset strategy: {stats_reset_strategy}")
        experiment_logger.info(f"Transaction handling: {transaction_handling}")
        experiment_logger.info(f"Using temporary database: {db_name} from dump: {dump_path}")
        
        if stats_source not in self.stats_sources:
            error_msg = f"Unknown stats source: {stats_source}"
            experiment_logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Instantiate stats source with the specified configuration
        source_class = self.stats_sources[stats_source]
        
        # Determine which configuration to use
        if config_yaml:
            # Use the custom YAML configuration
            import yaml
            try:
                config_data = yaml.safe_load(config_yaml)
                config = StatsSourceConfig(config_data)
                stats_source_instance = source_class(config)
                experiment_logger.info(f"Using custom configuration: {config.name}")
            except Exception as e:
                error_msg = f"Failed to parse custom configuration YAML: {str(e)}"
                experiment_logger.error(error_msg)
                raise ValueError(error_msg)
        elif config_name and config_name != 'default':
            # Use the named configuration
            config = source_class().load_config(config_name)
            stats_source_instance = source_class(config)
            experiment_logger.info(f"Using named configuration: {config_name}")
        else:
            # Use default configuration
            stats_source_instance = source_class()
            experiment_logger.info("Using default configuration")
        
        # Store the actual YAML that will be used
        actual_config_yaml = config_yaml
        if not actual_config_yaml:
            # If no custom YAML was provided, get the YAML from the configuration
            if hasattr(stats_source_instance.config, 'settings'):
                import yaml
                config_dict = {
                    'name': stats_source_instance.config.name,
                    'description': stats_source_instance.config.description,
                    'settings': stats_source_instance.config.settings
                }
                actual_config_yaml = yaml.dump(config_dict, default_flow_style=False)
        
        # Create experiment record
        try:
            log_and_callback("Creating experiment record...", 0, iterations)
            experiment = Experiment(
                name=name,
                stats_source=stats_source_instance.display_name(),
                config_name=config_name or 'default',
                config_yaml=actual_config_yaml,
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
                    execution_time, cost_estimate, query_plan = self._run_single_trial(experiment_db_session, query, transaction_handling)
                    execution_times.append(execution_time)
                    query_plans.append(query_plan)
                    
                    # Record trial
                    trial = Trial(
                        experiment_id=experiment.id,
                        run_index=i + 1,
                        execution_time=execution_time,
                        cost_estimate=cost_estimate
                    )
                    session.add(trial)
                    session.commit()
                    
                    # Log detailed trial information
                    query_logger.info(f"Trial {i + 1} completed successfully:")
                    query_logger.debug(f"Execution time: {execution_time:.4f}s")
                    query_logger.debug(f"Cost estimate: {cost_estimate:.2f}")
                    query_logger.debug(f"Query plan: {query_plan}")
                    
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
                experiment.experiment_logs = '\n'.join(experiment_logs)
                session.commit()
            except Exception:
                pass  # If we can't save, that's ok - the main error is more important
            
            raise
        finally:
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
    
    def _run_single_trial(self, session: Session, query: str, transaction_handling: str) -> Tuple[float, float, Dict[str, Any]]:
        """Run a single trial and return (execution_time, cost_estimate, query_plan)."""
        query_logger.debug(f"Starting new trial with query: {query} (transaction handling: {transaction_handling})")
        
        try:
            # Clear caches before trial
            session.commit()  # Commit any pending transaction
            
            # These operations need to be outside a transaction
            conn = session.connection().connection
            conn.set_session(autocommit=True)
            try:
                session.execute(text("DISCARD ALL"))
            finally:
                conn.set_session(autocommit=False)
            
            # These can run inside a transaction
            session.execute(text("SET LOCAL statement_timeout = 0"))
            session.execute(text("SET LOCAL work_mem = '16MB'"))
            session.execute(text("SET LOCAL maintenance_work_mem = '16MB'"))
            session.execute(text("SELECT pg_stat_reset()"))
            try:
                session.execute(text("SELECT pg_stat_statements_reset()"))
            except Exception:
                query_logger.debug("pg_stat_statements extension not available, skipping reset")
            session.commit()
            query_logger.debug("Cleared caches before trial")
            
            # Get query plan and cost estimate (this doesn't modify data)
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
            session.rollback()  # Rollback on error
            error_msg = f"Error during query execution: {str(e)}"
            query_logger.error(error_msg)
            query_logger.debug(traceback.format_exc())
            raise QueryExecutionError(error_msg) from e 