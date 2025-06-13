import time
import statistics
import traceback
from typing import List, Tuple, Callable, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from .models import Experiment, Trial
from .stats_sources.base import StatsSource
from .stats_sources.direct_pg import DirectPgStatsSource
from .stats_sources.random_pg import RandomPgStatsSource
from .logging_config import experiment_logger, query_logger, stats_logger
from .database import create_database, drop_database, load_dump, get_db_session

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
            "direct": DirectPgStatsSource(),
            "random": RandomPgStatsSource(),
        }
    
    def get_available_stats_sources(self) -> List[Tuple[str, str]]:
        """Get list of available statistics sources as (key, display_name) tuples."""
        return [(key, source.name()) for key, source in self.stats_sources.items()]
    
    async def run_experiment(self, session: AsyncSession, stats_source: str, query: str, iterations: int, progress_callback: Callable[[str, int, int], None], dump_path: str, name: str) -> Experiment:
        
        db_name = "test_database"
        
        """Run a complete experiment and return the result."""
        experiment_logger.info(f"Starting new experiment with {stats_source} stats source")
        experiment_logger.debug(f"Query: {query}")
        experiment_logger.debug(f"Iterations: {iterations}")
        experiment_logger.info(f"Using temporary database: {db_name} from dump: {dump_path}")
        
        if stats_source not in self.stats_sources:
            error_msg = f"Unknown stats source: {stats_source}"
            experiment_logger.error(error_msg)
            raise ValueError(error_msg)
        
        stats_source = self.stats_sources[stats_source]
        
        # Create experiment record
        try:
            progress_callback("Creating experiment record...", 0, iterations)
            experiment = Experiment(
                name=name,
                stats_source=stats_source.name(),
                query=query,
                iterations=iterations
            )
            session.add(experiment)
            await session.commit()
            await session.refresh(experiment)
            experiment_logger.info(f"Created experiment record with ID {experiment.id}")
            
        except SQLAlchemyError as e:
            error_msg = f"Failed to create experiment record: {str(e)}"
            experiment_logger.error(error_msg)
            experiment_logger.debug(traceback.format_exc())
            raise ExperimentError(error_msg) from e
        
        experiment_db_session = None
        try:
            # Create and setup the temporary database
            progress_callback(f"Creating temporary database '{db_name}'...", 0, iterations)
            await create_database(db_name)
            progress_callback(f"Loading data from '{dump_path}'...", 0, iterations)
            load_dump(db_name, dump_path)

            # Get a session to the temporary database
            experiment_db_session_generator = get_db_session(db_name)
            experiment_db_session = await anext(experiment_db_session_generator)

            # Apply statistics
            msg = f"Applying {stats_source.name()} statistics to database..."
            progress_callback(msg, 0, iterations)
            experiment_logger.info(msg)
            
            try:
                await stats_source.apply_statistics(experiment_db_session)
                stats_logger.info(f"Successfully applied {stats_source.name()} statistics")
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
                progress_callback(trial_msg, i, iterations)
                experiment_logger.info(trial_msg)
                
                try:
                    execution_time, cost_estimate, query_plan = await self._run_single_trial(experiment_db_session, query)
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
                    await session.commit()
                    
                    # Log detailed trial information
                    query_logger.info(f"Trial {i + 1} completed successfully:")
                    query_logger.debug(f"Execution time: {execution_time:.4f}s")
                    query_logger.debug(f"Cost estimate: {cost_estimate:.2f}")
                    query_logger.debug(f"Query plan: {query_plan}")
                    
                    result_msg = f"Trial {i + 1} completed: Time={execution_time:.4f}s, Cost={cost_estimate:.2f}"
                    progress_callback(result_msg, i + 1, iterations)
                    
                except Exception as e:
                    error_msg = f"Error in trial {i + 1}: {str(e)}"
                    query_logger.error(error_msg)
                    query_logger.debug(traceback.format_exc())
                    progress_callback(f"⚠️ {error_msg}", i + 1, iterations)
                    raise QueryExecutionError(error_msg) from e
            
            # Calculate aggregate statistics
            progress_callback("Calculating final statistics...", iterations, iterations)
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
            
            await session.commit()
            await session.refresh(experiment)
            
            final_msg = (
                f"Experiment completed! "
                f"Average time: {experiment.avg_time:.4f}s ± {experiment.stddev_time:.4f}s\n"
                f"Min: {min(execution_times):.4f}s, Max: {max(execution_times):.4f}s"
            )
            progress_callback(final_msg, iterations, iterations)
            experiment_logger.info("Experiment completed successfully")
            
            return experiment
            
        except Exception as e:
            await session.rollback()
            error_msg = f"Experiment failed: {str(e)}"
            experiment_logger.error(error_msg)
            experiment_logger.debug(traceback.format_exc())
            progress_callback(f"❌ {error_msg}", iterations, iterations)
            raise
        finally:
            if experiment_db_session:
                await experiment_db_session.close()
            # Drop the temporary database
            progress_callback(f"Cleaning up temporary database '{db_name}'...", iterations, iterations)
            await drop_database(db_name)
            experiment_logger.info(f"Dropped temporary database: {db_name}")
    
    async def _run_single_trial(self, session: AsyncSession, query: str) -> Tuple[float, float, Dict[str, Any]]:
        """Run a single trial and return (execution_time, cost_estimate, query_plan)."""
        query_logger.debug(f"Starting new trial with query: {query}")
        
        try:
            # Get query plan and cost estimate
            explain_query = text(f"EXPLAIN (FORMAT JSON) {query}")
            explain_result = await session.execute(explain_query)
            explain_result = await explain_result.fetchone()
            
            if not explain_result or not explain_result[0]:
                error_msg = "Failed to get query plan"
                query_logger.error(error_msg)
                raise QueryExecutionError(error_msg)
            
            plan_data = explain_result[0]
            query_plan = plan_data[0] if isinstance(plan_data, list) and len(plan_data) > 0 else {}
            cost_estimate = query_plan.get("Plan", {}).get("Total Cost", 0.0)
            
            query_logger.debug(f"Query plan obtained: {query_plan}")
            query_logger.debug(f"Estimated cost: {cost_estimate}")
            
            # Execute query and measure time
            query_logger.debug("Executing query...")
            start_time = time.perf_counter()
            await session.execute(text(query))
            await session.commit()  # Commit transaction to ensure execution
            end_time = time.perf_counter()
            
            execution_time = end_time - start_time
            query_logger.debug(f"Query executed in {execution_time:.4f} seconds")
            
            return execution_time, cost_estimate, query_plan
            
        except Exception as e:
            await session.rollback()  # Rollback on error
            error_msg = f"Error during query execution: {str(e)}"
            query_logger.error(error_msg)
            query_logger.debug(traceback.format_exc())
            raise QueryExecutionError(error_msg) from e 