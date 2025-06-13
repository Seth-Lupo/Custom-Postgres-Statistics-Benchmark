import asyncio
import json
import os
from fastapi import APIRouter, Request, Form, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from ..database import get_db, SessionLocal
from ..experiment import ExperimentRunner, ExperimentError
from ..logging_config import web_logger
from fastapi.exception_handlers import RequestValidationError
from fastapi.exceptions import RequestValidationError
from sqlmodel import select, Session
from ..models import Experiment as ExperimentModel
from datetime import datetime

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

# Global experiment runner
experiment_runner = ExperimentRunner()

# In-memory storage for experiment status (in production, use Redis or similar)
experiment_status = {}


@router.get("/experiment", response_class=HTMLResponse)
def experiment_page(request: Request):
    """Render the experiment page."""
    web_logger.info("Loading experiment page")
    src = experiment_runner.get_available_src()

    # List available dump and query files
    uploads_dir = "app/uploads"
    dumps_dir = os.path.join(uploads_dir, "dumps")
    queries_dir = os.path.join(uploads_dir, "queries")
    
    dump_files = [f for f in os.listdir(dumps_dir) if f.endswith(('.sql', '.dump'))]
    query_files = [f for f in os.listdir(queries_dir) if f.endswith('.sql')]

   

    queries_not_available = len(query_files) == 0
    return templates.TemplateResponse("experiment.html", {
        "request": request,
        "src": src,
        "dump_files": dump_files,
        "query_files": query_files,
        "queries_not_available": queries_not_available,
    })


@router.get("/experiment/configs/{stats_source}")
def get_configs(stats_source: str):
    """Get available configurations for a stats source."""
    try:
        configs = experiment_runner.get_available_configs(stats_source)
        return JSONResponse({
            "configs": configs
        })
    except Exception as e:
        web_logger.error(f"Failed to get configs for {stats_source}: {str(e)}")
        return JSONResponse({
            "error": f"Failed to get configurations: {str(e)}"
        }, status_code=500)


@router.get("/experiment/configs/{stats_source}/{config_name}/yaml")
def get_config_yaml(stats_source: str, config_name: str):
    """Get the raw YAML content for a specific configuration."""
    try:
        if stats_source not in experiment_runner.src:
            return JSONResponse({
                "error": f"Unknown stats source: {stats_source}"
            }, status_code=404)
        
        source_class = experiment_runner.src[stats_source]
        instance = source_class()
        config_path = instance._get_config_path(f"{config_name}.yaml")
        
        if not config_path.exists():
            return JSONResponse({
                "error": f"Configuration file not found: {config_name}"
            }, status_code=404)
        
        with open(config_path, 'r') as f:
            yaml_content = f.read()
        
        return JSONResponse({
            "yaml": yaml_content
        })
    except Exception as e:
        web_logger.error(f"Failed to get YAML for {stats_source}/{config_name}: {str(e)}")
        return JSONResponse({
            "error": f"Failed to get configuration YAML: {str(e)}"
        }, status_code=500)


@router.post("/experiment")
def run_experiment(
    request: Request,
    background_tasks: BackgroundTasks,
    experiment_name: str = Form(...),
    stats_source: str = Form(...),
    config_name: str = Form(None),
    config_yaml: str = Form(None),
    iterations: int = Form(...),
    stats_reset_strategy: str = Form(...),
    transaction_handling: str = Form(...),
    dump_file: str = Form(...),
    query_file: str = Form(...),
    db: Session = Depends(get_db)
):
    """Launch an experiment in the background with selected files."""
    try:
        config_display = f"config '{config_name}'" if config_name else "default config"
        web_logger.info(f"Starting experiment '{experiment_name}' with {stats_source} source ({config_display}), {iterations} iterations")
        web_logger.info(f"Stats reset strategy: {stats_reset_strategy}, Transaction handling: {transaction_handling}")
        web_logger.info(f"Dump: {dump_file}, Query: {query_file}")
        
        if config_yaml:
            web_logger.debug(f"Using custom configuration: {config_yaml[:200]}...")  # Log first 200 chars

        # Check if experiment with this name already exists and has been executed
        query = select(ExperimentModel).where(ExperimentModel.name == experiment_name)
        result = db.execute(query)
        existing_experiment = result.scalar_one_or_none()
        
        if existing_experiment and existing_experiment.is_executed:
            error_msg = f"An experiment with the name '{experiment_name}' has already been executed. Please choose a different name."
            web_logger.error(error_msg)
            return HTMLResponse(f"""
                <div class='alert alert-danger'>
                    <strong>Error:</strong> {error_msg}
                </div>
            """, status_code=400)

        uploads_dir = "app/uploads"
        dump_path = os.path.join(uploads_dir, "dumps", dump_file)
        query_path = os.path.join(uploads_dir, "queries", query_file)

        # Validate files exist
        if not os.path.exists(dump_path):
            error_msg = f"Selected dump file not found: {dump_file}"
            web_logger.error(error_msg)
            return HTMLResponse(f"""
                <div class='alert alert-danger'>
                    <strong>Error:</strong> {error_msg}<br>
                    <pre>dump_path: {dump_path}</pre>
                </div>
            """, status_code=400)
        if not os.path.exists(query_path):
            error_msg = f"Selected query file not found: {query_file}"
            web_logger.error(error_msg)
            return HTMLResponse(f"""
                <div class='alert alert-danger'>
                    <strong>Error:</strong> {error_msg}<br>
                    <pre>query_path: {query_path}</pre>
                </div>
            """, status_code=400)

        with open(query_path, "r") as f:
            queries_content = f.read()
        web_logger.debug("Successfully read queries file")
        queries = [q.strip() for q in queries_content.split(';') if q.strip()]
        if not queries:
            error_msg = "No valid queries found in file"
            web_logger.error(error_msg)
            return HTMLResponse(f"""
                <div class='alert alert-danger'>
                    <strong>Error:</strong> {error_msg}<br>
                    <pre>query_file: {query_file}</pre>
                </div>
            """, status_code=400)
        query = queries[0]  # For now, use the first query
        web_logger.debug(f"Using query: {query}")

        # TODO: Restore the selected dump file to the database before running the experiment
        # This may require additional logic depending on your DB setup

        experiment_id = len(experiment_status) + 1
        experiment_status[experiment_id] = {
            "status": "running",
            "progress": 0,
            "total": iterations,
            "messages": [],
            "log_level": "info",
            "experiment": None,
            "name": experiment_name
        }
        web_logger.info(f"Created experiment with ID {experiment_id}")
        background_tasks.add_task(run_experiment_background, experiment_id, stats_source, config_name, config_yaml, query, iterations, stats_reset_strategy, transaction_handling, dump_path, experiment_name)
        return HTMLResponse(f"""
        <div id=\"experiment-result\">
            <div class=\"alert alert-info\">
                <strong>Experiment Started!</strong> Running {iterations} iterations with {stats_source}...<br>
                <span class=\"text-muted\">Name: {experiment_name} | Stats: {stats_reset_strategy} | Transaction: {transaction_handling}</span><br>
                <span class=\"text-muted\">Dump: {dump_file} | Query: {query_file}</span>
            </div>
            <div class=\"progress mb-3\">
                <div class=\"progress-bar\" role=\"progressbar\" style=\"width: 0%\" id=\"progress-bar-{experiment_id}\">0%</div>
            </div>
            <div class=\"card\">
                <div class=\"card-header d-flex justify-content-between align-items-center\">
                    <h5 class=\"mb-0\">Experiment Progress</h5>
                    <div class=\"btn-group btn-group-sm\" role=\"group\">
                        <button type=\"button\" class=\"btn btn-outline-secondary active\" onclick=\"setLogLevel('info', this)\">Info</button>
                        <button type=\"button\" class=\"btn btn-outline-warning\" onclick=\"setLogLevel('warning', this)\">Warnings</button>
                        <button type=\"button\" class=\"btn btn-outline-danger\" onclick=\"setLogLevel('error', this)\">Errors</button>
                    </div>
                </div>
                <div class=\"card-body\">
                    <pre id=\"progress-log-{experiment_id}\" style=\"height: 300px; overflow-y: auto;\" class=\"mb-0 bg-light p-3\"></pre>
                </div>
            </div>
            <div hx-sse=\"connect:/experiment/stream/{experiment_id}\">
                <div hx-sse=\"swap:message\">
                </div>
            </div>
        </div>
        """)
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        error_msg = f"Failed to start experiment: {str(e)}"
        web_logger.error(error_msg)
        return HTMLResponse(f"""
            <div class='alert alert-danger'>
                <strong>Exception:</strong> {error_msg}<br>
                <pre>{tb}</pre>
            </div>
        """, status_code=500)


@router.get("/experiment/stream/{experiment_id}")
async def experiment_stream(experiment_id: int):
    """Stream experiment progress via SSE."""
    async def event_generator():
        while True:
            if experiment_id not in experiment_status:
                yield "data: " + json.dumps({
                    "error": "Experiment not found",
                    "html": """
                        <div class='alert alert-danger'>
                            <strong>Error:</strong> Experiment not found
                        </div>
                    """
                }) + "\n\n"
                break
            
            status = experiment_status[experiment_id]
            
            if status["status"] == "running":
                # Get any new messages
                messages = status["messages"]
                if messages:
                    progress_percent = int((status["progress"] / status["total"]) * 100)
                    
                    # Determine log level from messages (consistent approach)
                    current_log_level = "info"
                    for msg in messages:
                        if "❌" in msg or "ERROR" in msg.upper():
                            current_log_level = "error"
                            break
                        elif "⚠️" in msg or "WARNING" in msg.upper():
                            current_log_level = "warning"
                    
                    yield "data: " + json.dumps({
                        "messages": messages,
                        "progress": progress_percent,
                        "status": "running",
                        "log_level": current_log_level
                    }) + "\n\n"
                    status["messages"] = []  # Clear processed messages
            
            elif status["status"] == "completed":
                # Check if final logs are ready and send them first
                
                exp_id = status["experiment"].id
                yield "data: " + json.dumps({
                    "status": "completed",
                    "progress": 100,
                    "log_level": "info",
                    "html": f"""
                        <div class="alert alert-success mb-3">
                            <strong>Experiment Completed!</strong>
                            <br>
                            <a href="/results/{exp_id}" class="btn btn-primary mt-2">View Results</a>
                        </div>
                    """
                }) + "\n\n"
                break
            
            elif status["status"] == "error":
               
                
                yield "data: " + json.dumps({
                    "status": "error",
                    "progress": 100,
                    "log_level": "error", 
                    "html": f"""
                        <div class="alert alert-danger mb-3">
                            <strong>Experiment Failed:</strong> {status.get("error", "Unknown error")}
                        </div>
                    """
                }) + "\n\n"
                break
            
            await asyncio.sleep(0.05)  # Poll every 200ms for better responsiveness
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


def run_experiment_background(experiment_id: int, stats_source: str, config_name: str, config_yaml: str, query: str, iterations: int, stats_reset_strategy: str, transaction_handling: str, dump_path: str, name: str):
    """Run the experiment in the background."""
    db: Session = SessionLocal()
    try:
        def progress_callback(message: str, current: int, total: int):
            """Update experiment progress."""
            progress_percent = int((current / total) * 100)
            status = experiment_status[experiment_id]
            status["progress"] = current
            status["total"] = total
            
            # Send the message as it will be stored in database (with timestamp)
            timestamped_message = f"[{datetime.utcnow().strftime('%H:%M:%S')}] {message}"
            status["messages"].append(timestamped_message)
            web_logger.debug(f"Experiment {experiment_id} progress: {progress_percent}% - {message}")

        web_logger.info(f"Running experiment {experiment_id} in background")
        experiment = experiment_runner.run_experiment(
            session=db,
            stats_source=stats_source,
            config_name=config_name,
            config_yaml=config_yaml,
            query=query,
            iterations=iterations,
            stats_reset_strategy=stats_reset_strategy,
            transaction_handling=transaction_handling,
            dump_path=dump_path,
            progress_callback=progress_callback,
            name=name
        )
        
        
        experiment_status[experiment_id]["status"] = "completed"
        experiment_status[experiment_id]["experiment"] = experiment
        web_logger.info(f"Experiment {experiment_id} completed successfully.")
        
    except ExperimentError as e:
        web_logger.error(f"Experiment {experiment_id} failed: {e}")
        experiment_status[experiment_id]["status"] = "error"
        experiment_status[experiment_id]["error"] = str(e)
            
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        web_logger.error(f"An unexpected error occurred in experiment {experiment_id}: {e}\n{tb}")
        experiment_status[experiment_id]["status"] = "error"
        experiment_status[experiment_id]["error"] = f"An unexpected error occurred: {str(e)}"
    finally:
        db.close() 