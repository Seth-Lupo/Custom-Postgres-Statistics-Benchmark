import asyncio
import json
import os
from fastapi import APIRouter, Request, Form, Depends, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from ..database import get_session
from ..experiment import ExperimentRunner, ExperimentError
from ..logging_config import web_logger

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

# Global experiment runner
experiment_runner = ExperimentRunner()

# In-memory storage for experiment status (in production, use Redis or similar)
experiment_status = {}


@router.get("/experiment", response_class=HTMLResponse)
async def experiment_page(request: Request):
    """Render the experiment page."""
    web_logger.info("Loading experiment page")
    stats_sources = experiment_runner.get_available_stats_sources()
    
    # Check if queries file exists
    queries_available = os.path.exists("samples/queries.sql")
    web_logger.debug(f"Queries file available: {queries_available}")
    
    return templates.TemplateResponse("experiment.html", {
        "request": request,
        "stats_sources": stats_sources,
        "queries_available": queries_available
    })


@router.post("/experiment")
async def run_experiment(
    request: Request,
    background_tasks: BackgroundTasks,
    stats_source: str = Form(...),
    iterations: int = Form(...),
    session: Session = Depends(get_session)
):
    """Launch an experiment in the background."""
    web_logger.info(f"Starting experiment with {stats_source} source and {iterations} iterations")
    
    # Read queries from file
    if not os.path.exists("samples/queries.sql"):
        error_msg = "No queries file uploaded"
        web_logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    
    try:
        with open("samples/queries.sql", "r") as f:
            queries_content = f.read()
        web_logger.debug("Successfully read queries file")
        
        # Split queries (simple splitting by semicolon)
        queries = [q.strip() for q in queries_content.split(';') if q.strip()]
        
        if not queries:
            error_msg = "No valid queries found in file"
            web_logger.error(error_msg)
            raise HTTPException(status_code=400, detail=error_msg)
        
        # For now, use the first query
        query = queries[0]
        web_logger.debug(f"Using query: {query}")
        
        # Start experiment in background
        experiment_id = len(experiment_status) + 1
        experiment_status[experiment_id] = {
            "status": "running",
            "progress": 0,
            "total": iterations,
            "messages": [],
            "log_level": "info",  # Can be info, warning, or error
            "experiment": None
        }
        
        web_logger.info(f"Created experiment with ID {experiment_id}")
        background_tasks.add_task(run_experiment_background, experiment_id, stats_source, query, iterations)
        
        return f"""
        <div id="experiment-result">
            <div class="alert alert-info">
                <strong>Experiment Started!</strong> Running {iterations} iterations with {stats_source}...
            </div>
            <div class="progress mb-3">
                <div class="progress-bar" role="progressbar" style="width: 0%" id="progress-bar-{experiment_id}">0%</div>
            </div>
            <div class="card">
                <div class="card-header d-flex justify-content-between align-items-center">
                    <h5 class="mb-0">Experiment Progress</h5>
                    <div class="btn-group btn-group-sm" role="group">
                        <button type="button" class="btn btn-outline-secondary active" onclick="setLogLevel('info', this)">Info</button>
                        <button type="button" class="btn btn-outline-warning" onclick="setLogLevel('warning', this)">Warnings</button>
                        <button type="button" class="btn btn-outline-danger" onclick="setLogLevel('error', this)">Errors</button>
                    </div>
                </div>
                <div class="card-body">
                    <pre id="progress-log-{experiment_id}" style="height: 300px; overflow-y: auto;" class="mb-0 bg-light p-3"></pre>
                </div>
            </div>
            <div hx-sse="connect:/experiment/stream/{experiment_id}">
                <div hx-sse="swap:message">
                </div>
            </div>
        </div>
        """
    except Exception as e:
        error_msg = f"Failed to start experiment: {str(e)}"
        web_logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)


@router.get("/experiment/stream/{experiment_id}")
async def experiment_stream(experiment_id: int):
    """Stream experiment progress via SSE."""
    async def event_generator():
        while True:
            if experiment_id not in experiment_status:
                yield "data: " + json.dumps({"error": "Experiment not found"}) + "\n\n"
                break
            
            status = experiment_status[experiment_id]
            
            if status["status"] == "running":
                # Get any new messages
                messages = status["messages"]
                if messages:
                    progress_percent = int((status["progress"] / status["total"]) * 100)
                    
                    # Determine message level
                    if any("❌" in msg for msg in messages):
                        status["log_level"] = "error"
                    elif any("⚠️" in msg for msg in messages):
                        status["log_level"] = "warning"
                    
                    yield "data: " + json.dumps({
                        "messages": messages,
                        "progress": progress_percent,
                        "status": "running",
                        "log_level": status["log_level"]
                    }) + "\n\n"
                    status["messages"] = []  # Clear processed messages
            
            elif status["status"] == "completed":
                exp_id = status["experiment"].id
                yield "data: " + json.dumps({
                    "status": "completed",
                    "progress": 100,
                    "log_level": "info",
                    "html": f"""
                    <div class="alert alert-success">
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
                    <div class="alert alert-danger">
                        <strong>Experiment Failed:</strong> {status.get("error", "Unknown error")}
                    </div>
                    """
                }) + "\n\n"
                break
            
            await asyncio.sleep(0.5)  # Poll every 500ms
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


def run_experiment_background(experiment_id: int, stats_source: str, query: str, iterations: int):
    """Run experiment in background thread."""
    from ..database import SessionLocal
    
    session = SessionLocal()
    try:
        def progress_callback(message: str, current: int, total: int):
            experiment_status[experiment_id]["progress"] = current
            experiment_status[experiment_id]["messages"].append(message)
        
        # Run the experiment
        experiment = experiment_runner.run_experiment(
            session, 
            stats_source, 
            query, 
            iterations,
            progress_callback
        )
        
        experiment_status[experiment_id]["status"] = "completed"
        experiment_status[experiment_id]["experiment"] = experiment
        web_logger.info(f"Experiment {experiment_id} completed successfully")
        
    except ExperimentError as e:
        error_msg = str(e)
        web_logger.error(f"Experiment {experiment_id} failed with ExperimentError: {error_msg}")
        experiment_status[experiment_id]["status"] = "error"
        experiment_status[experiment_id]["error"] = error_msg
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        web_logger.error(f"Experiment {experiment_id} failed with unexpected error: {error_msg}")
        experiment_status[experiment_id]["status"] = "error"
        experiment_status[experiment_id]["error"] = error_msg
        
    finally:
        session.close() 