import asyncio
import json
import os
from fastapi import APIRouter, Request, Form, Depends, HTTPException, BackgroundTasks, status
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession
from ..database import get_session
from ..experiment import ExperimentRunner, ExperimentError
from ..logging_config import web_logger
from fastapi.exception_handlers import RequestValidationError
from fastapi.exceptions import RequestValidationError
<<<<<<< HEAD
from sqlmodel import select
from ..models import Experiment as ExperimentModel
=======
>>>>>>> 1144569 (important checkpoint)

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

    # List available dump and query files
<<<<<<< HEAD
    uploads_dir = "app/uploads"
    dumps_dir = os.path.join(uploads_dir, "dumps")
    queries_dir = os.path.join(uploads_dir, "queries")
    
    dump_files = [f for f in os.listdir(dumps_dir) if f.endswith(('.sql', '.dump'))]
    query_files = [f for f in os.listdir(queries_dir) if f.endswith('.sql')]

   
=======
    uploads_dir = "app/app/uploads"
    dump_files = [f for f in os.listdir(uploads_dir) if f.endswith('.sql') or f.endswith('.dump')]
    query_files = [f for f in os.listdir(uploads_dir) if f.endswith('.sql')]
>>>>>>> 1144569 (important checkpoint)

    queries_not_available = len(query_files) == 0
    return templates.TemplateResponse("experiment.html", {
        "request": request,
        "stats_sources": stats_sources,
        "dump_files": dump_files,
        "query_files": query_files,
        "queries_not_available": queries_not_available,
    })


@router.post("/experiment")
async def run_experiment(
    request: Request,
    background_tasks: BackgroundTasks,
    experiment_name: str = Form(...),
    stats_source: str = Form(...),
    iterations: int = Form(...),
    dump_file: str = Form(...),
    query_file: str = Form(...),
<<<<<<< HEAD
    session: AsyncSession = Depends(get_session)
):
    """Launch an experiment in the background with selected files."""
    try:
        web_logger.info(f"Starting experiment '{experiment_name}' with {stats_source} source, {iterations} iterations, dump {dump_file}, query {query_file}")

        # Check if experiment with this name already exists and has been executed
        query = select(ExperimentModel).where(ExperimentModel.name == experiment_name)
        result = await session.execute(query)
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
=======
    session: Session = Depends(get_session)
):
    """Launch an experiment in the background with selected files."""
    try:
        web_logger.info(f"Starting experiment with {stats_source} source, {iterations} iterations, dump {dump_file}, query {query_file}")

        uploads_dir = "app/app/uploads"
        dump_path = os.path.join(uploads_dir, dump_file)
        query_path = os.path.join(uploads_dir, query_file)
>>>>>>> 1144569 (important checkpoint)

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
<<<<<<< HEAD
            "experiment": None,
            "name": experiment_name
        }
        web_logger.info(f"Created experiment with ID {experiment_id}")
        background_tasks.add_task(run_experiment_background, experiment_id, stats_source, query, iterations, dump_path, experiment_name, session)
=======
            "experiment": None
        }
        web_logger.info(f"Created experiment with ID {experiment_id}")
        background_tasks.add_task(run_experiment_background, experiment_id, stats_source, query, iterations)
>>>>>>> 1144569 (important checkpoint)
        return HTMLResponse(f"""
        <div id=\"experiment-result\">
            <div class=\"alert alert-info\">
                <strong>Experiment Started!</strong> Running {iterations} iterations with {stats_source}...<br>
<<<<<<< HEAD
                <span class=\"text-muted\">Name: {experiment_name} | Dump: {dump_file} | Query: {query_file}</span>
=======
                <span class=\"text-muted\">Dump: {dump_file} | Query: {query_file}</span>
>>>>>>> 1144569 (important checkpoint)
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
                <pre>Form data: {await request.form()}</pre>
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
                    
                    # Determine message level and format HTML
                    message_html = ""
                    for msg in messages:
                        css_class = "text-info"
                        if "❌" in msg:
                            css_class = "text-danger"
                            status["log_level"] = "error"
                        elif "⚠️" in msg:
                            css_class = "text-warning"
                            status["log_level"] = "warning"
                        
                        message_html += f"<div class='{css_class}'>{msg}</div>"
                    
                    yield "data: " + json.dumps({
                        "messages": messages,
                        "html": message_html,
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
            
            await asyncio.sleep(0.5)  # Poll every 500ms
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")


async def run_experiment_background(experiment_id: int, stats_source: str, query: str, iterations: int, dump_path: str, name: str, session: AsyncSession):
    """Run an experiment in the background."""
    
    try:
        from sqlmodel import Session, select
        from ..database import engine
        from ..models import Experiment as ExperimentModel
        from ..database import AsyncSessionLocal
        
        status = experiment_status[experiment_id]
        experiment_name = status["name"]

        def progress_callback(message: str, current: int, total: int):
            """Update experiment progress."""
            if experiment_id in experiment_status:
                status = experiment_status[experiment_id]
                status["progress"] = current
                status["messages"].append(message)

        # Run the experiment
        experiment = await experiment_runner.run_experiment(
            stats_source=stats_source,
            session=session,
            query=query,
            iterations=iterations,
            dump_path=dump_path,
            progress_callback=progress_callback,
            name=experiment_name,
        )

        # Update experiment status
        if experiment_id in experiment_status:
            status = experiment_status[experiment_id]
            status["status"] = "completed"
            status["experiment"] = experiment
            status["messages"].append("✅ Experiment completed successfully!")

            # Mark the experiment as executed in the database
            async with AsyncSessionLocal() as session:
                query = select(ExperimentModel).where(ExperimentModel.name == experiment_name)
                result = await session.execute(query)
                db_experiment = result.scalar_one_or_none()
                if db_experiment:
                    db_experiment.is_executed = True
                    session.add(db_experiment)
                    await session.commit()

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
        pass 