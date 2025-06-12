import os
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
from fastapi import APIRouter, Request, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import text
from ..database import get_session
from ..models import Experiment, Trial

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

# Ensure charts directory exists
os.makedirs("app/static/charts", exist_ok=True)


@router.get("/results", response_class=HTMLResponse)
async def results_page(request: Request, session: Session = Depends(get_session)):
    """Render the results page with all experiments."""
    experiments = session.query(Experiment).order_by(Experiment.created_at.desc()).all()
    return templates.TemplateResponse("results.html", {
        "request": request,
        "experiments": experiments
    })


@router.get("/results/{experiment_id}")
async def experiment_detail(experiment_id: int, request: Request, session: Session = Depends(get_session)):
    """Show detailed results for a specific experiment."""
    experiment = session.query(Experiment).filter(Experiment.id == experiment_id).first()
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    trials = session.query(Trial).filter(Trial.experiment_id == experiment_id).all()
    
    return templates.TemplateResponse("experiment_detail.html", {
        "request": request,
        "experiment": experiment,
        "trials": trials
    })


@router.get("/results/{experiment_id}/table")
async def experiment_table(experiment_id: int, request: Request, session: Session = Depends(get_session)):
    """Return HTMX fragment with experiment trial table."""
    experiment = session.query(Experiment).filter(Experiment.id == experiment_id).first()
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    trials = session.query(Trial).filter(Trial.experiment_id == experiment_id).all()
    
    return templates.TemplateResponse("_partials/_results_table.html", {
        "request": request,
        "experiment": experiment,
        "trials": trials
    })


@router.get("/results/{experiment_id}/chart")
async def experiment_chart(
    experiment_id: int,
    request: Request,
    chart_type: str = Query("bar", regex="^(bar|line|histogram)$"),
    session: Session = Depends(get_session)
):
    """Generate and return chart for experiment."""
    experiment = session.query(Experiment).filter(Experiment.id == experiment_id).first()
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    trials = session.query(Trial).filter(Trial.experiment_id == experiment_id).all()
    
    # Generate chart
    chart_path = generate_chart(experiment, trials, chart_type)
    
    return templates.TemplateResponse("_partials/_chart_img.html", {
        "request": request,
        "chart_path": f"/static/charts/{os.path.basename(chart_path)}",
        "experiment_id": experiment_id,
        "chart_type": chart_type
    })


@router.get("/static/charts/{filename}")
async def serve_chart(filename: str):
    """Serve chart images."""
    file_path = f"app/static/charts/{filename}"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Chart not found")
    return FileResponse(file_path)


def generate_chart(experiment: Experiment, trials: list, chart_type: str) -> str:
    """Generate a chart for the experiment results."""
    plt.figure(figsize=(10, 6))
    
    execution_times = [trial.execution_time for trial in trials]
    trial_numbers = [trial.run_index for trial in trials]

    print(chart_type)
    
    if chart_type == "bar":
        plt.bar(trial_numbers, execution_times)
        plt.xlabel('Trial Number')
        plt.ylabel('Execution Time (seconds)')
        plt.title(f'Execution Times by Trial - {experiment.stats_source}')
        
    elif chart_type == "line":
        plt.plot(trial_numbers, execution_times, marker='o')
        plt.xlabel('Trial Number')
        plt.ylabel('Execution Time (seconds)')
        plt.title(f'Execution Times Trend - {experiment.stats_source}')
        
    elif chart_type == "histogram":
        plt.hist(execution_times, bins=min(20, len(execution_times)), edgecolor='black')
        plt.xlabel('Execution Time (seconds)')
        plt.ylabel('Frequency')
        plt.title(f'Execution Time Distribution - {experiment.stats_source}')
        
        # Add mean and std dev lines
        mean_time = np.mean(execution_times)
        std_time = np.std(execution_times)
        plt.axvline(mean_time, color='red', linestyle='--', label=f'Mean: {mean_time:.4f}s')
        plt.axvline(mean_time + std_time, color='orange', linestyle='--', alpha=0.7, label=f'±1 Std Dev')
        plt.axvline(mean_time - std_time, color='orange', linestyle='--', alpha=0.7)
        plt.legend()
    
    plt.tight_layout()
    
    # Save chart
    chart_filename = f"{experiment.id}_{chart_type}.png"
    chart_path = f"app/static/charts/{chart_filename}"
    plt.savefig(chart_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    return chart_path 