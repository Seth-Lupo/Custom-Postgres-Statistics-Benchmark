import os
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
import json
from fastapi import APIRouter, Request, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import text
from sqlmodel import Session, select, SQLModel
from ..database_sqlite import get_sqlite_db as get_db, sqlite_engine
from ..models import Experiment, Trial
from ..logging_config import web_logger
import plotly.graph_objects as go
import plotly.utils

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

# Ensure charts directory exists
os.makedirs("app/static/charts", exist_ok=True)


@router.get("/results", response_class=HTMLResponse)
def results_page(request: Request, session: Session = Depends(get_db)):
    """Render the results page with all experiments."""
    experiments = session.query(Experiment).order_by(Experiment.created_at.desc()).all()
    return templates.TemplateResponse("results.html", {
        "request": request,
        "experiments": experiments
    })


@router.get("/results/{experiment_id}")
def experiment_detail(experiment_id: int, request: Request, session: Session = Depends(get_db)):
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
def experiment_table(experiment_id: int, request: Request, session: Session = Depends(get_db)):
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
def experiment_chart(
    experiment_id: int,
    request: Request,
    chart_type: str = Query("bar", regex="^(bar|line|histogram)$"),
    session: Session = Depends(get_db)
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
def serve_chart(filename: str):
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


@router.get("/results/{experiment_id}/trial/{trial_id}/pg_stats")
def get_trial_pg_stats(experiment_id: int, trial_id: int, session: Session = Depends(get_db)):
    """Get pg_stats snapshot for a specific trial."""
    trial = session.query(Trial).filter(
        Trial.id == trial_id, 
        Trial.experiment_id == experiment_id
    ).first()
    
    if not trial:
        raise HTTPException(status_code=404, detail="Trial not found")
    
    if not trial.pg_stats_snapshot:
        return JSONResponse({"data": [], "columns": []})
    
    try:
        pg_stats_data = json.loads(trial.pg_stats_snapshot)
        
        # Define column headers for pg_stats
        columns = [
            {"key": "schemaname", "label": "Schema"},
            {"key": "tablename", "label": "Table"},
            {"key": "attname", "label": "Column"},
            {"key": "inherited", "label": "Inherited"},
            {"key": "null_frac", "label": "Null Fraction"},
            {"key": "avg_width", "label": "Avg Width"},
            {"key": "n_distinct", "label": "N Distinct"},
            {"key": "most_common_vals", "label": "Most Common Values"},
            {"key": "most_common_freqs", "label": "Most Common Freqs"},
            {"key": "histogram_bounds", "label": "Histogram Bounds"},
            {"key": "correlation", "label": "Correlation"},
            {"key": "most_common_elems", "label": "Most Common Elements"},
            {"key": "most_common_elem_freqs", "label": "Most Common Elem Freqs"},
            {"key": "elem_count_histogram", "label": "Element Count Histogram"}
        ]
        
        return JSONResponse({
            "data": pg_stats_data,
            "columns": columns,
            "title": f"pg_stats Snapshot - Trial {trial.run_index}"
        })
        
    except json.JSONDecodeError as e:
        web_logger.error(f"Failed to parse pg_stats snapshot for trial {trial_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to parse statistics data")


@router.get("/results/{experiment_id}/trial/{trial_id}/pg_statistic")
def get_trial_pg_statistic(experiment_id: int, trial_id: int, session: Session = Depends(get_db)):
    """Get pg_statistic snapshot for a specific trial."""
    trial = session.query(Trial).filter(
        Trial.id == trial_id, 
        Trial.experiment_id == experiment_id
    ).first()
    
    if not trial:
        raise HTTPException(status_code=404, detail="Trial not found")
    
    if not trial.pg_statistic_snapshot:
        return JSONResponse({"data": [], "columns": []})
    
    try:
        pg_statistic_data = json.loads(trial.pg_statistic_snapshot)
        
        # Define column headers for pg_statistic
        columns = [
            {"key": "table_name", "label": "Table Name"},
            {"key": "staattnum", "label": "Attribute Number"},
            {"key": "stainherit", "label": "Inherit"},
            {"key": "stanullfrac", "label": "Null Fraction"},
            {"key": "stawidth", "label": "Width"},
            {"key": "stadistinct", "label": "Distinct"},
            {"key": "stakind1", "label": "Kind 1"},
            {"key": "stakind2", "label": "Kind 2"},
            {"key": "stakind3", "label": "Kind 3"},
            {"key": "stakind4", "label": "Kind 4"},
            {"key": "stakind5", "label": "Kind 5"},
            {"key": "staop1", "label": "Op 1"},
            {"key": "staop2", "label": "Op 2"},
            {"key": "staop3", "label": "Op 3"},
            {"key": "staop4", "label": "Op 4"},
            {"key": "staop5", "label": "Op 5"},
            {"key": "stacoll1", "label": "Coll 1"},
            {"key": "stacoll2", "label": "Coll 2"},
            {"key": "stacoll3", "label": "Coll 3"},
            {"key": "stacoll4", "label": "Coll 4"},
            {"key": "stacoll5", "label": "Coll 5"},
            {"key": "stanumbers1", "label": "Numbers 1"},
            {"key": "stanumbers2", "label": "Numbers 2"},
            {"key": "stanumbers3", "label": "Numbers 3"},
            {"key": "stanumbers4", "label": "Numbers 4"},
            {"key": "stanumbers5", "label": "Numbers 5"},
            {"key": "stavalues1", "label": "Values 1"},
            {"key": "stavalues2", "label": "Values 2"},
            {"key": "stavalues3", "label": "Values 3"},
            {"key": "stavalues4", "label": "Values 4"},
            {"key": "stavalues5", "label": "Values 5"}
        ]
        
        return JSONResponse({
            "data": pg_statistic_data,
            "columns": columns,
            "title": f"pg_statistic Snapshot - Trial {trial.run_index}"
        })
        
    except json.JSONDecodeError as e:
        web_logger.error(f"Failed to parse pg_statistic snapshot for trial {trial_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to parse statistics data")


@router.get("/results/{experiment_id}/trial/{trial_id}/query_plan")
def get_trial_query_plan(experiment_id: int, trial_id: int, session: Session = Depends(get_db)):
    """Get query plan for a specific trial."""
    trial = session.query(Trial).filter(
        Trial.id == trial_id, 
        Trial.experiment_id == experiment_id
    ).first()
    
    if not trial:
        raise HTTPException(status_code=404, detail="Trial not found")
    
    if not trial.query_plan:
        return JSONResponse({"data": {}, "title": f"Query Plan - Trial {trial.run_index}", "message": "No query plan data available"})
    
    try:
        query_plan_data = json.loads(trial.query_plan)
        
        return JSONResponse({
            "data": query_plan_data,
            "title": f"Query Plan - Trial {trial.run_index}",
            "raw_json": trial.query_plan
        })
        
    except json.JSONDecodeError as e:
        web_logger.error(f"Failed to parse query plan for trial {trial_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to parse query plan data")


@router.get("/results/{experiment_id}/trial/{trial_id}/query_plan_viewer", response_class=HTMLResponse)
def query_plan_viewer(experiment_id: int, trial_id: int, request: Request, session: Session = Depends(get_db)):
    """Display query plan in pev2 viewer."""
    trial = session.query(Trial).filter(
        Trial.id == trial_id, 
        Trial.experiment_id == experiment_id
    ).first()
    
    if not trial:
        raise HTTPException(status_code=404, detail="Trial not found")
    
    # Get the experiment to access the query
    experiment = session.query(Experiment).filter(Experiment.id == experiment_id).first()
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")
    
    if not trial.query_plan:
        return templates.TemplateResponse("query_plan_viewer.html", {
            "request": request,
            "plan": None,
            "query": experiment.query,
            "title": f"Query Plan - Trial {trial.run_index}"
        })
    
    try:
        query_plan_data = json.loads(trial.query_plan)
        
        # Convert JSON plan to text format for pev2
        def json_plan_to_text(plan_json, indent=0):
            """Convert JSON query plan to PostgreSQL EXPLAIN text format."""
            if not plan_json or 'Plan' not in plan_json:
                return ""
            
            plan = plan_json['Plan']
            lines = []
            
            # Build the main line
            node_type = plan.get('Node Type', 'Unknown')
            
            # Add relation name if present
            if 'Relation Name' in plan:
                node_type += f" on {plan['Relation Name']}"
            elif 'Index Name' in plan:
                node_type += f" using {plan['Index Name']}"
            
            # Add cost and rows
            startup_cost = plan.get('Startup Cost', 0)
            total_cost = plan.get('Total Cost', 0)
            rows = plan.get('Plan Rows', 0)
            width = plan.get('Plan Width', 0)
            
            main_line = f"{' ' * indent}{node_type}  (cost={startup_cost:.2f}..{total_cost:.2f} rows={rows} width={width})"
            lines.append(main_line)
            
            # Add additional details
            if 'Filter' in plan:
                lines.append(f"{' ' * (indent + 2)}Filter: {plan['Filter']}")
            if 'Index Cond' in plan:
                lines.append(f"{' ' * (indent + 2)}Index Cond: {plan['Index Cond']}")
            if 'Join Filter' in plan:
                lines.append(f"{' ' * (indent + 2)}Join Filter: {plan['Join Filter']}")
            
            # Process child plans
            if 'Plans' in plan:
                for child_plan in plan['Plans']:
                    child_json = {'Plan': child_plan}
                    child_text = json_plan_to_text(child_json, indent + 2)
                    if child_text:
                        lines.append(child_text)
            
            return '\n'.join(lines)
        
        plan_text = json_plan_to_text(query_plan_data)
        
        # If conversion failed, try to use the raw JSON
        if not plan_text:
            plan_text = json.dumps(query_plan_data, indent=2)
        
        return templates.TemplateResponse("query_plan_viewer.html", {
            "request": request,
            "plan": plan_text,
            "query": experiment.query,
            "title": f"Query Plan - Trial {trial.run_index}"
        })
        
    except json.JSONDecodeError as e:
        web_logger.error(f"Failed to parse query plan for trial {trial_id}: {e}")
        return templates.TemplateResponse("query_plan_viewer.html", {
            "request": request,
            "plan": None,
            "query": experiment.query,
            "title": f"Query Plan - Trial {trial.run_index}",
            "error": "Failed to parse query plan data"
        })


@router.post("/clean-database")
def clean_database(session: Session = Depends(get_db)):
    """Clean the entire SQLite database by dropping all tables and recreating them."""
    try:
        web_logger.warning("Cleaning SQLite database - dropping all tables")
        
        # Drop all tables
        SQLModel.metadata.drop_all(sqlite_engine)
        web_logger.info("All tables dropped successfully")
        
        # Recreate all tables
        SQLModel.metadata.create_all(sqlite_engine)
        web_logger.info("All tables recreated successfully")
        
        # Redirect back to results page
        return RedirectResponse(url="/results", status_code=303)
        
    except Exception as e:
        web_logger.error(f"Failed to clean database: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to clean database: {str(e)}") 