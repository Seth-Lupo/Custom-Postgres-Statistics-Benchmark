from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from .database import create_db_and_tables, init_db, AsyncSessionLocal
from .routers import upload, run, results
from sqlmodel import select
from .models import Experiment

templates = Jinja2Templates(directory="app/templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    await init_db()
    await create_db_and_tables()
    yield
    # Shutdown
    pass


app = FastAPI(
    title="PostgreSQL Statistics Benchmarking Platform",
    description="A platform for benchmarking PostgreSQL query performance with different statistics sources",
    version="1.0.0",
    lifespan=lifespan
)

# Mount static files
app.mount("/static", StaticFiles(directory="app/static"), name="static")

# Include routers
app.include_router(upload.router, tags=["upload"])
app.include_router(run.router, tags=["experiment"])
app.include_router(results.router, tags=["results"])


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Home page with navigation and status information."""
    # Get experiment count using async session
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Experiment))
        experiments = await result.scalars().all()
        experiment_count = len(experiments)

    # Check for uploaded files
    import os
    uploads_dir = "app/uploads"
    dumps_dir = os.path.join(uploads_dir, "dumps")
    queries_dir = os.path.join(uploads_dir, "queries")
    
    dump_files = [f for f in os.listdir(dumps_dir) if f.endswith(('.sql', '.dump'))] if os.path.exists(dumps_dir) else []
    query_files = [f for f in os.listdir(queries_dir) if f.endswith('.sql')] if os.path.exists(queries_dir) else []

    return templates.TemplateResponse("home.html", {
        "request": request,
        "show_navigation": True,
        "has_dump": len(dump_files) > 0,
        "has_queries": len(query_files) > 0,
        "experiment_count": experiment_count
    })


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"} 