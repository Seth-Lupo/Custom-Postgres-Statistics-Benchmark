from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from .database import create_db_and_tables
from .routers import upload, run, results

templates = Jinja2Templates(directory="app/templates")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    create_db_and_tables()
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
    """Home page with navigation."""
    return templates.TemplateResponse("base.html", {
        "request": request,
        "show_navigation": True
    })


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"} 