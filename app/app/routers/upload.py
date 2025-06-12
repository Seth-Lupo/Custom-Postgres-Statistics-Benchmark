import os
from fastapi import APIRouter, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

# Ensure upload directories exist
UPLOAD_DIR = "app/uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


@router.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    """Render the upload page."""
    return templates.TemplateResponse("upload.html", {"request": request})


@router.post("/upload/dump")
async def upload_dump(file: UploadFile = File(...)):
    """Handle database dump file upload."""
    try:
        if not file.filename.endswith(('.sql', '.dump')):
            return """<div id="dump-status" class="alert alert-danger">
                <strong>Error!</strong> Only .sql and .dump files are allowed.
            </div>"""
        
        # Save the uploaded file
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        return f"""<div id="dump-status" class="alert alert-success">
            <strong>Success!</strong> Database dump uploaded successfully: {file.filename}
        </div>"""
    except Exception as e:
        return f"""<div id="dump-status" class="alert alert-danger">
            <strong>Error!</strong> Failed to upload file: {str(e)}
        </div>"""


@router.post("/upload/queries")
async def upload_queries(file: UploadFile = File(...)):
    """Handle queries file upload."""
    try:
        if not file.filename.endswith('.sql'):
            return """<div id="queries-status" class="alert alert-danger">
                <strong>Error!</strong> Only .sql files are allowed.
            </div>"""
        
        # Save the uploaded file
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        return f"""<div id="queries-status" class="alert alert-success">
            <strong>Success!</strong> Queries file uploaded successfully: {file.filename}
        </div>"""
    except Exception as e:
        return f"""<div id="queries-status" class="alert alert-danger">
            <strong>Error!</strong> Failed to upload file: {str(e)}
        </div>""" 