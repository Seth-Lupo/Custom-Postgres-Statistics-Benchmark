import os
from fastapi import APIRouter, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from typing import List, Dict
import datetime

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()

# Ensure upload directories exist
UPLOAD_DIR = "app/uploads"
DUMPS_DIR = os.path.join(UPLOAD_DIR, "dumps")
QUERIES_DIR = os.path.join(UPLOAD_DIR, "queries")
os.makedirs(DUMPS_DIR, exist_ok=True)
os.makedirs(QUERIES_DIR, exist_ok=True)


def get_file_info(filepath: str) -> Dict:
    """Get file information including size and last modified date."""
    stats = os.stat(filepath)
    return {
        "name": os.path.basename(filepath),
        "size": stats.st_size,
        "modified": datetime.datetime.fromtimestamp(stats.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        "path": filepath
    }


def get_directory_contents(directory: str) -> List[Dict]:
    """Get list of files and their information from a directory."""
    files = []
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            files.append(get_file_info(filepath))
    return sorted(files, key=lambda x: x["modified"], reverse=True)


def format_file_size(size_in_bytes: int) -> str:
    """Convert file size in bytes to human readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.1f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.1f} TB"


@router.get("/upload", response_class=HTMLResponse)
async def upload_page(request: Request):
    """Render the upload page with existing files information."""
    existing_dumps = get_directory_contents(DUMPS_DIR)
    existing_queries = get_directory_contents(QUERIES_DIR)
    
    # Add formatted size to each file
    for file in existing_dumps + existing_queries:
        file["formatted_size"] = format_file_size(file["size"])
    
    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "existing_dumps": existing_dumps,
            "existing_queries": existing_queries
        }
    )


@router.post("/upload/dump")
async def upload_dump(files: List[UploadFile] = File(...)):
    """Handle database dump files upload."""
    try:
        uploaded_files = []
        errors = []
        
        for file in files:
            if not file.filename.endswith(('.sql', '.dump')):
                errors.append(f"{file.filename}: Only .sql and .dump files are allowed.")
                continue
            
            # Save the uploaded file
            file_path = os.path.join(DUMPS_DIR, file.filename)
            with open(file_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            file_info = get_file_info(file_path)
            file_info["formatted_size"] = format_file_size(file_info["size"])
            uploaded_files.append(file_info)
        
        if errors:
            return HTMLResponse(f"""<div id="dump-status" class="alert alert-warning">
                <strong>Warning!</strong><br>
                {'<br>'.join(errors)}
                <br><br>
                Successfully uploaded: {', '.join(f.get('name') for f in uploaded_files) if uploaded_files else 'None'}
            </div>""")
        
        # Return updated file list
        all_dumps = get_directory_contents(DUMPS_DIR)
        for file in all_dumps:
            file["formatted_size"] = format_file_size(file["size"])
        
        files_html = ''.join([f"""
            <tr>
                <td>{file['name']}</td>
                <td>{file['formatted_size']}</td>
                <td>{file['modified']}</td>
            </tr>
        """ for file in all_dumps])
        
        return HTMLResponse(f"""
            <div id="dump-status" class="alert alert-success">
                <strong>Success!</strong> Database dumps uploaded successfully
            </div>
            <div class="table-responsive mt-3">
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Filename</th>
                            <th>Size</th>
                            <th>Modified</th>
                        </tr>
                    </thead>
                    <tbody>
                        {files_html}
                    </tbody>
                </table>
            </div>
        """)
    except Exception as e:
        return HTMLResponse(f"""<div id="dump-status" class="alert alert-danger">
            <strong>Error!</strong> Failed to upload files: {str(e)}
        </div>""")


@router.post("/upload/queries")
async def upload_queries(files: List[UploadFile] = File(...)):
    """Handle queries files upload."""
    try:
        uploaded_files = []
        errors = []
        
        for file in files:
            if not file.filename.endswith('.sql'):
                errors.append(f"{file.filename}: Only .sql files are allowed.")
                continue
            
            # Save the uploaded file
            file_path = os.path.join(QUERIES_DIR, file.filename)
            with open(file_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            file_info = get_file_info(file_path)
            file_info["formatted_size"] = format_file_size(file_info["size"])
            uploaded_files.append(file_info)
        
        if errors:
            return HTMLResponse(f"""<div id="queries-status" class="alert alert-warning">
                <strong>Warning!</strong><br>
                {'<br>'.join(errors)}
                <br><br>
                Successfully uploaded: {', '.join(f.get('name') for f in uploaded_files) if uploaded_files else 'None'}
            </div>""")
        
        # Return updated file list
        all_queries = get_directory_contents(QUERIES_DIR)
        for file in all_queries:
            file["formatted_size"] = format_file_size(file["size"])
        
        files_html = ''.join([f"""
            <tr>
                <td>{file['name']}</td>
                <td>{file['formatted_size']}</td>
                <td>{file['modified']}</td>
            </tr>
        """ for file in all_queries])
        
        return HTMLResponse(f"""
            <div id="queries-status" class="alert alert-success">
                <strong>Success!</strong> Query files uploaded successfully
            </div>
            <div class="table-responsive mt-3">
                <table class="table table-hover">
                    <thead>
                        <tr>
                            <th>Filename</th>
                            <th>Size</th>
                            <th>Modified</th>
                        </tr>
                    </thead>
                    <tbody>
                        {files_html}
                    </tbody>
                </table>
            </div>
        """)
    except Exception as e:
        return HTMLResponse(f"""<div id="queries-status" class="alert alert-danger">
            <strong>Error!</strong> Failed to upload files: {str(e)}
        </div>""") 