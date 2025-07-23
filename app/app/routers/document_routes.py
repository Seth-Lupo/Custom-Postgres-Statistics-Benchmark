import json
from datetime import datetime
from fastapi import APIRouter, Request, Depends, HTTPException, Form, File, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session
from typing import Optional, List
from ..database import get_db
from ..models import Experiment, Document
from ..logging_config import web_logger

templates = Jinja2Templates(directory="app/templates")
router = APIRouter()


@router.get("/experiments/{experiment_id}/documents")
def get_experiment_documents(experiment_id: int, session: Session = Depends(get_db)):
    """Get all documents for an experiment."""
    try:
        # Verify experiment exists
        experiment = session.get(Experiment, experiment_id)
        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")
        
        # Get all documents for this experiment
        documents = session.query(Document).filter(Document.experiment_id == experiment_id).order_by(Document.created_at.desc()).all()
        
        # Convert to serializable format
        documents_data = []
        for doc in documents:
            documents_data.append({
                "id": doc.id,
                "name": doc.name,
                "filename": doc.filename,
                "content_type": doc.content_type,
                "document_type": doc.document_type,
                "size_bytes": doc.size_bytes,
                "source": doc.source,
                "created_at": doc.created_at.isoformat(),
                "preview": doc.content[:200] + "..." if len(doc.content) > 200 else doc.content
            })
        
        return JSONResponse({
            "documents": documents_data,
            "count": len(documents_data)
        })
    
    except Exception as e:
        web_logger.error(f"Error fetching documents for experiment {experiment_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/documents/{document_id}")
def get_document(document_id: int, session: Session = Depends(get_db)):
    """Get a specific document by ID."""
    try:
        document = session.get(Document, document_id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")
        
        return JSONResponse({
            "id": document.id,
            "name": document.name,
            "filename": document.filename,
            "content_type": document.content_type,
            "document_type": document.document_type,
            "content": document.content,
            "size_bytes": document.size_bytes,
            "source": document.source,
            "extra_metadata": json.loads(document.extra_metadata) if document.extra_metadata else None,
            "created_at": document.created_at.isoformat()
        })
    
    except Exception as e:
        web_logger.error(f"Error fetching document {document_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/documents/{document_id}/content")
def get_document_content(document_id: int, session: Session = Depends(get_db)):
    """Get document content with appropriate content type."""
    try:
        document = session.get(Document, document_id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")
        
        # Return content with appropriate media type
        if document.content_type == "text/csv":
            return PlainTextResponse(
                content=document.content,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={document.filename}"}
            )
        elif document.content_type == "application/json":
            return JSONResponse(
                content=json.loads(document.content),
                headers={"Content-Disposition": f"attachment; filename={document.filename}"}
            )
        else:
            return PlainTextResponse(
                content=document.content,
                media_type=document.content_type,
                headers={"Content-Disposition": f"attachment; filename={document.filename}"}
            )
    
    except Exception as e:
        web_logger.error(f"Error fetching document content {document_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/experiments/{experiment_id}/documents")
def create_document(
    experiment_id: int,
    name: str = Form(...),
    document_type: str = Form(...),
    content_type: str = Form("text/plain"),
    source: Optional[str] = Form(None),
    content: str = Form(...),
    session: Session = Depends(get_db)
):
    """Create a new document for an experiment."""
    try:
        # Verify experiment exists
        experiment = session.get(Experiment, experiment_id)
        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")
        
        # Generate filename if not provided
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{name}_{timestamp}.txt"
        if content_type == "text/csv":
            filename = f"{name}_{timestamp}.csv"
        elif content_type == "application/json":
            filename = f"{name}_{timestamp}.json"
        
        # Create document
        document = Document(
            experiment_id=experiment_id,
            name=name,
            filename=filename,
            content_type=content_type,
            document_type=document_type,
            content=content,
            size_bytes=len(content.encode('utf-8')),
            source=source or "User Upload"
        )
        
        session.add(document)
        session.commit()
        session.refresh(document)
        
        web_logger.info(f"Created document {document.id} for experiment {experiment_id}")
        
        return JSONResponse({
            "id": document.id,
            "message": "Document created successfully"
        })
    
    except Exception as e:
        web_logger.error(f"Error creating document for experiment {experiment_id}: {str(e)}")
        session.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/experiments/{experiment_id}/documents/upload")
def upload_document(
    experiment_id: int,
    file: UploadFile = File(...),
    document_type: str = Form("user_upload"),
    session: Session = Depends(get_db)
):
    """Upload a file as a document for an experiment."""
    try:
        # Verify experiment exists
        experiment = session.get(Experiment, experiment_id)
        if not experiment:
            raise HTTPException(status_code=404, detail="Experiment not found")
        
        # Read file content
        content = file.file.read().decode('utf-8')
        
        # Determine content type
        content_type = file.content_type
        if file.filename.endswith('.csv'):
            content_type = "text/csv"
        elif file.filename.endswith('.json'):
            content_type = "application/json"
        elif file.filename.endswith('.txt'):
            content_type = "text/plain"
        
        # Create document
        document = Document(
            experiment_id=experiment_id,
            name=file.filename,
            filename=file.filename,
            content_type=content_type,
            document_type=document_type,
            content=content,
            size_bytes=len(content.encode('utf-8')),
            source="File Upload"
        )
        
        session.add(document)
        session.commit()
        session.refresh(document)
        
        web_logger.info(f"Uploaded document {document.id} for experiment {experiment_id}")
        
        return JSONResponse({
            "id": document.id,
            "message": "Document uploaded successfully"
        })
    
    except Exception as e:
        web_logger.error(f"Error uploading document for experiment {experiment_id}: {str(e)}")
        session.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/documents/{document_id}")
def delete_document(document_id: int, session: Session = Depends(get_db)):
    """Delete a document."""
    try:
        document = session.get(Document, document_id)
        if not document:
            raise HTTPException(status_code=404, detail="Document not found")
        
        session.delete(document)
        session.commit()
        
        web_logger.info(f"Deleted document {document_id}")
        
        return JSONResponse({"message": "Document deleted successfully"})
    
    except Exception as e:
        web_logger.error(f"Error deleting document {document_id}: {str(e)}")
        session.rollback()
        raise HTTPException(status_code=500, detail="Internal server error")


def save_api_response_as_document(
    experiment_id: int, 
    response_content: str, 
    response_type: str = "api_response",
    source_description: str = "AI API Response",
    session: Session = None
) -> Optional[int]:
    """
    Utility function to save API responses as documents.
    This will be called from the SchneiderAI code instead of logging responses.
    
    Args:
        experiment_id: ID of the experiment
        response_content: The API response content to save
        response_type: Type of response (e.g., 'api_response', 'ai_prompt', 'ai_response')
        source_description: Description of the source
        session: Database session (if not provided, will create one)
        
    Returns:
        Document ID if successful, None otherwise
    """
    should_close_session = False
    
    try:
        if session is None:
            from ..database import SessionLocal
            session = SessionLocal()
            should_close_session = True
        
        # Generate filename based on response type and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
        
        # Determine content type based on content
        content_type = "text/plain"
        filename = f"{response_type}_{timestamp}.txt"
        
        # Try to detect if it's JSON
        try:
            json.loads(response_content)
            content_type = "application/json"
            filename = f"{response_type}_{timestamp}.json"
        except:
            # Try to detect if it's CSV (contains semicolons or commas in structured format)
            if ';' in response_content or (',' in response_content and '\n' in response_content):
                content_type = "text/csv"
                filename = f"{response_type}_{timestamp}.csv"
        
        # Create document
        document = Document(
            experiment_id=experiment_id,
            name=f"{response_type.replace('_', ' ').title()} - {timestamp}",
            filename=filename,
            content_type=content_type,
            document_type=response_type,
            content=response_content,
            size_bytes=len(response_content.encode('utf-8')),
            source=source_description
        )
        
        session.add(document)
        session.commit()
        session.refresh(document)
        
        web_logger.info(f"Saved {source_description} as document {document.id} for experiment {experiment_id}")
        
        return document.id
    
    except Exception as e:
        web_logger.error(f"Error saving API response as document: {str(e)}")
        if session:
            session.rollback()
        return None
    
    finally:
        if should_close_session and session:
            session.close() 