import json
import httpx
import logging

# Get logger for this module
logger = logging.getLogger(__name__)

end_point = "http://localhost:3000"
api_key = "1234567890"

# Configure HTTP client settings
CLIENT_TIMEOUT = 120.0  # 2 minutes timeout for AWS API
CLIENT_RETRIES = 3

def retrieve(
    query: str,
    session_id: str,
    rag_threshold: float,
    rag_k: int
    ):

    headers = {
        'x-api-key': api_key,
        'request_type': 'retrieve'
    }

    request = {
        'query': query,
        'session_id': session_id,
        'rag_threshold': rag_threshold,
        'rag_k': rag_k
    }

    logger.info(f"Making retrieve request to {end_point}")
    logger.debug(f"Request: {request}")

    msg = None

    try:
        with httpx.Client(timeout=CLIENT_TIMEOUT) as client:
            response = client.post(end_point, headers=headers, json=request)

        logger.info(f"retrieve response: status={response.status_code}")

        if response.status_code == 200:
            msg = response.json()
            logger.debug(f"Successful retrieve response: {msg}")
        else:
            try:
                error_body = response.text
                logger.error(f"retrieve error response body: {error_body}")
                msg = f"Error: Received response code {response.status_code}. Response: {error_body}"
            except:
                msg = f"Error: Received response code {response.status_code}"
    except httpx.TimeoutException as e:
        msg = f"Request timeout: {e}"
        logger.error(f"retrieve timeout: {e}")
    except httpx.RequestError as e:
        msg = f"An error occurred: {e}"
        logger.error(f"retrieve request error: {e}")
    except Exception as e:
        msg = f"Unexpected error: {e}"
        logger.error(f"retrieve unexpected error: {e}")
    return msg  

def model_info():
    headers = {
        'x-api-key': api_key,
        'request_type': 'model_info'
    }

    logger.info(f"Making model_info request to {end_point}")
    logger.debug(f"Headers: {headers}")

    msg = None

    try:
        # Add timeout configuration
        with httpx.Client(timeout=CLIENT_TIMEOUT) as client:
            response = client.post(end_point, headers=headers, json={})

        logger.info(f"model_info response: status={response.status_code}")
        
        if response.status_code == 200:
            msg = response.json()
            logger.debug(f"Successful model_info response: {msg}")
        else:
            # Get response body for better error info
            try:
                error_body = response.text
                logger.error(f"model_info error response body: {error_body}")
                msg = f"Error: Received response code {response.status_code}. Response: {error_body}"
            except:
                msg = f"Error: Received response code {response.status_code}"
                
    except httpx.TimeoutException as e:
        msg = f"Request timeout: {e}"
        logger.error(f"model_info timeout: {e}")
    except httpx.RequestError as e:
        msg = f"An error occurred: {e}"
        logger.error(f"model_info request error: {e}")
    except Exception as e:
        msg = f"Unexpected error: {e}"
        logger.error(f"model_info unexpected error: {e}")
        
    return msg  


def generate(
	model: str,
	system: str,
	query: str,
	temperature: float | None = None,
	lastk: int | None = None,
	session_id: str | None = None,
    rag_threshold: float | None = 0.5,
    rag_usage: bool | None = False,
    rag_k: int | None = 0
	):

    headers = {
        'x-api-key': api_key,
        'request_type': 'call'
    }

    request = {
        'model': model,
        'system': system,
        'query': query,
        'temperature': temperature,
        'lastk': lastk,
        'session_id': session_id,
        'rag_threshold': rag_threshold,
        'rag_usage': rag_usage,
        'rag_k': rag_k
    }

    # Log request details (without full query for brevity)
    logger.info(f"Making generate request to {end_point}")
    logger.debug(f"Headers: {headers}")
    logger.info(f"Request params: model={model}, temperature={temperature}, "
               f"query_length={len(query)}, session_id={session_id}")
    logger.debug(f"Full system prompt: {system}")
    logger.debug(f"Query (first 200 chars): {query[:200]}...")

    msg = None

    try:
        # Add timeout configuration for potentially long AI requests
        with httpx.Client(timeout=CLIENT_TIMEOUT) as client:
            logger.info("Sending POST request to API...")
            response = client.post(end_point, headers=headers, json=request)

        logger.info(f"generate response: status={response.status_code}")
        
        if response.status_code == 200:
            try:
                res = response.json()
                logger.debug(f"Raw API response keys: {list(res.keys()) if isinstance(res, dict) else type(res)}")
                
                if isinstance(res, dict) and 'result' in res:
                    msg = {'response': res['result'], 'rag_context': res.get('rag_context', None)}
                    logger.info(f"Successfully parsed generate response, result length: {len(res['result']) if res['result'] else 0}")
                else:
                    logger.error(f"Unexpected response format: missing 'result' key. Response: {res}")
                    msg = f"Error: Unexpected response format: {res}"
                    
            except json.JSONDecodeError as je:
                logger.error(f"Failed to parse JSON response: {je}")
                response_text = response.text[:1000] + "..." if len(response.text) > 1000 else response.text
                logger.error(f"Response text: {response_text}")
                msg = f"Error: Invalid JSON response: {je}"
        else:
            # Get response body for better error info
            try:
                error_body = response.text
                logger.error(f"generate error response body: {error_body}")
                msg = f"Error: Received response code {response.status_code}. Response: {error_body}"
            except:
                msg = f"Error: Received response code {response.status_code}"
                
    except httpx.TimeoutException as e:
        msg = f"Request timeout after {CLIENT_TIMEOUT}s: {e}"
        logger.error(f"generate timeout: {e}")
    except httpx.RequestError as e:
        msg = f"An error occurred: {e}"
        logger.error(f"generate request error: {e}")
    except Exception as e:
        msg = f"Unexpected error: {e}"
        logger.error(f"generate unexpected error: {e}")
        
    return msg	



def upload(multipart_form_data):

    headers = {
        'x-api-key': api_key,
        'request_type': 'add'
    }

    msg = None
    try:
        with httpx.Client() as client:
            response = client.post(end_point, headers=headers, files=multipart_form_data)
        
        if response.status_code == 200:
            msg = "Successfully uploaded. It may take a short while for the document to be added to your context"
        else:
            msg = f"Error: Received response code {response.status_code}"
    except httpx.RequestError as e:
        msg = f"An error occurred: {e}"
    
    return msg


def pdf_upload(
    path: str,    
    strategy: str | None = None,
    description: str | None = None,
    session_id: str | None = None
    ):
    
    params = {
        'description': description,
        'session_id': session_id,
        'strategy': strategy
    }

    multipart_form_data = {
        'params': (None, json.dumps(params), 'application/json'),
        'file': (None, open(path, 'rb'), "application/pdf")
    }

    response = upload(multipart_form_data)
    return response

def text_upload(
    text: str,    
    strategy: str | None = None,
    description: str | None = None,
    session_id: str | None = None
    ):
    
    params = {
        'description': description,
        'session_id': session_id,
        'strategy': strategy
    }


    multipart_form_data = {
        'params': (None, json.dumps(params), 'application/json'),
        'text': (None, text, "application/text")
    }


    response = upload(multipart_form_data)
    return response 