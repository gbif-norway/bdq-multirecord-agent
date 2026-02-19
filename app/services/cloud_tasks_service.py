import os
import json
from typing import Dict, Any, Optional
from google.cloud import tasks_v2
from app.utils.helper import log


class CloudTasksService:
    """Service for creating and managing Cloud Tasks for email processing"""
    
    def __init__(self):
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.location = os.getenv("CLOUD_TASKS_LOCATION", "europe-west1")
        self.queue_name = os.getenv("CLOUD_TASKS_QUEUE_NAME", "email-processing-queue")
        self.service_url = os.getenv("CLOUD_RUN_SERVICE_URL")
        self.client: Optional[tasks_v2.CloudTasksClient] = None
        
        self._initialize_client()
    
    def _initialize_client(self) -> None:
        """Initialize Cloud Tasks client if configuration is available"""
        if not self.project_id:
            log("GOOGLE_CLOUD_PROJECT not set, Cloud Tasks disabled", "WARNING")
            return
        
        try:
            self.client = tasks_v2.CloudTasksClient()
        except Exception as e:
            log(f"Failed to initialize Cloud Tasks client: {e}", "ERROR")
            self.client = None
    
    def is_enabled(self) -> bool:
        """Check if Cloud Tasks is properly configured and ready to use"""
        return (
            self.client is not None and
            self.project_id is not None and
            self.service_url is not None and
            self.queue_name is not None
        )
    
    def create_email_processing_task(self, email_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a Cloud Task for email processing.
        
        Args:
            email_data: Email data dictionary to be processed
            
        Returns:
            Task name if successful, None otherwise
        """
        if not self.is_enabled():
            return None
        
        try:
            parent = self.client.queue_path(self.project_id, self.location, self.queue_name)
            
            task = {
                'http_request': {
                    'http_method': tasks_v2.HttpMethod.POST,
                    'url': f'{self.service_url}/tasks/process-email',
                    'headers': {
                        'Content-Type': 'application/json',
                    },
                    'body': json.dumps(email_data).encode(),
                }
            }
            
            response = self.client.create_task(request={'parent': parent, 'task': task})
            log(f"Created Cloud Task: {response.name}")
            return response.name
            
        except Exception as e:
            log(f"Failed to create Cloud Task: {e}", "ERROR")
            return None
