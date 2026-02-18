import os
import json
from typing import Dict, Any, Optional
from google.cloud import tasks_v2
from app.utils.helper import log


class CloudTasksService:
    """Service for creating and managing Cloud Tasks"""
    
    def __init__(self):
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.location = os.getenv("CLOUD_TASKS_LOCATION", "europe-west1")
        self.queue_name = os.getenv("CLOUD_TASKS_QUEUE_NAME", "email-processing-queue")
        self.service_url = os.getenv("CLOUD_RUN_SERVICE_URL")
        
        # Initialize Cloud Tasks client
        self.client = None
        if self.project_id:
            try:
                self.client = tasks_v2.CloudTasksClient()
            except Exception as e:
                log(f"Failed to initialize Cloud Tasks client: {e}", "WARNING")
                self.client = None
    
    def is_enabled(self) -> bool:
        """Check if Cloud Tasks is properly configured"""
        return (
            self.client is not None and
            self.project_id is not None and
            self.service_url is not None
        )
    
    def create_email_processing_task(self, email_data: Dict[str, Any]) -> Optional[str]:
        """
        Create a Cloud Task for email processing.
        
        Returns task name if successful, None otherwise.
        """
        if not self.is_enabled():
            log("Cloud Tasks not enabled or not configured", "WARNING")
            return None
        
        try:
            # Construct the fully qualified queue name
            parent = self.client.queue_path(self.project_id, self.location, self.queue_name)
            
            # Construct the task
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
            
            # Create the task
            response = self.client.create_task(request={'parent': parent, 'task': task})
            
            log(f"Created Cloud Task: {response.name} for email processing")
            return response.name
            
        except Exception as e:
            log(f"Failed to create Cloud Task: {e}", "ERROR")
            return None
