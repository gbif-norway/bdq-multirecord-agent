import os
import io
from datetime import datetime
from typing import Optional
from minio import Minio
from minio.error import S3Error
from app.utils.helper import log

class MinIOService:
    """Service for uploading files to MinIO S3 bucket"""
    
    def __init__(self):
        self.bucket_url = "https://storage.gbif-no.sigma2.no"
        self.username = "devops"
        self.password = os.getenv("MINIO_SECRET")
        self.bucket_name = "misc"
        self.base_path = "bdqreport/results"
        self.client = None
        
        if not self.password:
            log("MINIO_SECRET environment variable not set - MinIO uploads will be disabled", "WARNING")
            return
        
        try:
            # Initialize MinIO client
            self.client = Minio(
                "storage.gbif-no.sigma2.no",
                access_key=self.username,
                secret_key=self.password,
                secure=True
            )
            log("MinIO client initialized successfully")
        except Exception as e:
            log(f"Failed to initialize MinIO client: {e}", "ERROR")
            self.client = None
    
    def _generate_filename(self, prefix: str, original_name: str, extension: str = ".csv") -> str:
        """Generate unique filename with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Clean original name for filename safety
        clean_name = "".join(c for c in original_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        clean_name = clean_name.replace(' ', '_')
        return f"{prefix}_{clean_name}_{timestamp}{extension}"
    
    def upload_dataframe(self, df, original_filename: str, file_type: str) -> Optional[str]:
        """Upload a pandas DataFrame as CSV to MinIO"""
        if not self.client:
            log("MinIO client not available - skipping DataFrame upload", "WARNING")
            return None
            
        try:
            # Convert DataFrame to CSV with utf8 encoding
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()
            
            # Generate filename based on type
            if file_type == "original":
                filename = self._generate_filename("original", original_filename)
            elif file_type == "raw_results":
                filename = self._generate_filename("results", original_filename)
            elif file_type == "amended":
                filename = self._generate_filename("amended", original_filename)
            else:
                log(f"Unknown file type: {file_type}", "ERROR")
                return None
            
            object_path = f"{self.base_path}/{filename}"
            
            # Upload to MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_path,
                data=io.BytesIO(csv_content.encode('utf-8')),
                length=len(csv_content.encode('utf-8')),
                content_type='text/csv'
            )
            
            log(f"Uploaded {file_type} file to: {object_path}")
            return object_path
            
        except Exception as e:
            log(f"Failed to upload {file_type} file: {e}", "ERROR")
            return None
    
