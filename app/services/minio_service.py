import os
import io
from datetime import datetime
from typing import Optional
from minio import Minio
from app.utils.helper import log

class MinIOService:
    """Service for uploading files to MinIO S3 bucket"""
    
    def __init__(self):
        self.username = "devops"
        self.password = os.getenv("MINIO_SECRET")
        self.bucket_name = "misc"
        self.base_path = "bdqreport/results"
        self.client = None
        
        if not self.password:
            log("MINIO_SECRET environment variable not set - MinIO uploads will be disabled", "WARNING")
            return
        
        # Initialize MinIO client
        self.client = Minio(
            "storage.gbif-no.sigma2.no",
            access_key=self.username,
            secret_key=self.password,
            secure=True
        )
        log("MinIO client initialized successfully")
    
    def _generate_filename(self, prefix: str, original_name: str) -> str:
        """Generate unique filename with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Clean original name for filename safety
        clean_name = "".join(c for c in original_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        clean_name = clean_name.replace(' ', '_')
        return f"{prefix}_{clean_name}_{timestamp}.csv"
    
    def _upload_csv_content(self, csv_content: str, filename: str) -> Optional[str]:
        """Upload CSV content to MinIO and return filename"""
        if not self.client:
            log("MinIO client not available - skipping upload", "WARNING")
            return None
        
        object_path = f"{self.base_path}/{filename}"
        
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_path,
            data=io.BytesIO(csv_content.encode('utf-8')),
            length=len(csv_content.encode('utf-8')),
            content_type='text/csv'
        )
        
        log(f"Uploaded file to: {object_path}")
        return filename
    
    def upload_dataframe(self, df, original_filename: str, file_type: str) -> Optional[str]:
        """Upload a pandas DataFrame as CSV to MinIO"""
        # Convert DataFrame to CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()
        
        # Generate filename based on type
        filename = self._generate_filename(file_type, original_filename)
        
        return self._upload_csv_content(csv_content, filename)
    
    def generate_dashboard_url(self, unique_test_results_name: str, amended_name: str) -> str:
        """Generate dashboard URL for viewing breakdown report (unique + amended only)"""
        base_url = "https://storage.gbif-no.sigma2.no/misc/bdqreport/bdq-report.html"
        return f"{base_url}?unique_test_results={unique_test_results_name}&amended_dataset={amended_name}"
    
    def download_csv_from_url(self, s3_url: str) -> Optional[str]:
        """Download CSV content from S3 URL"""
        if not self.client:
            log("MinIO client not available - cannot download file", "WARNING")
            return None
        
        try:
            # Parse the S3 URL to extract bucket and object path
            # Expected format: https://storage.gbif-no.sigma2.no/misc/bdqreport/results/filename.csv
            if not s3_url.startswith("https://storage.gbif-no.sigma2.no/"):
                log(f"Invalid S3 URL format: {s3_url}", "ERROR")
                return None
            
            # Extract object path from URL
            url_parts = s3_url.replace("https://storage.gbif-no.sigma2.no/", "").split("/", 1)
            if len(url_parts) != 2:
                log(f"Could not parse S3 URL: {s3_url}", "ERROR")
                return None
            
            bucket_name, object_path = url_parts
            
            # Download the object
            response = self.client.get_object(bucket_name, object_path)
            csv_content = response.read().decode('utf-8')
            
            log(f"Downloaded CSV from S3: {s3_url} ({len(csv_content)} characters)")
            return csv_content
            
        except Exception as e:
            log(f"Error downloading CSV from S3 URL {s3_url}: {e}", "ERROR")
            return None
    
