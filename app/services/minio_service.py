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
    
    def upload_original_file(self, csv_data: str, original_filename: str) -> Optional[str]:
        """Upload original CSV file after pandas preprocessing"""
        if not self.client:
            log("MinIO client not available - skipping original file upload", "WARNING")
            return None
            
        try:
            # Process with pandas to ensure str dtype and utf8 encoding
            import pandas as pd
            
            # Parse CSV with str dtype
            df = pd.read_csv(io.StringIO(csv_data), dtype=str)
            
            # Ensure dwc: prefixed columns
            df = self._ensure_dwc_prefixed_columns(df)
            
            # Convert back to CSV with utf8 encoding
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            processed_csv = csv_buffer.getvalue()
            
            # Generate filename
            filename = self._generate_filename("original", original_filename)
            object_path = f"{self.base_path}/{filename}"
            
            # Upload to MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_path,
                data=io.BytesIO(processed_csv.encode('utf-8')),
                length=len(processed_csv.encode('utf-8')),
                content_type='text/csv'
            )
            
            log(f"Uploaded original file to: {object_path}")
            return object_path
            
        except Exception as e:
            log(f"Failed to upload original file: {e}", "ERROR")
            return None
    
    def upload_results_file(self, csv_data: str, original_filename: str, file_type: str) -> Optional[str]:
        """Upload results CSV (raw_results or amended_dataset)"""
        if not self.client:
            log("MinIO client not available - skipping results file upload", "WARNING")
            return None
            
        try:
            # Generate filename based on type
            if file_type == "raw_results":
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
                data=io.BytesIO(csv_data.encode('utf-8')),
                length=len(csv_data.encode('utf-8')),
                content_type='text/csv'
            )
            
            log(f"Uploaded {file_type} file to: {object_path}")
            return object_path
            
        except Exception as e:
            log(f"Failed to upload {file_type} file: {e}", "ERROR")
            return None
    
    def _ensure_dwc_prefixed_columns(self, df):
        """Rename columns to have 'dwc:' prefix if they don't already have it"""
        try:
            renamed = 0
            new_columns = []
            for col in df.columns:
                if not col.startswith('dwc:'):
                    new_columns.append(f'dwc:{col}')
                    renamed += 1
                else:
                    new_columns.append(col)
            
            if renamed:
                df.columns = new_columns
                log(f"Renamed {renamed} columns to have 'dwc:' prefix")
            return df
        except Exception as e:
            log(f"Error ensuring dwc-prefixed columns: {e}", "WARNING")
            return df
