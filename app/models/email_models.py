from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class EmailAttachment(BaseModel):
    """Model for email attachment data"""
    filename: str
    mime_type: str
    content_base64: str
    size: Optional[int] = None

class EmailPayload(BaseModel):
    """Model for incoming email data from Apps Script"""
    message_id: str
    thread_id: str
    from_email: str
    to_email: str
    subject: str
    body_text: Optional[str] = None
    body_html: Optional[str] = None
    attachments: List[EmailAttachment] = Field(default_factory=list)
    headers: Dict[str, str] = Field(default_factory=dict)

class EmailReply(BaseModel):
    """Model for email reply data"""
    thread_id: str
    body_text: str
    body_html: str
    attachments: List[EmailAttachment] = Field(default_factory=list)

class BDQTest(BaseModel):
    """Model for BDQ test definition"""
    id: str
    guid: str
    type: str  # "Validation" or "Amendment"
    className: str
    methodName: str
    actedUpon: List[str]
    consulted: List[str] = Field(default_factory=list)
    parameters: List[Any] = Field(default_factory=list)

class BDQTestResult(BaseModel):
    """Model for BDQ test execution result"""
    test_id: str
    status: str
    result: Optional[str] = None
    comment: Optional[str] = None
    amendment: Optional[Dict[str, Any]] = None

class TestExecutionResult(BaseModel):
    """Model for complete test execution result for a row"""
    record_id: str
    test_id: str
    status: str
    result: Optional[str] = None
    comment: Optional[str] = None
    amendment: Optional[Dict[str, Any]] = None
    test_type: str

class ProcessingSummary(BaseModel):
    """Model for processing summary"""
    total_records: int
    total_tests_run: int
    validation_failures: Dict[str, int] = Field(default_factory=dict)
    common_issues: List[str] = Field(default_factory=list)
    amendments_applied: int = 0
    skipped_tests: List[str] = Field(default_factory=list)
