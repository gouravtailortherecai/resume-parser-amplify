import os
import json
import io
from typing import Optional
import httpx
import pdfplumber
import docx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import asyncpg
import datetime
import boto3

GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GROQ_API_URL = os.environ.get("GROQ_API_URL", "https://api.groq.com/openai/v1/chat/completions")
DATABASE_URL = os.environ.get("DATABASE_URL")

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

if not GROQ_API_KEY or not DATABASE_URL or not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise RuntimeError("Set GROQ_API_KEY, DATABASE_URL, AWS_ACCESS_KEY_ID, and AWS_SECRET_ACCESS_KEY env vars")

# S3 client
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

app = FastAPI(title="Resume Parser with S3 + user_id")

# Database pool
db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    # ✅ Create table with user_id column
    async with db_pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS resumes (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                file_key TEXT NOT NULL,
                bucket_name TEXT NOT NULL,
                name TEXT,
                email TEXT,
                phone TEXT,
                skills TEXT[],
                experience TEXT[],
                education TEXT[],
                parsed_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        ''')

@app.on_event("startup")
async def on_startup():
    await init_db()

# ✅ Request model now has userId
class ParseRequest(BaseModel):
    userId: str
    bucketName: str
    fileKey: str
    mimeType: Optional[str] = None

def extract_text_from_pdf_bytes(data: bytes) -> str:
    text = ""
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for p in pdf.pages:
            text += p.extract_text() or ""
    return text.strip()

def extract_text_from_docx_bytes(data: bytes) -> str:
    doc = docx.Document(io.BytesIO(data))
    return "\n".join([p.text for p in doc.paragraphs if p.text]).strip()

def extract_text_from_bytes(data: bytes, mime_type: Optional[str]) -> str:
    if mime_type == "application/pdf":
        return extract_text_from_pdf_bytes(data)
    if mime_type in (
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/msword",
    ):
        return extract_text_from_docx_bytes(data)
    try:
        return data.decode(errors="ignore")
    except Exception:
        return ""

async def call_groq_api(cv_text: str, timeout: float = 30.0):
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a resume parser. "
                    "Extract candidate details and return JSON with this schema:\n"
                    "{\n"
                    '  "name": string,\n'
                    '  "email": string,\n'
                    '  "phone": string,\n'
                    '  "skills": [string],\n'
                    '  "experience": [string],\n'
                    '  "education": [string]\n'
                    "}"
                ),
            },
            {"role": "user", "content": cv_text},
        ],
        "temperature": 0.0,
        "max_tokens": 1024,
        "response_format": {"type": "json_object"},
    }

    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(GROQ_API_URL, headers=headers, json=payload)
        if r.status_code != 200:
            raise HTTPException(status_code=r.status_code, detail=r.text)
        data = r.json()
        content = data.get("choices", [{}])[0].get("message", {}).get("content")
        return json.loads(content)

@app.post("/parse")
async def parse_resume(req: ParseRequest, request: Request):
    user_id = req.userId
    bucket_name = req.bucketName
    file_key = req.fileKey
    mime_type = req.mimeType

    if not user_id or not bucket_name or not file_key:
        raise HTTPException(status_code=400, detail="userId, bucketName, and fileKey are required")

    api_timeout = float(request.headers.get("X-API-Timeout", 30.0))

    try:
        # ✅ Download file from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_bytes = obj["Body"].read()

        cv_text = extract_text_from_bytes(file_bytes, mime_type)
        if not cv_text.strip():
            raise HTTPException(status_code=400, detail="Could not extract text from file")

        parsed = await call_groq_api(cv_text, timeout=api_timeout)

        # ✅ Insert with user_id
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    '''INSERT INTO resumes (user_id, file_key, bucket_name, name, email, phone, skills, experience, education, parsed_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)''',
                    user_id,
                    file_key,
                    bucket_name,
                    parsed.get("name"),
                    parsed.get("email"),
                    parsed.get("phone"),
                    parsed.get("skills", []),
                    parsed.get("experience", []),
                    parsed.get("education", []),
                    datetime.datetime.utcnow()
                )
        except Exception:
            pass

        return {"userId": user_id, "bucketName": bucket_name, "fileKey": file_key, "parsedData": parsed}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
