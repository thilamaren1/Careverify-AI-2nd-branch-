"""
CareVerify - Supabase Client
Singleton pattern for Supabase connections (anon + service role)
"""

from __future__ import annotations
import os
from functools import lru_cache
from typing import Optional

import sys

try:
    from supabase import create_client, Client
except ImportError:
    class Client:
        pass
    def create_client(url, key):
        return MockSupabaseClient()

import jwt


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    """Anon-key client for operations respecting RLS."""
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_ANON_KEY"]
    return create_client(url, key)


@lru_cache(maxsize=1)
def get_supabase_admin() -> Client:
    """Service-role client that bypasses RLS — use carefully."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    
    if not url or not key:
        print("[WARNING] Supabase credentials missing. Using MockSupabaseClient.")
        return MockSupabaseClient()
        
    return create_client(url, key)

class MockSupabaseClient:
    """Mock client for local development without Supabase keys."""
    class Table:
        def __init__(self, name): self.name = name
        def select(self, *args, **kwargs): return self
        def insert(self, *args, **kwargs): return self
        def update(self, *args, **kwargs): return self
        def eq(self, *args, **kwargs): return self
        def neq(self, *args, **kwargs): return self
        def gte(self, *args, **kwargs): return self
        def lt(self, *args, **kwargs): return self
        def in_(self, *args, **kwargs): return self
        def contains(self, *args, **kwargs): return self
        def order(self, *args, **kwargs): return self
        def limit(self, *args, **kwargs): return self
        def single(self, *args, **kwargs): return self
        def range(self, *args, **kwargs): return self
        def execute(self):
            # Return empty but valid data structures
            return MockResponse()

    class MockResponse:
        def __init__(self, data=None):
            self.data = data or []
            self.count = len(self.data)

    def table(self, name): return self.Table(name)
    def storage(self): return self
    def from_(self, bucket): return self
    def upload(self, *args, **kwargs): return True
    def download(self, *args, **kwargs): return b""
    def create_signed_url(self, *args, **kwargs): return {"signedURL": "mock-url"}


def verify_supabase_jwt(token: str) -> Optional[dict]:
    """
    Verify a Supabase JWT and return the decoded payload.
    Returns None if token is invalid or expired.
    """
    try:
        secret = os.environ["SUPABASE_JWT_SECRET"]
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def get_user_from_token(token: str) -> Optional[dict]:
    """
    Decode JWT and fetch full user profile including org and role.
    """
    payload = verify_supabase_jwt(token)
    if not payload:
        return None

    user_id = payload.get("sub")
    if not user_id:
        return None

    supabase = get_supabase_admin()
    result = (
        supabase.table("users")
        .select("*, organizations(id, name, type, trust_score, is_active)")
        .eq("id", user_id)
        .single()
        .execute()
    )

    if not result.data:
        return None

    user = result.data
    user["jwt_payload"] = payload
    return user


def upload_document(
    file_bytes: bytes,
    file_path: str,
    content_type: str = "application/pdf",
) -> str:
    """
    Upload a document to Supabase Storage.
    Returns the storage path.
    """
    supabase = get_supabase_admin()
    bucket = os.environ.get("SUPABASE_STORAGE_BUCKET", "medical-documents")

    supabase.storage.from_(bucket).upload(
        file_path,
        file_bytes,
        {"content-type": content_type, "upsert": "false"},
    )
    return file_path


def create_signed_url(file_path: str, expires_in: int = 3600) -> str:
    """
    Generate a signed URL for temporary secure document access.
    Default: 1 hour expiry.
    """
    supabase = get_supabase_admin()
    bucket = os.environ.get("SUPABASE_STORAGE_BUCKET", "medical-documents")

    result = supabase.storage.from_(bucket).create_signed_url(file_path, expires_in)
    return result["signedURL"]