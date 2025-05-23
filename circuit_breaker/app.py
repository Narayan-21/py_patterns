import asyncio
import logging
import random
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Global credential store
cred_store = {"header": None, "created_at": None, "refresh_count": 0}

# Statistics for monitoring
stats = {
    "total_attempts": 0,
    "successful_attempts": 0,
    "failed_attempts": 0,
    "consecutive_failures": 0,
    "last_success": None,
    "last_failure": None
}

# Configuration
class Config:
    AUTH_URL = "http://mock-auth-service/token"
    CLIENT_ID = "demo-client-123"
    SECRET_KEY = "demo-secret-key"
    TOKEN_REFRESH_INTERVAL = 30  # 30 seconds for demo (normally 1200)
    RETRY_INTERVAL = 5  # 5 seconds for demo
    FAILURE_RATE = 0.3  # 30% chance of failure for demo

config = Config()

# Pydantic models
class TokenResponse(BaseModel):
    token: str
    expires_at: datetime
    created_at: datetime

class HealthResponse(BaseModel):
    status: str
    has_token: bool
    token_age_seconds: Optional[int]
    refresh_count: int
    stats: dict

# Demo JWT token generation function that randomly crashes
async def get_jwt_token(auth_url: str, id: str, secret_key: str) -> str:
    """
    Demo JWT token generation function that randomly fails to simulate real-world issues
    """
    stats["total_attempts"] += 1
    
    # Simulate network delay
    await asyncio.sleep(random.uniform(0.1, 0.5))
    
    # Random failure simulation
    if random.random() < config.FAILURE_RATE:
        stats["failed_attempts"] += 1
        stats["consecutive_failures"] += 1
        stats["last_failure"] = datetime.now()
        
        # Different types of failures
        failure_type = random.choice([
            "connection_error",
            "timeout_error", 
            "auth_error",
            "server_error"
        ])
        
        error_messages = {
            "connection_error": "Connection refused to auth service",
            "timeout_error": "Request timeout while connecting to auth service",
            "auth_error": "Authentication failed - invalid credentials",
            "server_error": "Internal server error in auth service"
        }
        
        log.error(f"JWT token generation failed: {error_messages[failure_type]}")
        raise Exception(error_messages[failure_type])
    
    # Successful token generation
    stats["successful_attempts"] += 1
    stats["consecutive_failures"] = 0
    stats["last_success"] = datetime.now()
    
    # Generate a mock JWT token
    token_payload = f"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{uuid.uuid4().hex[:16]}.{uuid.uuid4().hex[:16]}"
    
    log.info(f"JWT token generated successfully from {auth_url}")
    return f"Bearer {token_payload}"

async def get_jwt_token_with_retry(auth_url: str, id: str, secret_key: str, max_retries: int = 3, base_delay: int = 2) -> str:
    """
    Get JWT token with exponential backoff retry logic
    """
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            token = await get_jwt_token(auth_url=auth_url, id=id, secret_key=secret_key)
            if attempt > 0:
                log.info(f"JWT token generation succeeded on attempt {attempt + 1}")
            return token
            
        except Exception as e:
            last_exception = e
            delay = base_delay * (2 ** attempt)  # Exponential backoff
            log.warning(f"JWT token generation failed (attempt {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:  # Don't sleep on the last attempt
                log.info(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
    
    log.error(f"All JWT token generation attempts failed. Last error: {last_exception}")
    raise last_exception

async def token_refresh_manager(auth_url: str, id: str, secret_key: str):
    """
    Continuously manage JWT token with proper error handling and recovery
    """
    initial_setup = True
    
    while True:
        try:
            if cred_store["header"] is None or initial_setup:
                log.info("Attempting to create new JWT token...")
                cred_store["header"] = await get_jwt_token_with_retry(
                    auth_url=auth_url, 
                    id=id, 
                    secret_key=secret_key
                )
                cred_store["created_at"] = datetime.now()
                cred_store["refresh_count"] += 1
                
                log.info(f"✅ New token created (refresh #{cred_store['refresh_count']})")
                initial_setup = False
                
                # Sleep for the full refresh interval after successful creation
                await asyncio.sleep(config.TOKEN_REFRESH_INTERVAL)
            else:
                # Regular token refresh
                log.info("Attempting to refresh JWT token...")
                cred_store["header"] = await get_jwt_token_with_retry(
                    auth_url=auth_url, 
                    id=id, 
                    secret_key=secret_key
                )
                cred_store["created_at"] = datetime.now()
                cred_store["refresh_count"] += 1
                
                log.info(f"🔄 Token refreshed (refresh #{cred_store['refresh_count']})")
                
                # Sleep for the full refresh interval after successful refresh
                await asyncio.sleep(config.TOKEN_REFRESH_INTERVAL)
                
        except Exception as e:
            # If all retries failed, clear the token and try again after a shorter interval
            log.error(f"❌ Failed to generate/refresh JWT token: {e}")
            cred_store["header"] = None
            cred_store["created_at"] = None
            
            log.info(f"⏳ Will retry token generation in {config.RETRY_INTERVAL} seconds...")
            await asyncio.sleep(config.RETRY_INTERVAL)

# FastAPI lifespan manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 Starting JWT Token Management Demo")
    
    # Start the token refresh background task
    token_task = asyncio.create_task(token_refresh_manager(
        auth_url=config.AUTH_URL,
        id=config.CLIENT_ID,
        secret_key=config.SECRET_KEY
    ))
    
    log.info("📡 Token refresh manager started")
    
    try:
        yield
    finally:
        log.info("🛑 Shutting down token refresh manager...")
        token_task.cancel()
        
        try:
            await asyncio.wait_for(token_task, timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            log.info("✅ Token refresh task cancelled successfully")
        except Exception as e:
            log.error(f"❌ Error during token refresh task shutdown: {e}")

# Create FastAPI app
app = FastAPI(
    title="JWT Token Management Demo",
    description="Demo FastAPI app showing JWT token management with retry logic and random failures",
    version="1.0.0",
    lifespan=lifespan
)

# Dependency to get current token
async def get_current_token():
    if cred_store["header"] is None:
        raise HTTPException(status_code=503, detail="No valid JWT token available")
    return cred_store["header"]

# API Endpoints
@app.get("/")
async def root():
    return {
        "message": "JWT Token Management Demo",
        "docs": "/docs",
        "health": "/health",
        "token_status": "/token/status"
    }

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Get application health and token status"""
    token_age = None
    if cred_store["created_at"]:
        token_age = int((datetime.now() - cred_store["created_at"]).total_seconds())
    
    return HealthResponse(
        status="healthy" if cred_store["header"] else "degraded",
        has_token=cred_store["header"] is not None,
        token_age_seconds=token_age,
        refresh_count=cred_store["refresh_count"],
        stats=stats
    )

@app.get("/token/status")
async def token_status():
    """Get detailed token status"""
    return {
        "has_token": cred_store["header"] is not None,
        "token_preview": cred_store["header"][:30] + "..." if cred_store["header"] else None,
        "created_at": cred_store["created_at"],
        "refresh_count": cred_store["refresh_count"],
        "token_age_seconds": int((datetime.now() - cred_store["created_at"]).total_seconds()) if cred_store["created_at"] else None
    }

@app.get("/token/full")
async def get_full_token(token: str = Depends(get_current_token)):
    """Get the full current token (requires valid token)"""
    return {
        "token": token,
        "created_at": cred_store["created_at"],
        "refresh_count": cred_store["refresh_count"]
    }

@app.get("/protected")
async def protected_endpoint(token: str = Depends(get_current_token)):
    """A protected endpoint that requires a valid token"""
    return {
        "message": "This is a protected endpoint",
        "timestamp": datetime.now(),
        "token_used": token[:30] + "...",
        "access_granted": True
    }

@app.get("/stats")
async def get_statistics():
    """Get token generation statistics"""
    success_rate = 0
    if stats["total_attempts"] > 0:
        success_rate = (stats["successful_attempts"] / stats["total_attempts"]) * 100
    
    return {
        **stats,
        "success_rate_percent": round(success_rate, 2),
        "current_failure_rate_percent": config.FAILURE_RATE * 100
    }

@app.post("/config/failure-rate")
async def update_failure_rate(failure_rate: float):
    """Update the failure rate for testing (0.0 to 1.0)"""
    if not 0 <= failure_rate <= 1:
        raise HTTPException(status_code=400, detail="Failure rate must be between 0.0 and 1.0")
    
    config.FAILURE_RATE = failure_rate
    return {
        "message": f"Failure rate updated to {failure_rate * 100}%",
        "new_failure_rate": failure_rate
    }

@app.post("/token/force-refresh")
async def force_token_refresh():
    """Force a token refresh by clearing the current token"""
    cred_store["header"] = None
    cred_store["created_at"] = None
    return {"message": "Token cleared, refresh will be attempted shortly"}

if __name__ == "__main__":
    print("🎯 JWT Token Management Demo")
    print("📋 Available endpoints:")
    print("   • GET  /           - Root endpoint")
    print("   • GET  /health     - Health check with token status")
    print("   • GET  /token/status - Token status details")
    print("   • GET  /protected  - Protected endpoint (requires token)")
    print("   • GET  /stats      - Token generation statistics")
    print("   • POST /config/failure-rate - Update failure rate")
    print("   • POST /token/force-refresh - Force token refresh")
    print()
    print("🔧 Configuration:")
    print(f"   • Token refresh interval: {config.TOKEN_REFRESH_INTERVAL}s")
    print(f"   • Retry interval: {config.RETRY_INTERVAL}s") 
    print(f"   • Failure rate: {config.FAILURE_RATE * 100}%")
    print()
    print("🚀 Starting server...")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info"
    )