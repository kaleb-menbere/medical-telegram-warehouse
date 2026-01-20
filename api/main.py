from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .endpoints import router
import uvicorn

# Create FastAPI app
app = FastAPI(
    title="Telegram Medical Analytics API",
    description="API for analyzing Ethiopian medical Telegram channels",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(router, prefix="/api", tags=["Analytics"])

@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "message": "Telegram Medical Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "health": "/api/health",
            "top_products": "/api/reports/top-products",
            "channel_activity": "/api/channels/{channel_name}/activity",
            "search_messages": "/api/search/messages",
            "visual_content": "/api/reports/visual-content",
            "channels": "/api/channels",
            "recent_messages": "/api/messages/recent"
        }
    }

if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)