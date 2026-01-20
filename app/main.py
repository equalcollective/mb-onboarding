"""FastAPI application entry point."""

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from .config import get_settings
from .api.routes import router

# Path to built frontend
FRONTEND_DIR = Path(__file__).parent.parent / "frontend" / "dist"

# Create FastAPI app
app = FastAPI(
    title="MB Onboarding Data Pipeline",
    description="Data pipeline for Amazon seller analytics from Metabase",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(router)

# Serve static frontend files if the build exists
if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

    @app.get("/")
    async def serve_frontend():
        """Serve the frontend."""
        return FileResponse(FRONTEND_DIR / "index.html")

    @app.get("/{full_path:path}")
    async def serve_spa(request: Request, full_path: str):
        """Serve SPA - return index.html for all non-API routes."""
        # Don't catch API routes
        if full_path.startswith("api/") or full_path.startswith("docs") or full_path.startswith("openapi"):
            return None
        # Serve static files if they exist
        file_path = FRONTEND_DIR / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        # Otherwise return index.html for SPA routing
        return FileResponse(FRONTEND_DIR / "index.html")
else:
    @app.get("/")
    async def root():
        """Root endpoint when frontend not built."""
        return {
            "name": "MB Onboarding Data Pipeline",
            "version": "0.1.0",
            "docs": "/docs",
            "health": "/api/health",
            "note": "Frontend not built. Run 'npm run build' in frontend directory."
        }


def main():
    """Run the application."""
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()
