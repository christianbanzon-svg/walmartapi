from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import os
import sys
import subprocess
import json
import time
from datetime import datetime
import asyncio
from pathlib import Path

# Add the walmart directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from run_walmart import main as run_scraper
from run_walmart_id_crawler import run_id_crawler
from run_walmart_id_crawler_fast_simple import run_fast_id_crawler

app = FastAPI(
    title="Walmart Scraper API",
    description="API for running Walmart product and seller scraping",
    version="1.0.0"
)

# Store running tasks
running_tasks = {}

class ScrapeRequest(BaseModel):
    keywords: str = "nike"
    max_per_keyword: int = 10
    sleep: int = 1
    export: str = "csv"
    debug: bool = False
    zipcode: Optional[str] = None
    category_id: Optional[str] = None
    retry_seller_passes: int = 3
    retry_seller_delay: int = 5

class IDCrawlRequest(BaseModel):
    item_ids: str = "5245210374"
    export: str = "csv"
    debug: bool = False
    sleep: float = 0.5

class ScrapeResponse(BaseModel):
    task_id: str
    status: str
    message: str
    timestamp: str

@app.get("/")
async def root():
    return {"message": "Walmart Scraper API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/scrape", response_model=ScrapeResponse)
async def start_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start a new scraping task"""
    task_id = f"task_{int(time.time())}"
    
    # Store task info
    running_tasks[task_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "request": request.dict(),
        "output_files": []
    }
    
    # Start background task
    background_tasks.add_task(run_scrape_task, task_id, request)
    
    return ScrapeResponse(
        task_id=task_id,
        status="started",
        message="Scraping task started successfully",
        timestamp=datetime.now().isoformat()
    )

async def run_scrape_task(task_id: str, request: ScrapeRequest):
    """Background task to run the scraper"""
    try:
        # Prepare arguments for the scraper
        args = [
            "--keywords", request.keywords,
            "--max-per-keyword", str(request.max_per_keyword),
            "--sleep", str(request.sleep),
            "--export", request.export,
        ]
        
        if request.debug:
            args.append("--debug")
        if request.zipcode:
            args.extend(["--zipcode", request.zipcode])
        if request.category_id:
            args.extend(["--category-id", request.category_id])
        
        args.extend(["--retry-seller-passes", str(request.retry_seller_passes)])
        args.extend(["--retry-seller-delay", str(request.retry_seller_delay)])
        
        # Run the scraper
        result = run_scraper(args)
        
        # Update task status
        running_tasks[task_id]["status"] = "completed"
        running_tasks[task_id]["end_time"] = datetime.now().isoformat()
        running_tasks[task_id]["result"] = result
        
        # Find output files
        output_dir = Path("output")
        if output_dir.exists():
            output_files = list(output_dir.glob("*.csv")) + list(output_dir.glob("*.json"))
            running_tasks[task_id]["output_files"] = [str(f) for f in output_files]
            
    except Exception as e:
        running_tasks[task_id]["status"] = "failed"
        running_tasks[task_id]["error"] = str(e)
        running_tasks[task_id]["end_time"] = datetime.now().isoformat()

@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    """Get the status of a specific task"""
    if task_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return running_tasks[task_id]

@app.get("/tasks")
async def list_tasks():
    """List all tasks"""
    return {
        "tasks": running_tasks,
        "count": len(running_tasks)
    }

@app.get("/download/{task_id}/{filename}")
async def download_file(task_id: str, filename: str):
    """Download a file from a completed task"""
    if task_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = running_tasks[task_id]
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail="Task not completed")
    
    file_path = Path("output") / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/octet-stream"
    )

@app.get("/latest")
async def get_latest_results():
    """Get the latest scraping results"""
    output_dir = Path("output")
    if not output_dir.exists():
        raise HTTPException(status_code=404, detail="No output directory found")
    
    # Find the most recent CSV file
    csv_files = list(output_dir.glob("*.csv"))
    if not csv_files:
        raise HTTPException(status_code=404, detail="No CSV files found")
    
    latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
    
    return FileResponse(
        path=str(latest_file),
        filename=latest_file.name,
        media_type="text/csv"
    )

@app.post("/crawl-ids", response_model=ScrapeResponse)
async def start_id_crawl(request: IDCrawlRequest, background_tasks: BackgroundTasks):
    """Start crawling specific Walmart item IDs."""
    task_id = f"id_crawl_{int(time.time())}"
    
    # Parse item IDs
    item_ids = [id.strip() for id in request.item_ids.split(",") if id.strip()]
    if not item_ids:
        raise HTTPException(status_code=400, detail="No valid item IDs provided")
    
    # Store task info
    running_tasks[task_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "request": request.dict(),
        "output_files": [],
        "type": "id_crawl"
    }
    
    # Start background task with fast crawler
    background_tasks.add_task(run_fast_id_crawl_task, task_id, request, item_ids)
    
    return ScrapeResponse(
        task_id=task_id,
        status="started",
        message=f"FAST ID crawling task started for {len(item_ids)} item IDs",
        timestamp=datetime.now().isoformat()
    )

async def run_fast_id_crawl_task(task_id: str, request: IDCrawlRequest, item_ids: List[str]):
    """Background task to run FAST ID crawler."""
    try:
        # Prepare export formats
        export_formats = [request.export] if isinstance(request.export, str) else request.export
        
        # Run the FAST ID crawler
        results = await run_fast_id_crawler(
            item_ids=item_ids,
            export_formats=export_formats,
            debug=request.debug,
            sleep=request.sleep,
            skip_seller_enrichment=False,  # Keep seller enrichment for API
            max_concurrent=10
        )
        
        # Update task status
        running_tasks[task_id].update({
            "status": "completed",
            "end_time": datetime.now().isoformat(),
            "results_count": len(results),
            "output_files": [f for f in os.listdir(get_config().output_dir) if f.startswith("walmart_id_crawl_fast")]
        })
        
    except Exception as e:
        running_tasks[task_id].update({
            "status": "failed",
            "end_time": datetime.now().isoformat(),
            "error": str(e)
        })

async def run_id_crawl_task(task_id: str, request: IDCrawlRequest, item_ids: List[str]):
    """Background task to run ID crawler."""
    try:
        # Prepare export formats
        export_formats = [request.export] if isinstance(request.export, str) else request.export
        
        # Run the ID crawler
        results = await run_id_crawler(
            item_ids=item_ids,
            export_formats=export_formats,
            debug=request.debug,
            sleep=request.sleep
        )
        
        # Update task status
        running_tasks[task_id].update({
            "status": "completed",
            "end_time": datetime.now().isoformat(),
            "results_count": len(results),
            "output_files": [f for f in os.listdir(get_config().output_dir) if f.startswith("walmart_id_crawl")]
        })
        
    except Exception as e:
        running_tasks[task_id].update({
            "status": "failed",
            "end_time": datetime.now().isoformat(),
            "error": str(e)
        })

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task from memory"""
    if task_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    del running_tasks[task_id]
    return {"message": "Task deleted successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
