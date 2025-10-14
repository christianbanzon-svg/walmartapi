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
import logging

# Add the walmart directory to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_config
from run_walmart import main as run_scraper
from run_walmart_id_crawler import run_id_crawler
from run_walmart_id_crawler_fast_simple import run_fast_id_crawler

# Setup logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import new enhancement systems (optional)
try:
    from data_quality import DataQualityManager, DataQualityReport
    from performance_optimizer import performance_optimizer, PerformanceOptimizer
    from reliability_system import reliability_manager, retry_with_circuit_breaker, RetryConfig, CircuitBreakerConfig
    ENHANCEMENTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Enhanced features not available: {e}")
    ENHANCEMENTS_AVAILABLE = False

# Import new improvements (optional)
try:
    from enhanced_exporters import export_csv_enhanced, export_json_enhanced, export_excel, get_export_preset, EXPORT_PRESETS
    from progress_tracker import progress_manager, ProgressTracker
    ENHANCED_EXPORTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Enhanced exports not available: {e}")
    ENHANCED_EXPORTS_AVAILABLE = False

app = FastAPI(
    title="Walmart Scraper API",
    description="Enhanced API for running Walmart product and seller scraping with data quality, performance optimization, and reliability features",
    version="2.0.0"
)

# Store running tasks
running_tasks = {}

# Initialize enhancement systems
data_quality_manager = DataQualityManager()

@app.on_event("startup")
async def startup_event():
    """Initialize enhancement systems on startup"""
    try:
        # Initialize reliability system
        reliability_manager.initialize()
        
        # Initialize performance optimizer (Redis will be optional)
        try:
            await performance_optimizer.initialize()
            logger.info("✅ Performance optimization enabled")
        except Exception as e:
            logger.warning(f"⚠️ Performance optimization disabled: {e}")
        
        logger.info("🚀 Enhanced Walmart Scraper API initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize enhancement systems: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    try:
        await performance_optimizer.shutdown()
        logger.info("✅ Enhancement systems shutdown complete")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

class ScrapeRequest(BaseModel):
    keywords: str = "nike"
    max_per_keyword: int = 10
    sleep: int = 1
    export: str = "csv"
    debug: bool = False
    walmart_domain: Optional[str] = None  # e.g., "walmart.com", "walmart.ca", "walmart.com.mx"
    category_id: Optional[str] = None
    retry_seller_passes: int = 3
    retry_seller_delay: int = 5
    
    # New enhanced options
    export_format: Optional[str] = "csv"  # "csv", "excel", "both"
    export_preset: Optional[str] = "detailed"  # "basic", "detailed", "seller_focus", "analytics", "full"
    custom_fields: Optional[List[str]] = None
    include_metadata: bool = True

class IDCrawlRequest(BaseModel):
    item_ids: str = "5245210374"
    export: str = "csv"
    debug: bool = False
    sleep: float = 0.5
    walmart_domain: Optional[str] = None  # e.g., "walmart.com", "walmart.ca", "walmart.com.mx"

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

@app.get("/locations")
async def get_locations():
    """Get all available locations (ZIP codes) for scraping"""
    return {
        "description": "Available Walmart store locations for scraping",
        "total_locations": 80,
        "categories": {
            "east_coast": [
                {"name": "New York, NY", "zipcode": "10001", "population": "8.8M"},
                {"name": "Boston, MA", "zipcode": "02101", "population": "695K"},
                {"name": "Philadelphia, PA", "zipcode": "19101", "population": "1.6M"},
                {"name": "Washington, DC", "zipcode": "20001", "population": "705K"},
                {"name": "Atlanta, GA", "zipcode": "30301", "population": "498K"},
                {"name": "Miami, FL", "zipcode": "33101", "population": "467K"},
                {"name": "Tampa, FL", "zipcode": "33601", "population": "384K"},
                {"name": "Orlando, FL", "zipcode": "32801", "population": "307K"},
                {"name": "Charlotte, NC", "zipcode": "28201", "population": "885K"},
                {"name": "Richmond, VA", "zipcode": "23219", "population": "226K"}
            ],
            "midwest": [
                {"name": "Chicago, IL", "zipcode": "60601", "population": "2.7M"},
                {"name": "Detroit, MI", "zipcode": "48201", "population": "639K"},
                {"name": "Cleveland, OH", "zipcode": "44101", "population": "383K"},
                {"name": "Columbus, OH", "zipcode": "43201", "population": "906K"},
                {"name": "Indianapolis, IN", "zipcode": "46201", "population": "887K"},
                {"name": "Milwaukee, WI", "zipcode": "53201", "population": "577K"},
                {"name": "Minneapolis, MN", "zipcode": "55401", "population": "429K"},
                {"name": "Kansas City, MO", "zipcode": "64101", "population": "508K"},
                {"name": "St. Louis, MO", "zipcode": "63101", "population": "301K"},
                {"name": "Cincinnati, OH", "zipcode": "45201", "population": "309K"}
            ],
            "west_coast": [
                {"name": "Los Angeles, CA", "zipcode": "90210", "population": "3.9M"},
                {"name": "San Francisco, CA", "zipcode": "94101", "population": "873K"},
                {"name": "San Diego, CA", "zipcode": "92101", "population": "1.4M"},
                {"name": "Sacramento, CA", "zipcode": "95814", "population": "524K"},
                {"name": "Seattle, WA", "zipcode": "98101", "population": "749K"},
                {"name": "Portland, OR", "zipcode": "97201", "population": "652K"},
                {"name": "Las Vegas, NV", "zipcode": "89101", "population": "641K"},
                {"name": "Phoenix, AZ", "zipcode": "85001", "population": "1.6M"},
                {"name": "Denver, CO", "zipcode": "80201", "population": "715K"},
                {"name": "Salt Lake City, UT", "zipcode": "84101", "population": "200K"}
            ],
            "south_southwest": [
                {"name": "Houston, TX", "zipcode": "77001", "population": "2.3M"},
                {"name": "Dallas, TX", "zipcode": "75201", "population": "1.3M"},
                {"name": "Austin, TX", "zipcode": "78701", "population": "965K"},
                {"name": "San Antonio, TX", "zipcode": "78201", "population": "1.5M"},
                {"name": "Fort Worth, TX", "zipcode": "76101", "population": "918K"},
                {"name": "Oklahoma City, OK", "zipcode": "73101", "population": "681K"},
                {"name": "Tulsa, OK", "zipcode": "74101", "population": "411K"},
                {"name": "Little Rock, AR", "zipcode": "72201", "population": "198K"},
                {"name": "Memphis, TN", "zipcode": "38101", "population": "633K"},
                {"name": "Nashville, TN", "zipcode": "37201", "population": "689K"}
            ],
            "walmart_corporate": [
                {"name": "Bentonville, AR", "zipcode": "72712", "note": "Walmart HQ"},
                {"name": "Rogers, AR", "zipcode": "72756", "note": "Near Walmart HQ"},
                {"name": "Springdale, AR", "zipcode": "72764", "note": "Near Walmart HQ"},
                {"name": "Fayetteville, AR", "zipcode": "72701", "note": "University of Arkansas"}
            ],
            "major_markets": [
                {"name": "Pittsburgh, PA", "zipcode": "15201", "population": "303K"},
                {"name": "Buffalo, NY", "zipcode": "14201", "population": "278K"},
                {"name": "Rochester, NY", "zipcode": "14601", "population": "211K"},
                {"name": "Albany, NY", "zipcode": "12201", "population": "99K"},
                {"name": "Hartford, CT", "zipcode": "06101", "population": "121K"},
                {"name": "Providence, RI", "zipcode": "02901", "population": "190K"},
                {"name": "Baltimore, MD", "zipcode": "21201", "population": "586K"},
                {"name": "Norfolk, VA", "zipcode": "23501", "population": "238K"},
                {"name": "Louisville, KY", "zipcode": "40201", "population": "617K"},
                {"name": "Lexington, KY", "zipcode": "40501", "population": "323K"}
            ]
        },
        "usage": "Copy any zipcode from this list and use it in the POST /scrape endpoint"
    }

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
    """Background task to run the scraper with built-in improvements"""
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
        if request.walmart_domain:
            args.extend(["--walmart-domain", request.walmart_domain])
        if request.category_id:
            args.extend(["--category-id", request.category_id])
        
        args.extend(["--retry-seller-passes", str(request.retry_seller_passes)])
        args.extend(["--retry-seller-delay", str(request.retry_seller_delay)])
        
        # Run the scraper with built-in improvements (caching, error handling, etc.)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_scraper, args)
        
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

@app.get("/domains")
async def get_domains():
    """Get all available Walmart domains for scraping"""
    domains = {
        "description": "Available Walmart domains for scraping via BlueCart API",
        "total_domains": 2,
        "default_domain": "walmart.com",
        "domains": {
            "united_states": {
                "domain": "walmart.com",
                "country": "United States",
                "region": "North America",
                "currency": "USD",
                "language": "English",
                "description": "Main US Walmart store",
                "status": "✅ Working",
                "notes": "Full product catalog available"
            },
            "canada": {
                "domain": "walmart.ca", 
                "country": "Canada",
                "region": "North America",
                "currency": "CAD",
                "language": "English/French",
                "description": "Canadian Walmart store",
                "status": "✅ Working",
                "notes": "Limited product catalog - fewer items available"
            }
        },
        "usage": {
            "api_parameter": "walmart_domain",
            "example_request": {
                "keywords": "nike",
                "max_per_keyword": 10,
                "walmart_domain": "walmart.ca"
            },
            "note": "If walmart_domain is not specified, defaults to walmart.com",
            "important": "Canadian Walmart (walmart.ca) has fewer products available than US Walmart"
        }
    }
    return domains

@app.delete("/tasks/{task_id}")
async def delete_task(task_id: str):
    """Delete a task from memory"""
    if task_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    del running_tasks[task_id]
    return {"message": "Task deleted successfully"}

# ===== SIMPLIFIED HEALTH CHECK =====

@app.get("/status")
async def get_simple_status():
    """Simple system status check"""
    return {
        "status": "healthy",
        "version": "2.0.0",
        "active_tasks": len(running_tasks),
        "features": [
            "Data Quality Management",
            "Performance Optimization", 
            "Reliability & Error Handling",
            "Enhanced CSV/Excel Export",
            "Real-time Progress Tracking"
        ],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/progress/{task_id}")
async def get_task_progress(task_id: str):
    """Get real-time progress for a specific task"""
    tracker = progress_manager.get_tracker(task_id)
    if not tracker:
        raise HTTPException(status_code=404, detail="Task not found or no progress available")
    
    return tracker.get_completion_summary()

@app.get("/progress")
async def get_all_progress():
    """Get progress for all active tasks"""
    return progress_manager.get_all_trackers()

@app.get("/export-presets")
async def get_export_presets():
    """Get available export field presets"""
    return {
        "presets": EXPORT_PRESETS,
        "description": "Predefined field selections for targeted exports",
        "usage": "Use preset name in 'export_preset' parameter"
    }

@app.post("/scrape-enhanced", response_model=ScrapeResponse)
async def start_enhanced_scrape(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start enhanced scraping with better CSV structure and progress tracking"""
    task_id = f"enhanced_{int(time.time())}"
    
    # Parse keywords
    keywords = [k.strip() for k in request.keywords.split(",") if k.strip()]
    if not keywords:
        raise HTTPException(status_code=400, detail="No valid keywords provided")
    
    # Store task info
    running_tasks[task_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "request": request.dict(),
        "output_files": [],
        "enhancements": {
            "enhanced_csv": True,
            "progress_tracking": True,
            "export_presets": True
        }
    }
    
    # Create progress tracker
    tracker = progress_manager.create_tracker(task_id, keywords, request.max_per_keyword)
    
    # Start background task
    background_tasks.add_task(run_enhanced_scrape_task, task_id, request, tracker)
    
    return ScrapeResponse(
        task_id=task_id,
        status="started",
        message=f"Enhanced scraping started for {len(keywords)} keywords with progress tracking",
        timestamp=datetime.now().isoformat()
    )

async def run_enhanced_scrape_task(task_id: str, request: ScrapeRequest, tracker: ProgressTracker):
    """Enhanced background task with progress tracking and better exports"""
    try:
        # Parse keywords
        keywords = [k.strip() for k in request.keywords.split(",") if k.strip()]
        
        # Prepare arguments for the scraper
        args = [
            "--keywords", request.keywords,
            "--max-per-keyword", str(request.max_per_keyword),
            "--sleep", str(request.sleep),
            "--export", "json",  # Always export JSON for enhanced processing
        ]
        
        if request.debug:
            args.append("--debug")
        if request.walmart_domain:
            args.extend(["--walmart-domain", request.walmart_domain])
        if request.category_id:
            args.extend(["--category-id", request.category_id])
        
        args.extend(["--retry-seller-passes", str(request.retry_seller_passes)])
        args.extend(["--retry-seller-delay", str(request.retry_seller_delay)])
        
        # Run the scraper
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_scraper, args)
        
        # Process results with enhanced export
        await process_enhanced_export(task_id, request, tracker)
        
        # Mark task as completed
        tracker.mark_completed()
        running_tasks[task_id]["status"] = "completed"
        running_tasks[task_id]["end_time"] = datetime.now().isoformat()
        
    except Exception as e:
        error_msg = f"Enhanced scrape task failed: {str(e)}"
        tracker.mark_failed(error_msg)
        running_tasks[task_id]["status"] = "failed"
        running_tasks[task_id]["error"] = error_msg
        running_tasks[task_id]["end_time"] = datetime.now().isoformat()
        logger.error(error_msg)

async def process_enhanced_export(task_id: str, request: ScrapeRequest, tracker: ProgressTracker):
    """Process and export data with enhanced features"""
    try:
        # Find the most recent JSON file
        output_dir = Path("output")
        json_files = list(output_dir.glob(f"walmart_scan_*.json"))
        if not json_files:
            raise Exception("No JSON export found")
        
        latest_json = max(json_files, key=lambda f: f.stat().st_mtime)
        
        # Load the data
        with open(latest_json, 'r', encoding='utf-8') as f:
            records = json.load(f)
        
        if not records:
            raise Exception("No data to export")
        
        # Determine export fields
        if request.custom_fields:
            export_fields = request.custom_fields
        elif request.export_preset and request.export_preset in EXPORT_PRESETS:
            export_fields = get_export_preset(request.export_preset)
        else:
            export_fields = None  # Use all fields
        
        output_files = []
        
        # Enhanced CSV export
        if request.export_format in ["csv", "both"]:
            csv_path = export_csv_enhanced(
                records=records,
                name_prefix=f"enhanced_{task_id}",
                custom_fields=export_fields,
                include_metadata=request.include_metadata,
                domain=request.walmart_domain or "walmart.com"
            )
            output_files.append(csv_path)
            logger.info(f"Enhanced CSV exported: {csv_path}")
        
        # Enhanced JSON export
        if request.export_format in ["json", "both"]:
            json_path = export_json_enhanced(
                records=records,
                name_prefix=f"enhanced_{task_id}",
                domain=request.walmart_domain or "walmart.com"
            )
            output_files.append(json_path)
            logger.info(f"Enhanced JSON exported: {json_path}")
        
        # Excel export
        if request.export_format in ["excel", "both"]:
            # For Excel, we'd need offers data too, but for now just use records
            excel_path = export_excel(
                records=records,
                offers=[],  # Could be enhanced to include offers
                name_prefix=f"enhanced_{task_id}"
            )
            output_files.append(excel_path)
            logger.info(f"Excel exported: {excel_path}")
        
        # Update task with output files
        running_tasks[task_id]["output_files"] = output_files
        
        # Log completion
        logger.info(f"Enhanced export completed for task {task_id}: {len(output_files)} files")
        
    except Exception as e:
        logger.error(f"Enhanced export failed for task {task_id}: {e}")
        raise

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
