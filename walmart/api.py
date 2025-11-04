from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import os
import sys
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
    ENHANCEMENTS_AVAILABLE = False
    logger.warning(f"Enhanced features not available: {e}")

# Import enhanced exporters for integration format
try:
    from enhanced_exporters import export_csv_enhanced, export_json_enhanced
    ENHANCED_EXPORTS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Enhanced exports not available: {e}")
    ENHANCED_EXPORTS_AVAILABLE = False

# Request Models
class ScrapeRequest(BaseModel):
    keywords: str = "nike"
    max_per_keyword: int = 10
    sleep: int = 1
    export: str = "csv"
    debug: bool = False
    walmart_domain: Optional[str] = "walmart.com"  # e.g., "walmart.com", "walmart.ca", "walmart.com.mx"
    category_id: Optional[str] = None
    retry_seller_passes: int = 3
    retry_seller_delay: int = 5
    
    # Enhanced options (now default for main endpoint)
    export_format: Optional[str] = "csv"  # "csv", "json", "both"
    include_metadata: bool = True

class IDCrawlRequest(BaseModel):
    item_ids: str  # Comma-separated item IDs
    export: str = "csv"
    sleep: int = 1
    walmart_domain: Optional[str] = None

class ScrapeResponse(BaseModel):
    task_id: str
    status: str
    message: str
    timestamp: str

app = FastAPI(
    title="Walmart Scraper API",
    description="Enhanced API for running Walmart product and seller scraping with data quality, performance optimization, and reliability features",
    version="2.0.0"
)

# Store running tasks
running_tasks = {}

# Initialize enhancement systems (optional)
data_quality_manager = None
if ENHANCEMENTS_AVAILABLE:
    try:
        data_quality_manager = DataQualityManager()
    except Exception as e:
        logger.warning(f"Data quality manager not available: {e}")

@app.on_event("startup")
async def startup_event():
    """Initialize enhancement systems on startup"""
    if ENHANCEMENTS_AVAILABLE:
        try:
            # Initialize reliability system
            from reliability_system import reliability_manager
            reliability_manager.initialize()

            # Initialize performance optimizer (Redis will be optional)
            try:
                from performance_optimizer import performance_optimizer
                await performance_optimizer.initialize()
                logger.info("✅ Performance optimization enabled")
            except Exception as e:
                logger.warning(f"⚠️ Performance optimization disabled: {e}")

            logger.info("🚀 Enhanced Walmart Scraper API initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize enhancement systems: {e}")
    else:
        logger.info("🚀 Walmart Scraper API initialized (basic mode)")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if ENHANCEMENTS_AVAILABLE:
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
    walmart_domain: Optional[str] = "walmart.com"  # e.g., "walmart.com", "walmart.ca", "walmart.com.mx"
    category_id: Optional[str] = None
    retry_seller_passes: int = 3
    retry_seller_delay: int = 5
    
    # Enhanced options (now default for main endpoint)
    export_format: Optional[str] = "csv"  # "csv", "json", "both"
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


@app.post("/scan", response_model=ScrapeResponse)
async def start_scan(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """Start a Walmart scan"""
    scan_id = f"scan_{int(time.time())}"
    
    # Parse keywords for metadata
    keywords = [k.strip() for k in request.keywords.split(",") if k.strip()]
    
    # Store task info
    running_tasks[scan_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "keywords": keywords,
        "domain": request.walmart_domain or "walmart.com",
        "max_per_keyword": request.max_per_keyword,
        "items_collected": 0,
        "request": request.model_dump()
    }
    
    # Start background task
    background_tasks.add_task(run_enhanced_scrape_task, scan_id, request)
    
    return ScrapeResponse(
        task_id=scan_id,
        status="started",
        message="Scan started successfully",
        timestamp=datetime.now().isoformat()
    )

async def run_enhanced_scrape_task(task_id: str, request: ScrapeRequest):
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
        
        # Get the actual output directory from config
        cfg = get_config()
        output_dir_path = Path(cfg.output_dir)
        
        # Update task status
        running_tasks[task_id]["status"] = "completed"
        running_tasks[task_id]["end_time"] = datetime.now().isoformat()
        running_tasks[task_id]["result"] = result

        # Find output files and set the latest one as output_file
        if output_dir_path.exists():
            output_files = list(output_dir_path.glob("*.csv")) + list(output_dir_path.glob("*.json"))
            if output_files:
                running_tasks[task_id]["output_files"] = [str(f) for f in output_files]
                # Set the most recent file as the output_file for JSON results endpoint
                latest_file = max(output_files, key=lambda f: f.stat().st_mtime)
                running_tasks[task_id]["output_file"] = str(latest_file)
            
    except Exception as e:
        running_tasks[task_id]["status"] = "failed"
        running_tasks[task_id]["error"] = str(e)
        running_tasks[task_id]["end_time"] = datetime.now().isoformat()




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

@app.delete("/scan/{scan_id}")
async def delete_scan(scan_id: str):
    """Delete a scan from memory"""
    if scan_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Scan not found")
    
    del running_tasks[scan_id]
    return {"message": "Scan deleted successfully"}

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

@app.get("/rate-limit")
async def get_rate_limit_info():
    """Get rate limit information"""
    return {
        "rate_limit": "No rate limiting implemented",
        "api_key": "Active BlueCart API key configured",
        "domains": ["walmart.com", "walmart.ca"],
        "message": "Use sleep parameter to control request frequency"
    }

@app.get("/scans")
async def get_all_scans():
    """Get all scans"""
    return {
        "total_scans": len(running_tasks),
        "active_scans": len([t for t in running_tasks.values() if t["status"] == "running"]),
        "completed_scans": len([t for t in running_tasks.values() if t["status"] == "completed"]),
        "failed_scans": len([t for t in running_tasks.values() if t["status"] == "failed"]),
        "scans": {scan_id: {"status": task["status"], "start_time": task.get("start_time")} 
                 for scan_id, task in running_tasks.items()}
    }

@app.get("/scan/{scan_id}/status")
async def get_scan_status(scan_id: str):
    """Get scan status by scan_id"""
    if scan_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Scan not found")
    
    task = running_tasks[scan_id]
    return {
        "scan_id": scan_id,
        "status": task["status"],
        "start_time": task.get("start_time"),
        "end_time": task.get("end_time"),
        "keywords": task.get("keywords", []),
        "domain": task.get("domain", "walmart.com"),
        "items_collected": task.get("items_collected", 0)
    }

@app.get("/scan/{scan_id}/results")
async def get_scan_results(scan_id: str):
    """Get scan results as JSON by scan_id"""
    if scan_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Scan not found")
    
    task = running_tasks[scan_id]
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail="Scan not completed yet")
    
    # Try to load the JSON results file
    output_file = task.get("output_file")
    if output_file and os.path.exists(output_file):
        try:
            import json
            with open(output_file, 'r', encoding='utf-8') as f:
                results_data = json.load(f)
            
            return {
                "scan_id": scan_id,
                "status": "completed",
                "items_collected": task.get("items_collected", 0),
                "keywords": task.get("keywords", []),
                "domain": task.get("domain", "walmart.com"),
                "results": results_data
            }
        except Exception as e:
            logger.error(f"Error loading JSON results: {e}")
    
    # Fallback to task metadata if file not found
    return {
        "scan_id": scan_id,
        "status": "completed",
        "items_collected": task.get("items_collected", 0),
        "keywords": task.get("keywords", []),
        "domain": task.get("domain", "walmart.com"),
        "results": [],
        "message": "Results file not found"
    }

@app.get("/scan/{scan_id}/results/csv")
async def get_scan_results_csv(scan_id: str):
    """Download CSV results by scan_id"""
    if scan_id not in running_tasks:
        raise HTTPException(status_code=404, detail="Scan not found")
    
    task = running_tasks[scan_id]
    if task["status"] != "completed":
        raise HTTPException(status_code=400, detail="Scan not completed yet")
    
    # Find the output file for this scan
    output_dir = get_config().output_dir
    if os.path.exists(output_dir):
        # Look for files that might belong to this scan
        for filename in os.listdir(output_dir):
            if filename.endswith('.csv') and os.path.getsize(os.path.join(output_dir, filename)) > 500:
                file_path = os.path.join(output_dir, filename)
                from fastapi.responses import FileResponse
                return FileResponse(
                    path=file_path,
                    filename=filename,
                    media_type='text/csv'
                )
    
    raise HTTPException(status_code=404, detail="No CSV results found")

@app.post("/scan/items")
async def start_item_scan(request: IDCrawlRequest, background_tasks: BackgroundTasks):
    """Start scanning specific Walmart item IDs"""
    scan_id = f"items_{int(time.time())}"
    
    # Parse item IDs
    item_ids = [id.strip() for id in request.item_ids.split(",") if id.strip()]
    
    # Store task info
    running_tasks[scan_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "item_ids": item_ids,
        "domain": request.walmart_domain or "walmart.com",
        "items_collected": 0,
        "request": request.model_dump()
    }
    
    # Start background task for ID crawling
    background_tasks.add_task(run_id_crawl_task, scan_id, request)
    
    return ScrapeResponse(
        task_id=scan_id,
        status="started",
        message="Item scan started successfully",
        timestamp=datetime.now().isoformat()
    )

async def run_id_crawl_task(scan_id: str, request: IDCrawlRequest):
    """Background task to crawl specific item IDs"""
    try:
        # Prepare arguments for ID crawler
        args = [
            "--item-ids", request.item_ids,
            "--export", request.export,
            "--sleep", str(request.sleep),
        ]
        
        if request.walmart_domain:
            args.extend(["--walmart-domain", request.walmart_domain])
        
        # Run the ID crawler
        result = asyncio.get_event_loop().run_in_executor(
            None, run_id_crawler, args
        )
        await result
        
        # Update task status
        running_tasks[scan_id].update({
            "status": "completed",
            "end_time": datetime.now().isoformat(),
            "items_collected": len(request.item_ids.split(","))
        })
        
    except Exception as e:
        logger.error(f"ID crawl task {scan_id} failed: {e}")
        running_tasks[scan_id].update({
            "status": "failed",
            "end_time": datetime.now().isoformat(),
            "error": str(e)
        })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
