"""
Progress Tracking System
Real-time progress updates, ETA calculations, and resume capability
"""
import json
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from pathlib import Path
import threading
import logging

logger = logging.getLogger(__name__)

@dataclass
class ProgressSnapshot:
    """Snapshot of current progress"""
    task_id: str
    status: str  # "running", "completed", "failed", "paused"
    total_keywords: int
    completed_keywords: int
    current_keyword: str
    items_collected: int
    target_items: int
    pages_scraped: int
    start_time: str
    last_update: str
    estimated_completion: Optional[str] = None
    errors: List[str] = None
    resume_data: Dict[str, Any] = None

@dataclass
class KeywordProgress:
    """Progress for a single keyword"""
    keyword: str
    status: str  # "pending", "running", "completed", "failed"
    items_found: int
    pages_scraped: int
    errors: List[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

class ProgressTracker:
    """Real-time progress tracking with ETA calculations and resume capability"""
    
    def __init__(self, task_id: str, keywords: List[str], target_items_per_keyword: int):
        self.task_id = task_id
        self.keywords = keywords
        self.target_items_per_keyword = target_items_per_keyword
        self.total_target_items = len(keywords) * target_items_per_keyword
        
        # Progress state
        self.current_keyword_index = 0
        self.current_keyword = ""
        self.items_collected = 0
        self.pages_scraped = 0
        self.start_time = datetime.now()
        self.keyword_progress: Dict[str, KeywordProgress] = {}
        self.errors: List[str] = []
        self.resume_data: Dict[str, Any] = {}
        
        # Initialize keyword progress
        for keyword in keywords:
            self.keyword_progress[keyword] = KeywordProgress(
                keyword=keyword,
                status="pending",
                items_found=0,
                pages_scraped=0
            )
        
        # File paths
        self.progress_file = Path(f"output/progress_{task_id}.json")
        self.resume_file = Path(f"output/resume_{task_id}.json")
        
        # Thread safety
        self.lock = threading.Lock()
        
        # Save initial state
        self.save_progress()
    
    def start_keyword(self, keyword: str) -> None:
        """Mark keyword as started"""
        with self.lock:
            self.current_keyword = keyword
            if keyword in self.keyword_progress:
                self.keyword_progress[keyword].status = "running"
                self.keyword_progress[keyword].start_time = datetime.now().isoformat()
            self.save_progress()
            logger.info(f"Started keyword: {keyword}")
    
    def update_keyword_progress(self, keyword: str, items_found: int, pages_scraped: int) -> None:
        """Update progress for current keyword"""
        with self.lock:
            if keyword in self.keyword_progress:
                self.keyword_progress[keyword].items_found = items_found
                self.keyword_progress[keyword].pages_scraped = pages_scraped
            
            # Update overall progress
            self.items_collected = sum(kp.items_found for kp in self.keyword_progress.values())
            self.pages_scraped = sum(kp.pages_scraped for kp in self.keyword_progress.values())
            
            self.save_progress()
    
    def complete_keyword(self, keyword: str, items_found: int) -> None:
        """Mark keyword as completed"""
        with self.lock:
            if keyword in self.keyword_progress:
                self.keyword_progress[keyword].status = "completed"
                self.keyword_progress[keyword].items_found = items_found
                self.keyword_progress[keyword].end_time = datetime.now().isoformat()
            
            self.current_keyword_index += 1
            if self.current_keyword_index < len(self.keywords):
                self.current_keyword = self.keywords[self.current_keyword_index]
            else:
                self.current_keyword = ""
            
            self.save_progress()
            logger.info(f"Completed keyword: {keyword} ({items_found} items)")
    
    def add_error(self, error: str) -> None:
        """Add error to progress tracking"""
        with self.lock:
            self.errors.append(f"{datetime.now().isoformat()}: {error}")
            if self.current_keyword and self.current_keyword in self.keyword_progress:
                if not self.keyword_progress[self.current_keyword].errors:
                    self.keyword_progress[self.current_keyword].errors = []
                self.keyword_progress[self.current_keyword].errors.append(error)
            self.save_progress()
    
    def calculate_eta(self) -> Optional[str]:
        """Calculate estimated time to completion"""
        if self.items_collected == 0:
            return None
        
        elapsed_time = datetime.now() - self.start_time
        items_per_second = self.items_collected / elapsed_time.total_seconds()
        
        if items_per_second == 0:
            return None
        
        remaining_items = self.total_target_items - self.items_collected
        if remaining_items <= 0:
            return None
        
        remaining_seconds = remaining_items / items_per_second
        eta = datetime.now() + timedelta(seconds=remaining_seconds)
        
        return eta.isoformat()
    
    def get_progress_snapshot(self) -> ProgressSnapshot:
        """Get current progress snapshot"""
        completed_keywords = sum(1 for kp in self.keyword_progress.values() if kp.status == "completed")
        
        return ProgressSnapshot(
            task_id=self.task_id,
            status="running",
            total_keywords=len(self.keywords),
            completed_keywords=completed_keywords,
            current_keyword=self.current_keyword,
            items_collected=self.items_collected,
            target_items=self.total_target_items,
            pages_scraped=self.pages_scraped,
            start_time=self.start_time.isoformat(),
            last_update=datetime.now().isoformat(),
            estimated_completion=self.calculate_eta(),
            errors=self.errors.copy(),
            resume_data=self.resume_data.copy()
        )
    
    def save_progress(self) -> None:
        """Save progress to file"""
        try:
            os.makedirs(self.progress_file.parent, exist_ok=True)
            
            snapshot = self.get_progress_snapshot()
            with open(self.progress_file, 'w') as f:
                json.dump(asdict(snapshot), f, indent=2)
            
            # Also save resume data
            resume_data = {
                "task_id": self.task_id,
                "keywords": self.keywords,
                "current_keyword_index": self.current_keyword_index,
                "completed_keywords": [k for k, v in self.keyword_progress.items() if v.status == "completed"],
                "keyword_progress": {k: asdict(v) for k, v in self.keyword_progress.items()},
                "target_items_per_keyword": self.target_items_per_keyword,
                "timestamp": datetime.now().isoformat()
            }
            
            with open(self.resume_file, 'w') as f:
                json.dump(resume_data, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save progress: {e}")
    
    def mark_completed(self) -> None:
        """Mark task as completed"""
        with self.lock:
            snapshot = self.get_progress_snapshot()
            snapshot.status = "completed"
            
            with open(self.progress_file, 'w') as f:
                json.dump(asdict(snapshot), f, indent=2)
            
            logger.info(f"Task {self.task_id} completed successfully")
    
    def mark_failed(self, error: str) -> None:
        """Mark task as failed"""
        with self.lock:
            self.add_error(error)
            snapshot = self.get_progress_snapshot()
            snapshot.status = "failed"
            
            with open(self.progress_file, 'w') as f:
                json.dump(asdict(snapshot), f, indent=2)
            
            logger.error(f"Task {self.task_id} failed: {error}")
    
    @classmethod
    def load_from_file(cls, task_id: str) -> Optional['ProgressTracker']:
        """Load progress tracker from file"""
        progress_file = Path(f"output/progress_{task_id}.json")
        resume_file = Path(f"output/resume_{task_id}.json")
        
        if not progress_file.exists() or not resume_file.exists():
            return None
        
        try:
            with open(resume_file, 'r') as f:
                resume_data = json.load(f)
            
            tracker = cls(
                task_id=task_id,
                keywords=resume_data["keywords"],
                target_items_per_keyword=resume_data["target_items_per_keyword"]
            )
            
            # Restore state
            tracker.current_keyword_index = resume_data["current_keyword_index"]
            tracker.keyword_progress = {
                k: KeywordProgress(**v) for k, v in resume_data["keyword_progress"].items()
            }
            
            # Calculate current totals
            tracker.items_collected = sum(kp.items_found for kp in tracker.keyword_progress.values())
            tracker.pages_scraped = sum(kp.pages_scraped for kp in tracker.keyword_progress.values())
            
            return tracker
            
        except Exception as e:
            logger.error(f"Failed to load progress tracker: {e}")
            return None
    
    def get_progress_percentage(self) -> float:
        """Get progress as percentage"""
        if self.total_target_items == 0:
            return 0.0
        return min(100.0, (self.items_collected / self.total_target_items) * 100)
    
    def get_keywords_remaining(self) -> List[str]:
        """Get list of remaining keywords"""
        return [k for k, v in self.keyword_progress.items() if v.status in ["pending", "running"]]
    
    def get_completion_summary(self) -> Dict[str, Any]:
        """Get completion summary"""
        completed = [k for k, v in self.keyword_progress.items() if v.status == "completed"]
        failed = [k for k, v in self.keyword_progress.items() if v.status == "failed"]
        running = [k for k, v in self.keyword_progress.items() if v.status == "running"]
        pending = [k for k, v in self.keyword_progress.items() if v.status == "pending"]
        
        return {
            "total_keywords": len(self.keywords),
            "completed": len(completed),
            "failed": len(failed),
            "running": len(running),
            "pending": len(pending),
            "progress_percentage": self.get_progress_percentage(),
            "items_collected": self.items_collected,
            "target_items": self.total_target_items,
            "eta": self.calculate_eta()
        }

class ProgressManager:
    """Manager for multiple progress trackers"""
    
    def __init__(self):
        self.trackers: Dict[str, ProgressTracker] = {}
        self.lock = threading.Lock()
    
    def create_tracker(self, task_id: str, keywords: List[str], target_items_per_keyword: int) -> ProgressTracker:
        """Create new progress tracker"""
        with self.lock:
            tracker = ProgressTracker(task_id, keywords, target_items_per_keyword)
            self.trackers[task_id] = tracker
            return tracker
    
    def get_tracker(self, task_id: str) -> Optional[ProgressTracker]:
        """Get existing tracker"""
        with self.lock:
            if task_id in self.trackers:
                return self.trackers[task_id]
            
            # Try to load from file
            tracker = ProgressTracker.load_from_file(task_id)
            if tracker:
                self.trackers[task_id] = tracker
            return tracker
    
    def get_all_trackers(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all trackers"""
        with self.lock:
            return {
                task_id: tracker.get_completion_summary()
                for task_id, tracker in self.trackers.items()
            }
    
    def cleanup_old_trackers(self, max_age_hours: int = 24) -> None:
        """Clean up old tracker files"""
        try:
            output_dir = Path("output")
            if not output_dir.exists():
                return
            
            cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
            
            for file_path in output_dir.glob("progress_*.json"):
                if file_path.stat().st_mtime < cutoff_time.timestamp():
                    file_path.unlink()
            
            for file_path in output_dir.glob("resume_*.json"):
                if file_path.stat().st_mtime < cutoff_time.timestamp():
                    file_path.unlink()
                    
        except Exception as e:
            logger.error(f"Failed to cleanup old trackers: {e}")

# Global progress manager
progress_manager = ProgressManager()

