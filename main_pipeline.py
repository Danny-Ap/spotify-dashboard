#!/usr/bin/env python3
"""
Spotify Analytics Pipeline Orchestrator
Runs the complete data collection and enrichment pipeline with conditional logic.

Pipeline Flow:
1. Fetch recent tracks from Spotify API
2. If new tracks found (>0), continue to process new content
3. Enrich with lyrics and language detection
4. Validate data integrity

Collections Used:
- StreamingHistory: Main streaming data
- songs_master: Unique songs collection  
- artists_master: Unique artists collection
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

# Import pipeline modules
from data_collection.fetch_recent_tracks import main as fetch_tracks
from data_collection.process_new_content import main as process_content
from enrichment.enrich_with_lyrics import main as enrich_lyrics
from enrichment.validate_data import main as validate_data

def setup_logging():
    """Setup logging to file with timestamp"""
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Create timestamped log file
    timestamp = datetime.now().strftime("%d-%m-%Y_%H-%M")
    log_file = log_dir / f"{timestamp}.txt"
    
    # Remove any existing handlers to prevent conflicts
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Configure logging with force=True to override any existing config
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        force=True  # This ensures we override any existing logging config
    )
    
    # Get logger and test that it works
    logger = logging.getLogger(__name__)
    logger.info(f"📝 Logging initialized - writing to: {log_file}")
    
    return logger

def main():
    """Run the complete Spotify analytics pipeline"""
    logger = setup_logging()
    
    logger.info("=" * 80)
    logger.info("🎵 STARTING SPOTIFY ANALYTICS PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Pipeline Version: 2.0")
    logger.info("=" * 80)
    
    try:
        # ===== STEP 1: FETCH RECENT TRACKS =====
        logger.info("")
        logger.info("📥 STEP 1: FETCHING RECENT TRACKS FROM SPOTIFY API")
        logger.info("-" * 60)
        
        new_tracks_count = fetch_tracks()
        
        logger.info(f"✅ Step 1 Complete: {new_tracks_count} new tracks found")
        
        # Check if we should continue pipeline
        if new_tracks_count == 0:
            logger.info("")
            logger.info("🛑 PIPELINE STOPPING: No new tracks found")
            logger.info("All recent tracks are already in the database.")
            logger.info("=" * 80)
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            
            # Ensure logs are flushed before returning
            for handler in logging.getLogger().handlers:
                handler.flush()
            
            return
        
        logger.info(f"🎶 Found {new_tracks_count} new tracks. Continuing pipeline...")
        
        # ===== STEP 2: PROCESS NEW CONTENT =====
        logger.info("")
        logger.info("🔍 STEP 2: PROCESSING NEW SONGS AND ARTISTS")
        logger.info("-" * 60)
        
        process_content()
        logger.info("✅ Step 2 Complete: New songs and artists processed")
        
        # ===== STEP 3: ENRICH WITH LYRICS =====
        logger.info("")
        logger.info("📝 STEP 3: ENRICHING WITH LYRICS AND LANGUAGE DETECTION")
        logger.info("-" * 60)
        
        enrich_lyrics()
        logger.info("✅ Step 3 Complete: Lyrics and language detection finished")
        
        # ===== STEP 4: VALIDATE DATA =====
        logger.info("")
        logger.info("🔍 STEP 4: VALIDATING DATA INTEGRITY")
        logger.info("-" * 60)
        
        validate_data()
        logger.info("✅ Step 4 Complete: Data validation finished")
        
        # ===== PIPELINE SUMMARY =====
        logger.info("")
        logger.info("=" * 80)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"📊 Pipeline Summary:")
        logger.info(f"   • New tracks processed: {new_tracks_count}")
        logger.info(f"   • New songs and artists added to master collections")
        logger.info(f"   • Lyrics fetched and languages detected")
        logger.info(f"   • Data validation completed")
        logger.info("")
        logger.info("🔄 Next run will check for new tracks in 2 hours")
        logger.info("📊 Dashboard will automatically reflect new data")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("")
        logger.error("=" * 80)
        logger.error("❌ PIPELINE FAILED")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}")
        logger.error("Stack trace:", exc_info=True)
        logger.error("=" * 80)
        
        # Re-raise the exception for GitHub Actions to detect failure
        raise
    
    finally:
        # Ensure all logs are written to file before exiting
        logger.info("🔄 Flushing logs to file...")
        for handler in logging.getLogger().handlers:
            handler.flush()
        
        # Also explicitly close file handlers
        for handler in logging.getLogger().handlers:
            if isinstance(handler, logging.FileHandler):
                handler.close()

if __name__ == "__main__":
    main()
