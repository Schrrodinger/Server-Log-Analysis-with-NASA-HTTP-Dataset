# from fastapi import FastAPI, HTTPException, Query
# from fastapi.middleware.cors import CORSMiddleware
# from service import LogAnalysisService
# from typing import List, Optional
# from datetime import datetime
# import logging
# from pydantic import BaseModel

# # Set up logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# app = FastAPI(title="NASA Logs Analysis API")

# # Add CORS middleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # Initialize service
# service = LogAnalysisService()

# class DateRange(BaseModel):
#     start_date: datetime
#     end_date: datetime

# @app.on_event("startup")
# async def startup_event():
#     """Load data when the application starts"""
#     try:
#         await service.initialize()
#         logger.info("Application started and data loaded successfully")
#     except Exception as e:
#         logger.error(f"Failed to initialize application: {e}")
#         raise

# @app.get("/logs/status_distribution")
# async def get_status_distribution(
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     methods: Optional[List[str]] = Query(None)
# ):
#     """Get distribution of HTTP status codes with optional filtering"""
#     try:
#         return await service.get_status_distribution(start_date, end_date, methods)
#     except Exception as e:
#         logger.error(f"Error in status distribution: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/logs/top_endpoints")
# async def get_top_endpoints(
#     limit: int = 20,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     methods: Optional[List[str]] = Query(None)
# ):
#     """Get most frequently accessed endpoints with optional filtering"""
#     try:
#         return await service.get_top_endpoints(limit, start_date, end_date, methods)
#     except Exception as e:
#         logger.error(f"Error in top endpoints: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/logs/method_distribution")
# async def get_method_distribution(
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None
# ):
#     """Get distribution of HTTP methods"""
#     try:
#         return await service.get_method_distribution(start_date, end_date)
#     except Exception as e:
#         logger.error(f"Error in method distribution: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/logs/content_size_distribution")
# async def get_content_size_distribution(
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None,
#     bin_count: int = 50
# ):
#     """Get distribution of response content sizes"""
#     try:
#         return await service.get_content_size_distribution(start_date, end_date, bin_count)
#     except Exception as e:
#         logger.error(f"Error in content size distribution: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/logs/host_frequency")
# async def get_host_frequency(
#     limit: int = 20,
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None
# ):
#     """Get most frequent requesting hosts"""
#     try:
#         return await service.get_host_frequency(limit, start_date, end_date)
#     except Exception as e:
#         logger.error(f"Error in host frequency: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/logs/traffic_pattern")
# async def get_traffic_pattern(
#     pattern_type: str = Query(..., enum=["hourly", "daily"]),
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None
# ):
#     """Get traffic patterns (hourly or daily)"""
#     try:
#         return await service.get_traffic_pattern(pattern_type, start_date, end_date)
#     except Exception as e:
#         logger.error(f"Error in traffic pattern: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# @app.get("/logs/statistics")
# async def get_statistics(
#     start_date: Optional[datetime] = None,
#     end_date: Optional[datetime] = None
# ):
#     """Get overall statistics"""
#     try:
#         return await service.get_statistics(start_date, end_date)
#     except Exception as e:
#         logger.error(f"Error in statistics: {e}")
#         raise HTTPException(status_code=500, detail=str(e))

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8001)

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from service import LogAnalysisService
from typing import List, Optional
from datetime import datetime
import logging
from pydantic import BaseModel

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="NASA Logs Analysis API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize service
service = LogAnalysisService()

class DateRange(BaseModel):
    start_date: datetime
    end_date: datetime

@app.on_event("startup")
async def startup_event():
    """Load data when the application starts"""
    try:
        await service.initialize()
        logger.info("Application started and data loaded successfully")
    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        raise

@app.get("/logs/status_distribution")
async def get_status_distribution(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    methods: Optional[List[str]] = Query(None)
):
    """Get distribution of HTTP status codes with optional filtering"""
    try:
        return await service.get_status_distribution(start_date, end_date, methods)
    except Exception as e:
        logger.error(f"Error in status distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/top_endpoints")
async def get_top_endpoints(
    limit: int = 20,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    methods: Optional[List[str]] = Query(None)
):
    """Get most frequently accessed endpoints with optional filtering"""
    try:
        return await service.get_top_endpoints(limit, start_date, end_date, methods)
    except Exception as e:
        logger.error(f"Error in top endpoints: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/method_distribution")
async def get_method_distribution(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Get distribution of HTTP methods"""
    try:
        return await service.get_method_distribution(start_date, end_date)
    except Exception as e:
        logger.error(f"Error in method distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/content_size_distribution")
async def get_content_size_distribution(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    bin_count: int = 50
):
    """Get distribution of response content sizes"""
    try:
        return await service.get_content_size_distribution(start_date, end_date, bin_count)
    except Exception as e:
        logger.error(f"Error in content size distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/host_frequency")
async def get_host_frequency(
    limit: int = 20,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Get most frequent requesting hosts"""
    try:
        return await service.get_host_frequency(limit, start_date, end_date)
    except Exception as e:
        logger.error(f"Error in host frequency: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/traffic_pattern")
async def get_traffic_pattern(
    pattern_type: str = Query(..., enum=["hourly", "daily"]),
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Get traffic patterns (hourly or daily)"""
    try:
        return await service.get_traffic_pattern(pattern_type, start_date, end_date)
    except Exception as e:
        logger.error(f"Error in traffic pattern: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/statistics")
async def get_statistics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Get overall statistics"""
    try:
        return await service.get_statistics(start_date, end_date)
    except Exception as e:
        logger.error(f"Error in statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
