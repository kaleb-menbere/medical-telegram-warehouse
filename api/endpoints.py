from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
from typing import List, Optional
from . import schemas
from .database import get_db

router = APIRouter()

@router.get("/health", response_model=schemas.HealthCheck)
def health_check(db: Session = Depends(get_db)):
    """Check API and database health"""
    try:
        # Check database connection
        db.execute(text("SELECT 1"))
        db_status = "connected"
        
        # Get counts
        models_count = db.execute(
            text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema LIKE 'medical_warehouse_%'")
        ).scalar()
        
        messages_count = db.execute(
            text("SELECT COUNT(*) FROM medical_warehouse_marts.fct_messages")
        ).scalar()
        
        return schemas.HealthCheck(
            status="healthy",
            database=db_status,
            timestamp=pd.Timestamp.now(),
            models_count=models_count,
            messages_count=messages_count
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {str(e)}")

@router.get("/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(
    limit: int = Query(10, description="Number of top products to return"),
    db: Session = Depends(get_db)
):
    """
    Get most frequently mentioned medical products or drugs across all channels.
    
    Extracts product terms from message text and counts mentions.
    """
    query = text("""
        WITH product_terms AS (
            SELECT 
                UNNEST(STRING_TO_ARRAY(LOWER(message_text), ' ')) as term,
                view_count,
                channel_name
            FROM medical_warehouse_marts.fct_messages fm
            JOIN medical_warehouse_marts.dim_channels dc ON fm.channel_key = dc.channel_key
            WHERE LENGTH(message_text) > 0
        ),
        filtered_terms AS (
            SELECT 
                term,
                COUNT(*) as mention_count,
                AVG(view_count) as avg_views,
                ARRAY_AGG(DISTINCT channel_name) as channels
            FROM product_terms
            WHERE term IN (
                'paracetamol', 'aspirin', 'ibuprofen', 'vitamin', 'calcium', 'zinc',
                'cream', 'ointment', 'tablet', 'capsule', 'syrup', 'injection',
                'antibiotic', 'antiseptic', 'analgesic', 'antihistamine',
                'd3', 'omega', 'protein', 'collagen', 'serum', 'lotion'
            )
            OR term ~ '^[0-9]+mg$'
            GROUP BY term
            HAVING COUNT(*) >= 2
        )
        SELECT 
            term as product_term,
            mention_count,
            ROUND(avg_views::numeric, 1) as avg_views,
            channels
        FROM filtered_terms
        ORDER BY mention_count DESC, avg_views DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {"limit": limit})
    products = []
    for row in result:
        products.append(schemas.TopProduct(
            product_term=row.product_term,
            mention_count=row.mention_count,
            avg_views=row.avg_views,
            channels=row.channels
        ))
    
    return products

@router.get("/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(
    channel_name: str,
    days: int = Query(7, description="Number of days to analyze"),
    db: Session = Depends(get_db)
):
    """
    Get posting activity and trends for a specific channel.
    """
    query = text("""
        SELECT 
            TO_CHAR(dd.full_date, 'YYYY-MM-DD') as date,
            COUNT(fm.message_id) as post_count,
            SUM(fm.view_count) as total_views,
            ROUND(AVG(fm.view_count)::numeric, 1) as avg_views
        FROM medical_warehouse_marts.fct_messages fm
        JOIN medical_warehouse_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        JOIN medical_warehouse_marts.dim_dates dd ON fm.date_key = dd.date_key
        WHERE dc.channel_name = :channel_name
          AND dd.full_date >= CURRENT_DATE - INTERVAL ':days days'
        GROUP BY dd.full_date
        ORDER BY dd.full_date DESC
    """)
    
    result = db.execute(query, {"channel_name": channel_name, "days": days})
    activities = []
    for row in result:
        activities.append(schemas.ChannelActivity(
            date=row.date,
            post_count=row.post_count,
            total_views=row.total_views,
            avg_views=row.avg_views
        ))
    
    return activities

@router.get("/search/messages", response_model=schemas.SearchResponse)
def search_messages(
    query: str = Query(..., description="Search keyword"),
    channel_name: Optional[str] = Query(None, description="Filter by channel"),
    limit: int = Query(20, description="Number of results"),
    page: int = Query(1, description="Page number"),
    db: Session = Depends(get_db)
):
    """
    Search for messages containing specific keywords.
    """
    offset = (page - 1) * limit
    
    # Build query
    sql = """
        SELECT 
            fm.message_id,
            dc.channel_name,
            fm.message_text,
            fm.message_length,
            fm.view_count,
            fm.forward_count,
            fm.has_media,
            dd.full_date as post_timestamp,
            fm.date_key
        FROM medical_warehouse_marts.fct_messages fm
        JOIN medical_warehouse_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        JOIN medical_warehouse_marts.dim_dates dd ON fm.date_key = dd.date_key
        WHERE LOWER(fm.message_text) LIKE LOWER(:search_term)
    """
    
    params = {"search_term": f"%{query}%", "limit": limit, "offset": offset}
    
    if channel_name:
        sql += " AND dc.channel_name = :channel_name"
        params["channel_name"] = channel_name
    
    # Count total
    count_sql = f"SELECT COUNT(*) FROM ({sql.replace('dd.full_date as post_timestamp, fm.date_key', '1')}) as subquery"
    total_count = db.execute(text(count_sql.replace("LIMIT :limit OFFSET :offset", "")), params).scalar()
    
    # Get paginated results
    sql += " ORDER BY fm.view_count DESC LIMIT :limit OFFSET :offset"
    
    result = db.execute(text(sql), params)
    
    messages = []
    for row in result:
        messages.append(schemas.MessageResponse(
            message_id=row.message_id,
            channel_name=row.channel_name,
            message_text=row.message_text,
            message_length=row.message_length,
            view_count=row.view_count,
            forward_count=row.forward_count,
            has_media=row.has_media,
            post_timestamp=row.post_timestamp,
            date_key=row.date_key
        ))
    
    return schemas.SearchResponse(
        messages=messages,
        total_count=total_count,
        page=page,
        limit=limit
    )

@router.get("/reports/visual-content", response_model=List[schemas.VisualContentStats])
def get_visual_content_stats(db: Session = Depends(get_db)):
    """
    Get statistics about image usage across channels.
    Includes YOLO detection categories.
    """
    query = text("""
        WITH channel_stats AS (
            SELECT 
                dc.channel_name,
                COUNT(DISTINCT fm.message_id) as total_posts,
                COUNT(DISTINCT fid.message_id) as posts_with_images
            FROM medical_warehouse_marts.fct_messages fm
            JOIN medical_warehouse_marts.dim_channels dc ON fm.channel_key = dc.channel_key
            LEFT JOIN medical_warehouse_marts.fct_image_detections fid ON fm.message_id = fid.message_id
            GROUP BY dc.channel_name
        ),
        category_stats AS (
            SELECT 
                dc.channel_name,
                fid.image_category,
                COUNT(*) as category_count
            FROM medical_warehouse_marts.fct_image_detections fid
            JOIN medical_warehouse_marts.fct_messages fm ON fid.message_id = fm.message_id
            JOIN medical_warehouse_marts.dim_channels dc ON fid.channel_key = dc.channel_key
            GROUP BY dc.channel_name, fid.image_category
        )
        SELECT 
            cs.channel_name,
            cs.total_posts,
            cs.posts_with_images,
            ROUND(100.0 * cs.posts_with_images / NULLIF(cs.total_posts, 0), 1) as image_percentage,
            COALESCE(MAX(CASE WHEN cat.image_category = 'lifestyle' THEN cat.category_count END), 0) as lifestyle_count,
            COALESCE(MAX(CASE WHEN cat.image_category = 'product_display' THEN cat.category_count END), 0) as product_display_count,
            COALESCE(MAX(CASE WHEN cat.image_category = 'other' THEN cat.category_count END), 0) as other_count
        FROM channel_stats cs
        LEFT JOIN category_stats cat ON cs.channel_name = cat.channel_name
        GROUP BY cs.channel_name, cs.total_posts, cs.posts_with_images
        ORDER BY image_percentage DESC
    """)
    
    result = db.execute(query)
    stats = []
    for row in result:
        stats.append(schemas.VisualContentStats(
            channel_name=row.channel_name,
            total_posts=row.total_posts,
            posts_with_images=row.posts_with_images,
            image_percentage=row.image_percentage,
            lifestyle_count=row.lifestyle_count,
            product_display_count=row.product_display_count,
            other_count=row.other_count
        ))
    
    return stats

@router.get("/channels", response_model=List[schemas.ChannelResponse])
def get_channels(db: Session = Depends(get_db)):
    """Get all channels with their statistics"""
    query = text("""
        SELECT 
            channel_key,
            channel_name,
            channel_type,
            total_posts,
            avg_views,
            posts_with_media,
            first_post_date,
            last_post_date
        FROM medical_warehouse_marts.dim_channels
        ORDER BY avg_views DESC
    """)
    
    result = db.execute(query)
    channels = []
    for row in result:
        channels.append(schemas.ChannelResponse(
            channel_key=row.channel_key,
            channel_name=row.channel_name,
            channel_type=row.channel_type,
            total_posts=row.total_posts,
            avg_views=float(row.avg_views),
            posts_with_media=row.posts_with_media,
            first_post_date=row.first_post_date,
            last_post_date=row.last_post_date
        ))
    
    return channels

@router.get("/messages/recent", response_model=List[schemas.MessageResponse])
def get_recent_messages(
    limit: int = Query(10, description="Number of recent messages"),
    db: Session = Depends(get_db)
):
    """Get most recent messages"""
    query = text("""
        SELECT 
            fm.message_id,
            dc.channel_name,
            fm.message_text,
            fm.message_length,
            fm.view_count,
            fm.forward_count,
            fm.has_media,
            dd.full_date as post_timestamp,
            fm.date_key
        FROM medical_warehouse_marts.fct_messages fm
        JOIN medical_warehouse_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        JOIN medical_warehouse_marts.dim_dates dd ON fm.date_key = dd.date_key
        ORDER BY dd.full_date DESC
        LIMIT :limit
    """)
    
    result = db.execute(query, {"limit": limit})
    messages = []
    for row in result:
        messages.append(schemas.MessageResponse(
            message_id=row.message_id,
            channel_name=row.channel_name,
            message_text=row.message_text,
            message_length=row.message_length,
            view_count=row.view_count,
            forward_count=row.forward_count,
            has_media=row.has_media,
            post_timestamp=row.post_timestamp,
            date_key=row.date_key
        ))
    
    return messages