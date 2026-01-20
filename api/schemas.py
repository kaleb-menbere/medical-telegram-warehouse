from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

# Response Models
class ChannelBase(BaseModel):
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float
    posts_with_media: int

class ChannelResponse(ChannelBase):
    channel_key: int
    first_post_date: datetime
    last_post_date: datetime
    
    class Config:
        from_attributes = True

class MessageBase(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    message_length: int
    view_count: int
    forward_count: int
    has_media: bool
    post_timestamp: datetime

class MessageResponse(MessageBase):
    date_key: int
    
    class Config:
        from_attributes = True

class TopProduct(BaseModel):
    product_term: str
    mention_count: int
    avg_views: float
    channels: List[str]

class ChannelActivity(BaseModel):
    date: str
    post_count: int
    total_views: int
    avg_views: float

class VisualContentStats(BaseModel):
    channel_name: str
    total_posts: int
    posts_with_images: int
    image_percentage: float
    lifestyle_count: int
    product_display_count: int
    other_count: int

class SearchResponse(BaseModel):
    messages: List[MessageResponse]
    total_count: int
    page: int
    limit: int

class HealthCheck(BaseModel):
    status: str
    database: str
    timestamp: datetime
    models_count: int
    messages_count: int