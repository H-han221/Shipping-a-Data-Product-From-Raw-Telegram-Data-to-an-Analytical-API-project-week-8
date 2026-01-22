from pydantic import BaseModel

class TopProduct(BaseModel):
    product: str
    count: int

class MessageResult(BaseModel):
    message_id: str
    message_text: str
    views: int
