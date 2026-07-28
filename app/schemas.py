from typing import Optional
from pydantic import BaseModel, Field

# Схемы валидации данных (Pydantic)
class CallSchema(BaseModel):
    anum: int = Field(..., ge=1)
    bnum: int = Field(..., ge=1)
    timestamp: int = Field(..., description="Unix timestamp в секундах", examples=[1752116172])

    # Метод для автоматической сборки формата "схема + данные" для Kafka
    def to_kafka_connect_json(self) -> dict:
        return {
            "schema": {
                "type": "struct",
                "optional": False,
                "version": 1,
                "fields": [
                    {"type": "int64", "optional": False, "field": "anum"},
                    {"type": "int64", "optional": False, "field": "bnum"},
                    {"type": "int64", "optional": False, "field": "timestamp"}
                ]
            },
            "payload": {
                "anum": self.anum,
                "bnum": self.bnum,
                "timestamp": self.timestamp
            }
        }

class StatsRequestSchema(BaseModel):
    anum: Optional[int] = Field(None)

class StatsResponseSchema(BaseModel):
    anum: int
    cnt: int
