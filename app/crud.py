import json
from typing import List, Optional
from main import *
from core.config import settings
from core.security import authenticate
from fastapi import APIRouter, Depends, HTTPException, status
from schemas import CallSchema, StatsRequestSchema, StatsResponseSchema

router = APIRouter()

# Эндпоинт 1: Отправка call в Kafka
@router.post("/calls", status_code=status.HTTP_201_CREATED, dependencies=[Depends(authenticate)])
async def create_call(call: CallSchema):
    payload = call.to_kafka_connect_json()
    # Конвертируем в json-строку и байты для отправки в топик
    message_bytes = json.dumps(payload).encode("utf-8")
    
    try:
        await kafka_producer.send_and_wait(settings.kafka_topic, message_bytes)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send to Kafka: {str(e)}")

# Эндпоинт 2: Агрегация статистики из Postgres
@router.post("/stats", response_model=List[StatsResponseSchema], dependencies=[Depends(authenticate)])
async def get_stats(filters: Optional[StatsRequestSchema] = None):
    # Если тело запроса пустое илиanum не передан
    if not filters or not filters.anum:
        query = """
            SELECT anum, COUNT(*) as cnt 
            FROM calls 
            GROUP BY anum 
            ORDER BY cnt DESC;
        """
        args = []
    else:
        query = """
            SELECT anum, COUNT(*) as cnt 
            FROM calls 
            WHERE anum = $1
            GROUP BY anum;
        """
        args = [filters.anum]

    async with pg_pool.acquire() as connection:
        rows = await connection.fetch(query, *args)

        if not rows:
            raise HTTPException(status_code=404, detail="No records found")

        # Преобразуем записи базы в формат ответа API
        return [{"anum": row["anum"], "cnt": row["cnt"]} for row in rows]

# Эндпоинт 3: Проверка здоровья (Health Check)
@router.post("/health")
async def health_check():
    health_status = {"postgres": "unhealthy", "kafka": "unhealthy"}
    status_code = status.HTTP_200_OK

    # Проверка Postgres
    try:
        async with pg_pool.acquire() as connection:
            await connection.execute("SELECT 1;")
            health_status["postgres"] = "healthy"
    except Exception as e:
        print(f"POSTGRESS: {e}")
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    # Проверка Kafka
    try:
        # Проверяем доступность метаданных кластера
        await kafka_producer.client.fetch_all_metadata()
        health_status["kafka"] = "healthy"
    except Exception as e:
        print(f"KAFFKA: {e}")
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    if status_code != status.HTTP_200_OK:
        raise HTTPException(status_code=status_code, detail=health_status)
        
    return health_status
