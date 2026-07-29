import json
from typing import Optional, List
from fastapi import FastAPI, Depends, HTTPException, status
import asyncpg
from aiokafka import AIOKafkaProducer
from core.config import settings
from schemas import CallSchema, StatsRequestSchema, StatsResponseSchema
from core.security import authenticate


app = FastAPI(title="Kafka-Postgres API")

# Глобальные клиенты для пулов соединений
kafka_producer: Optional[AIOKafkaProducer] = None
pg_pool: Optional[asyncpg.Pool] = None

# Жизненный цикл приложения (Управление подключениями)
@app.on_event("startup")
async def startup_event():
    global kafka_producer, pg_pool
    # Инициализация Kafka Producer
    kafka_producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await kafka_producer.start()
    
    # Инициализация пула соединений Postgres
    pg_pool = await asyncpg.create_pool(settings.database_url)

@app.on_event("shutdown")
async def shutdown_event():
    global kafka_producer, pg_pool
    if kafka_producer:
        await kafka_producer.stop()
    if pg_pool:
        await pg_pool.close()

# Эндпоинт 1: Отправка call в Kafka
@app.post("/calls", status_code=status.HTTP_201_CREATED, dependencies=[Depends(authenticate)])
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
@app.post("/stats", response_model=List[StatsResponseSchema], dependencies=[Depends(authenticate)])
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
@app.post("/health")
async def health_check():
    health_status = {"postgres": "unhealthy", "kafka": "unhealthy"}
    status_code = status.HTTP_200_OK

    # Проверка Postgres
    try:
        async with pg_pool.acquire() as connection:
            await connection.execute("SELECT 1;")
            health_status["postgres"] = "healthy"
    except Exception as e:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    # Проверка Kafka
    try:
        # Проверяем доступность метаданных кластера
        await kafka_producer.client.fetch_all_metadata()
        health_status["kafka"] = "healthy"
    except Exception as e:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    if status_code != status.HTTP_200_OK:
        raise HTTPException(status_code=status_code, detail=health_status)
        
    return health_status
