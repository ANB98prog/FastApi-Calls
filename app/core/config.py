from pydantic_settings import BaseSettings

# Класс для получения переменных окружения
class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "calls"
    database_url: str = "postgresql://myuser:mypassword@localhost:5432/mydb"
    api_login: str
    api_password: str

settings = Settings()
