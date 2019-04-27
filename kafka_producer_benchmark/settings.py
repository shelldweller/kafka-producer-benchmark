import os

KAFKA_CONFIG = {
    'bootstrap.servers': os.environ.get('KAFKA_SERVER', 'localhost:9092')
}
