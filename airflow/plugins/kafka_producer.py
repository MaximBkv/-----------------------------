import json
import logging
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class DataPlatformKafkaProducer:
    """Kafka producer для загрузки данных в топики"""
    
    def __init__(self, bootstrap_servers: List[str] = None):
        if bootstrap_servers is None:
            bootstrap_servers = ['kafka:29092']
        
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"Kafka producer initialized with servers: {bootstrap_servers}")
    
    def send_message(self, topic: str, message: Dict, key: str = None):
        """Отправка одного сообщения в топик"""
        try:
            future = self.producer.send(topic, key=key, value=message)
            record_metadata = future.get(timeout=10)
            logger.debug(
                f"Message sent to {record_metadata.topic} "
                f"partition {record_metadata.partition} "
                f"offset {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False
    
    def send_batch(self, topic: str, messages: List[Dict], key_field: str = None):
        """Отправка батча сообщений в топик"""
        success_count = 0
        fail_count = 0
        
        for message in messages:
            key = message.get(key_field) if key_field else None
            if self.send_message(topic, message, key):
                success_count += 1
            else:
                fail_count += 1
        
        self.producer.flush()
        logger.info(
            f"Batch send completed for {topic}: "
            f"{success_count} successful, {fail_count} failed"
        )
        return success_count, fail_count
    
    def close(self):
        """Закрытие producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


def load_jsonl_to_kafka(file_path: str, topic: str, key_field: str = None):
    """Загрузка данных из JSONL файла в Kafka топик"""
    producer = DataPlatformKafkaProducer()
    messages = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    message = json.loads(line.strip())
                    messages.append(message)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse line {line_num} in {file_path}: {e}")
        
        logger.info(f"Loaded {len(messages)} messages from {file_path}")
        success, failed = producer.send_batch(topic, messages, key_field)
        
        return {
            'file': file_path,
            'topic': topic,
            'total': len(messages),
            'success': success,
            'failed': failed
        }
    
    finally:
        producer.close()
