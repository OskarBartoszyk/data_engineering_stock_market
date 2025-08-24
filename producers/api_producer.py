import os
import time
import json
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9095')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'financial-data')
# prefer lowercase service name for docker-compose networking
API_URL = os.environ.get('API_URL', 'http://python-api:8000/all')
POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', '10'))
API_TIMEOUT = int(os.environ.get('API_TIMEOUT', '30'))


def create_producer(bootstrap_servers):
    # Using kafka-python
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5
    )


def create_session(retries=3, backoff_factor=1):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(['GET'])
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def fetch_api(session, url, timeout):
    try:
        start = time.time()
        resp = session.get(url, timeout=timeout)
        elapsed = time.time() - start
        resp.raise_for_status()
        logging.info('Fetched %s in %.2fs', url, elapsed)
        return resp.json()
    except requests.exceptions.ReadTimeout:
        logging.warning('Read timeout when requesting %s (timeout=%s)', url, timeout)
        return None
    except Exception:
        logging.exception('Failed to fetch API %s', url)
        return None


def main():
    logging.info('Starting API -> Kafka producer')
    producer = create_producer(KAFKA_BOOTSTRAP)
    session = create_session(retries=3, backoff_factor=1)

    # wait until API responds at least once (with limited attempts)
    attempts = 0
    while attempts < 6:
        payload = fetch_api(session, API_URL, API_TIMEOUT)
        if payload:
            logging.info('Initial API check succeeded')
            try:
                producer.send(KAFKA_TOPIC, payload)
                producer.flush()
                logging.info('Sent initial payload to topic %s (count=%s)', KAFKA_TOPIC, payload.get('total_count'))
            except Exception:
                logging.exception('Failed to send initial message to Kafka')
            break
        attempts += 1
        logging.info('API not ready yet, retrying in %s seconds (%s/6)', POLL_INTERVAL, attempts)
        time.sleep(POLL_INTERVAL)

    try:
        while True:
            payload = fetch_api(session, API_URL, API_TIMEOUT)
            if payload:
                # payload is expected to match APIResponse {data, total_count, timestamp}
                try:
                    producer.send(KAFKA_TOPIC, payload)
                    producer.flush()
                    logging.info('Sent payload to topic %s (count=%s)', KAFKA_TOPIC, payload.get('total_count'))
                except Exception:
                    logging.exception('Failed to send message to Kafka')
            else:
                logging.warning('No payload received from API')

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logging.info('Shutting down producer')
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
