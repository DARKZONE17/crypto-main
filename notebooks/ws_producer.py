import asyncio
import json, time
import websockets
import random 
from websockets.exceptions import ConnectionClosedError
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pydantic import BaseModel, ValidationError
import logging 
import os

KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_TOPIC_DLQ = os.environ["KAFKA_TOPIC_DLQ"]

logging.basicConfig( 
    level=logging.INFO, 
    format="%(asctime)s | %(levelname)s | %(message)s" 
    )

logger = logging.getLogger(__name__)

class TradeEvent(BaseModel):
    stream: str
    coin: str
    price: float
    volume: float
    event_time: int


# Create Topic DLQ to store error and logs
def create_topic():
    admin_client = AdminClient({
        'bootstrap.servers' : 'localhost:9092'
    })
    topic = NewTopic(
        topic = KAFKA_TOPIC_DLQ,
        num_partitions=5,
        replication_factor=1
    )
    fs = admin_client.create_topics([topic])

    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"Topic: {topic} Created")
        except Exception as e:
            if "already exists" not in str(e):
                logger.error(f"Error creating Topic: {topic} {e}")
            else:
                logger.info(f"Topic Already Exists: {topic} {e}")

producer = Producer({
    'bootstrap.servers' : 'localhost:9092',
    'enable.idempotence' : True,
    'linger.ms' : 5,
    'batch.size' : 16384,
    'compression.type': 'snappy',
    'acks': 'all', 
})

def delivery_report(err, msg):
    global success_count
    if err is not None:
        logger.error(f"Delivery failed: {err}")
    else:
        success_count += 1

def send_to_dlq(message,e,key,retries):
    global dlq_count
    error_event = build_error_event(message,e,retries)
    
    producer.produce(
        topic = KAFKA_TOPIC_DLQ,
        key = key.encode('utf-8'),
        value = json.dumps(error_event).encode('utf-8'),
        callback= delivery_report
    )
    
    dlq_count += 1
    producer.poll(1)

def build_error_event(original_message,error_msg, retries):
    return{
        "event_type" : "DLQ ERROR",
        "original_message" : original_message,
        "error" : {
            "message" : str(error_msg),
            "type" : type(error_msg).__name__
        },
        "event_time" :  time.strftime("%Y-%m-%d %H:%M:%S"),
        "retries" : retries
    }

success_count = 0
dlq_count = 0

async def connect_and_stream():
    global success_count , dlq_count
    attempts = 0
    url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade/xrpusdt@trade/adausdt@trade"
    logger.info("Connecting...")
    create_topic()

    while attempts < 10:
        try:
            async with websockets.connect(                                         # Open Connecting with WebSocet -> (Binance)
                url ,                                                              # URL To Binance
                ping_interval=20,                                                  # send ping every 20 sec (Heartbeat) to check connection is still alive
                ping_timeout=10                                                    # wait 10 sec for pong (Heartbeat)
                ) as ws:                                         
                logger.info("Connection Opened")
                attempts = 0                                                       # If connection got succesfull then attempt set to 0 
                max_retries = 5
            
                async for message in ws:                                           # Wait for message
                    retries = 0 
                    if (success_count + dlq_count) % 100 == 0:
                        logger.info(f"Metrics → Success: {success_count}, DLQ: {dlq_count}")    

                    while retries < max_retries:
                        try:
                            data_from_websocket = json.loads(message)
                            stream = data_from_websocket['stream']
                            data = data_from_websocket['data']

                            event = TradeEvent(
                                stream=stream,
                                coin = data['s'],
                                price = data['p'],
                                volume = data['q'],
                                event_time = data['T'],
                            )

                            coin = event.coin
                            price = event.price
                            volume = event.volume
                            event_time = event.event_time

                            logger.info(json.dumps({
                                "coin" : coin,
                                "Price": price,
                                "Volume" : volume,
                                "event_time" : event_time
                                }
                            ))

                            payload = json.dumps({
                                "stream" : stream,
                                "coin" : coin,
                                "price" : price,
                                "volume" : volume,
                                "event_time" : event_time,
                            }).encode('utf-8')

                            # Sending the Data to Kafka Topic
                            producer.produce(
                                topic = KAFKA_TOPIC,
                                key = coin.encode(),
                                value = payload,
                                callback = delivery_report
                            )
                            producer.poll(0)
                            break;
                        
                        except ValidationError as e:
                            retries += 1
                            if retries >= max_retries:
                                logger.error(f"Validation error: {e}")
                                send_to_dlq(message,e,"validation_error",retries)
                                break;

                        except json.JSONDecodeError as e:
                            retries += 1
                            if retries >= max_retries:
                                logger.error(f"JSON error: {e}")
                                send_to_dlq(message,e,"json_error",retries)
                                break;
                        
                        except BufferError as e:    # When the input comes and fill the batch before it is able to send to kafka we slow the input by 1 sec
                            producer.poll(1)
                            retries += 1
                            if retries >= max_retries:
                                send_to_dlq(message,e,"buffer_error",retries)
                                break;

                        except Exception as e:
                            retries += 1
                            if retries >= max_retries:
                                logger.error(f"General error: {e}")
                                send_to_dlq(message,e,"general_error",retries)
                                break;

            
        except ConnectionClosedError as e:                                               # if Connection failed to connect then this block of script will run
            attempts += 1
            logger.error(f"ConnectionClosedError : Attempt {attempts} failed : {e}")
            delay = min(2 ** attempts , 60)                                              # Maximum wait of 60 sec before calling it failed attempt
            jitter = random.uniform(0,1)
            logger.info("Reconnecting..")
            await asyncio.sleep(delay + jitter)                                          # Backoff Stratergy

        except Exception as p:
            attempts += 1
            logger.error(f"Attempt {attempts} failed : {p}")
            delay = min(2 ** attempts , 60)
            jitter = random.uniform(0,1)
            logger.info("Reconnecting..")
            await asyncio.sleep(delay + jitter)

    if attempts >= 10: 
        title = "🚨 Connection Failure Alert 🚨"
        message = f"WebSocket failed after {attempts} retries."

        os.system(f'''
        osascript -e 'display notification "{message}" with title "{title}"'
        ''');
        send_to_dlq(message,Exception("WebSocket failure"),title,attempts)
        producer.flush()

try:
    asyncio.run(connect_and_stream())
finally:
    logger.info("Flushing producer: ")
    producer.flush()