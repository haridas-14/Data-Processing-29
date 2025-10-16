# kafka_producer.py
# sends fake sensor JSON messages to topic 'sensor-data'
import json, time, random
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "sensor-data"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

def gen_message(id):
    return {
        "id": id,
        "ts": int(time.time() * 1000),
        "temp": round(random.uniform(20, 40), 2),
        "humidity": round(random.uniform(10, 90), 2),
        "status": random.choice(["OK", "WARN", "FAIL"])
    }

if __name__ == "__main__":
    msg_id = 1
    try:
        while True:
            m = gen_message(msg_id)
            producer.send(TOPIC, m)
            print("sent:", m)
            msg_id += 1
            time.sleep(0.5)  # adjust rate
    except KeyboardInterrupt:
        producer.flush()
        print("stopped")
