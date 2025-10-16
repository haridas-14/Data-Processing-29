# cdc_consumer.py
# simple consumer that reads change events and applies incremental updates to an in-memory structure or model
import json
from kafka import KafkaConsumer
from collections import Counter
import joblib
import numpy as np
from sklearn.cluster import MiniBatchKMeans

BROKER = "localhost:9092"
TOPIC = "dbserver1.inventory.customers"  # depends on connector 'database.server.name' + db + table

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=True,
    group_id='cdc-processor'
)

MODEL_PATH = "kmeans_mini_batch.joblib"

# load or init clustering model
if __name__ == "__main__":
    if not hasattr(__builtins__, 'kmeans'):
        try:
            kmeans = joblib.load(MODEL_PATH)
        except:
            kmeans = MiniBatchKMeans(n_clusters=5, random_state=42, batch_size=20)
            # seed with dummy partial_fit
            kmeans.partial_fit(np.zeros((1,2)))
    counts = Counter()
    for msg in consumer:
        payload = msg.value
        # Debezium envelopes contain 'payload' with operation 'op' and 'after'/'before'
        p = payload.get("payload", {})
        op = p.get("op")
        after = p.get("after")
        before = p.get("before")
        if op == "c":  # create/insert
            counts["inserts"] += 1
            # example: extract numeric features and update model
            if after:
                # assume after contains fields 'feature1','feature2'
                f1 = float(after.get("feature1", 0) or 0)
                f2 = float(after.get("feature2", 0) or 0)
                X = np.array([[f1, f2]])
                kmeans.partial_fit(X)
                joblib.dump(kmeans, MODEL_PATH)
        elif op == "u":
            counts["updates"] += 1
        elif op == "d":
            counts["deletes"] += 1
        print("processed op:", op, "counts:", dict(counts))
