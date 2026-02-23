import json
import time
import yaml
from confluent_kafka import Producer
from src.generate.telemetry_generator import generate_telemetry
from src.utils.schema import validate_event

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")

def main():
    cfg = yaml.safe_load(open("configs/kafka.yaml"))

    producer = Producer({"bootstrap.servers": cfg["bootstrap_servers"]})

    gnb_ids = [f"gnb_{i:03d}" for i in range(1, 6)]
    regions = ["us-east", "us-west"]

    t = 0
    print("Producing telemetry to Kafka topic:", cfg["topic_telemetry"])
    scenarios = ["normal", "upf_overload", "amf_storm", "ran_congestion", "transport_loss"]

    while True:
        scenario = scenarios[(t // 60) % len(scenarios)]
        for gnb in gnb_ids:
            evt = generate_telemetry(
                gnb_id=gnb,
                region=regions[hash(gnb) % len(regions)],
                t=t,
                scenario=scenario
            )
            validate_event(evt)

            producer.produce(
                topic=cfg["topic_telemetry"],
                value=json.dumps(evt).encode("utf-8"),
                on_delivery=delivery_report
            )

        producer.poll(0)  # serve delivery callbacks
        producer.flush(1)
        t += 1
        time.sleep(1)

if __name__ == "__main__":
    main()
