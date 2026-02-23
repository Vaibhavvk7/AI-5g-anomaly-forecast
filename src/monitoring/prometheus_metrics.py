from prometheus_client import Counter, Histogram, Gauge

EVENTS_CONSUMED = Counter("events_consumed_total", "Total telemetry events consumed")
ANOMALIES_FOUND = Counter("anomalies_found_total", "Total anomalies flagged")
INFER_LATENCY = Histogram("inference_latency_seconds", "Inference latency in seconds")
ANOMALY_RATE_GAUGE = Gauge("anomaly_rate_recent", "Recent anomaly rate (rolling)")
