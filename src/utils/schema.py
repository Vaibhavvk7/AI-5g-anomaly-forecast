from typing import Dict, Any, List

NUMERIC_FIELDS: List[str] = [
    "amf_cpu", "smf_cpu", "upf_cpu",
    "upf_latency_ms",
    "pdu_setup_sr", "pdu_setup_rate",
    "dl_throughput_mbps", "ul_throughput_mbps",
    "prb_util", "handover_rate", "rrc_sr",
    "rtt_ms", "jitter_ms", "pkt_loss_pct"
]

ID_FIELDS: List[str] = ["ts", "gnb_id", "region", "scenario"]

def validate_event(evt: Dict[str, Any]) -> None:
    for k in ID_FIELDS:
        if k not in evt:
            raise ValueError(f"Missing required field: {k}")
    for k in NUMERIC_FIELDS:
        if k not in evt:
            raise ValueError(f"Missing numeric field: {k}")
        if not isinstance(evt[k], (int, float)):
            raise ValueError(f"Field {k} must be numeric")
