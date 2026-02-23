import math
import random
from datetime import datetime, timezone
from typing import Dict, Any

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def generate_telemetry(
    gnb_id: str,
    region: str,
    t: int,
    scenario: str = "normal",
    seed: int = 42
) -> Dict[str, Any]:
    """
    t: step index (e.g. each second or each 5 seconds, your choice in producer)
    scenario: normal | upf_overload | amf_storm | ran_congestion | transport_loss
    """
    random.seed(seed + hash(gnb_id) % 10_000 + t)

    # Daily-ish sinusoid (fake seasonality)
    base = 50 + 20 * math.sin(2 * math.pi * (t % 3600) / 3600.0)  # 1-hour cycle for demo
    noise = random.gauss(0, 3)

    dl = base + noise
    ul = 0.35 * dl + random.gauss(0, 1.5)

    prb = _clip(40 + 0.6 * dl + random.gauss(0, 5), 0, 100)
    ho = _clip(2 + 0.03 * dl + random.gauss(0, 0.5), 0, 30)

    upf_lat = _clip(15 + 0.12 * prb + random.gauss(0, 3), 5, 200)
    rtt = _clip(20 + 0.25 * (upf_lat - 15) + random.gauss(0, 2), 5, 300)
    jitter = _clip(2 + 0.05 * (rtt - 20) + random.gauss(0, 0.5), 0.2, 50)
    loss = _clip(0.2 + 0.01 * (jitter) + random.gauss(0, 0.05), 0, 10)

    amf_cpu = _clip(20 + 0.15 * dl + random.gauss(0, 2), 0, 100)
    smf_cpu = _clip(18 + 0.12 * dl + random.gauss(0, 2), 0, 100)
    upf_cpu = _clip(25 + 0.20 * dl + random.gauss(0, 3), 0, 100)

    pdu_rate = _clip(80 + 0.8 * dl + random.gauss(0, 10), 0, 500)
    pdu_sr = _clip(99.0 - 0.02 * (prb - 60) - 0.05 * loss + random.gauss(0, 0.2), 70, 100)
    rrc_sr = _clip(99.3 - 0.015 * (prb - 60) + random.gauss(0, 0.15), 75, 100)

    # Inject scenario patterns
    if scenario == "upf_overload":
        upf_cpu = _clip(upf_cpu + 35, 0, 100)
        upf_lat = _clip(upf_lat + 60, 5, 500)
        rtt = _clip(rtt + 40, 5, 500)
        pdu_sr = _clip(pdu_sr - 2.5, 50, 100)

    elif scenario == "amf_storm":
        amf_cpu = _clip(amf_cpu + 45, 0, 100)
        pdu_rate = _clip(pdu_rate + 200, 0, 1000)
        pdu_sr = _clip(pdu_sr - 1.5, 50, 100)

    elif scenario == "ran_congestion":
        prb = _clip(prb + 30, 0, 100)
        dl = _clip(dl - 15, 0, 300)
        rrc_sr = _clip(rrc_sr - 2.0, 50, 100)
        ho = _clip(ho + 5, 0, 50)

    elif scenario == "transport_loss":
        loss = _clip(loss + 2.5, 0, 20)
        jitter = _clip(jitter + 10, 0.2, 100)
        rtt = _clip(rtt + 30, 5, 500)

    ts = datetime.now(timezone.utc).isoformat()

    return {
        "ts": ts,
        "gnb_id": gnb_id,
        "region": region,
        "scenario": scenario,

        "amf_cpu": float(amf_cpu),
        "smf_cpu": float(smf_cpu),
        "upf_cpu": float(upf_cpu),
        "upf_latency_ms": float(upf_lat),

        "pdu_setup_sr": float(pdu_sr),
        "pdu_setup_rate": float(pdu_rate),

        "dl_throughput_mbps": float(_clip(dl, 0, 300)),
        "ul_throughput_mbps": float(_clip(ul, 0, 200)),
        "prb_util": float(prb),
        "handover_rate": float(ho),
        "rrc_sr": float(rrc_sr),

        "rtt_ms": float(rtt),
        "jitter_ms": float(jitter),
        "pkt_loss_pct": float(loss),
    }
