"""
Traffic Data Simulator
- Sends records to Flask API (docker service name via API_BASE env var)
- Kafka is optional — simulator degrades gracefully without it
- Removed all broken pyarrow / hdfs imports
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE        = os.environ.get("API_BASE",        "http://localhost:5000/api")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

ROADS = [
    {"road_id": "RD001", "name": "Main Street",
     "coords": [(40.7500, -74.0050), (40.7600, -73.9950)]},
    {"road_id": "RD002", "name": "Broadway",
     "coords": [(40.7550, -74.0100), (40.7650, -74.0000)]},
    {"road_id": "RD003", "name": "5th Avenue",
     "coords": [(40.7450, -74.0050), (40.7550, -73.9950)]},
    {"road_id": "RD004", "name": "Park Avenue",
     "coords": [(40.7480, -74.0150), (40.7580, -74.0050)]},
    {"road_id": "RD005", "name": "Madison Avenue",
     "coords": [(40.7520, -74.0080), (40.7620, -73.9980)]},
]

VEHICLE_IDS = [f"V{i:04d}" for i in range(1, 201)]

# ---------------------------------------------------------------------------
# Kafka (optional)
# ---------------------------------------------------------------------------
def setup_kafka():
    try:
        from kafka import KafkaProducer  # kafka-python-ng
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=lambda v: json.dumps(v).encode(),
            request_timeout_ms=8000,
            retries=2,
        )
        # Quick liveness test
        future = producer.send("vehicle_gps", {"init": True})
        future.get(timeout=5)
        print("✅ Kafka connected:", KAFKA_BOOTSTRAP)
        return producer
    except Exception as exc:
        print(f"⚠️  Kafka unavailable ({exc}) — running without it.")
        return None


# ---------------------------------------------------------------------------
# Vehicle position state
# ---------------------------------------------------------------------------
_positions: dict[str, dict] = {}

def _initial_position() -> dict:
    road = random.choice(ROADS)
    t    = random.random()
    s, e = road["coords"]
    return {"road": road, "t": t,
            "lat": s[0] + (e[0]-s[0])*t,
            "lon": s[1] + (e[1]-s[1])*t}

def _move(vid: str) -> dict:
    pos  = _positions.setdefault(vid, _initial_position())
    road = pos["road"]
    s, e = road["coords"]
    pos["t"] = max(0.0, min(1.0, pos["t"] + random.uniform(-0.08, 0.08)))
    pos["lat"] = s[0] + (e[0]-s[0]) * pos["t"]
    pos["lon"] = s[1] + (e[1]-s[1]) * pos["t"]
    return pos

# ---------------------------------------------------------------------------
# Traffic generation
# ---------------------------------------------------------------------------
def generate_batch() -> list[dict]:
    hour = datetime.now().hour
    if 7 <= hour <= 9 or 16 <= hour <= 18:   # rush hour
        base, var, n = 18, 12, random.randint(15, 25)
    elif 0 <= hour <= 5:                       # night
        base, var, n = 65, 15, random.randint(3,  8)
    else:                                      # normal
        base, var, n = 45, 20, random.randint(8,  15)

    batch = []
    for _ in range(n):
        vid   = random.choice(VEHICLE_IDS)
        pos   = _move(vid)
        speed = round(max(5.0, base + random.uniform(-var, var)), 1)
        ts    = datetime.now(timezone.utc).isoformat()
        batch.append({
            "vehicle_id":       vid,
            "timestamp":        ts,
            "latitude":         round(pos["lat"], 6),
            "longitude":        round(pos["lon"], 6),
            "speed":            speed,
            "road_id":          pos["road"]["road_id"],
            "road_name":        pos["road"]["name"],
            "vehicle_type":     random.choice(["car","truck","bus","motorcycle"]),
            "congestion_level": "high" if speed<20 else "medium" if speed<40 else "low",
        })
    return batch

def detect_congestion(batch: list[dict]) -> list[dict]:
    by_road: dict[str, list] = {}
    for rec in batch:
        by_road.setdefault(rec["road_id"], []).append(rec)

    alerts = []
    for road_id, recs in by_road.items():
        avg  = sum(r["speed"] for r in recs) / len(recs)
        if avg < 25 and len(recs) > 5:
            alerts.append({
                "alert_id":    f"CONG_{int(time.time())}_{road_id}",
                "timestamp":   datetime.now(timezone.utc).isoformat(),
                "road_id":     road_id,
                "road_name":   recs[0]["road_name"],
                "severity":    "high" if avg < 15 else "medium",
                "avg_speed":   round(avg, 1),
                "vehicle_count": len(recs),
                "cause":       random.choice(["accident","construction","volume","weather"]),
                "resolved":    False,
            })
    return alerts

def post_to_api(endpoint: str, payload: dict) -> bool:
    try:
        r = requests.post(f"{API_BASE}/{endpoint}", json=payload, timeout=3)
        return r.ok
    except Exception:
        return False

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    print("🚦 Traffic Simulator starting …")
    print(f"   API  : {API_BASE}")
    print(f"   Kafka: {KAFKA_BOOTSTRAP}")

    # Wait for Flask to be ready (it might still be running DB migrations)
    for attempt in range(20):
        try:
            r = requests.get(f"{API_BASE}/health", timeout=3)
            if r.ok:
                print("✅ Flask API reachable.")
                break
        except Exception:
            pass
        print(f"   Waiting for API … ({attempt+1}/20)")
        time.sleep(5)

    producer = setup_kafka()
    batch_no = 0

    print("▶  Streaming started.\n" + "─" * 50)

    while True:
        try:
            batch = generate_batch()
            batch_no += 1

            # Push to Kafka (best-effort)
            if producer:
                for rec in batch:
                    try:
                        producer.send("vehicle_gps", rec)
                    except Exception:
                        producer = None   # stop trying if Kafka went away
                        break

            # Every 5 batches push one sample record to the Flask API
            if batch_no % 5 == 0:
                post_to_api("simulate-data", random.choice(batch))

            # Congestion detection every 3 batches
            if batch_no % 3 == 0:
                alerts = detect_congestion(batch)
                for alert in alerts:
                    if producer:
                        try:
                            producer.send("congestion_alerts", alert)
                        except Exception:
                            pass

            # Console status
            avg = sum(r["speed"] for r in batch) / len(batch)
            ts  = datetime.now().strftime("%H:%M:%S")
            print(f"[{ts}] batch={batch_no:04d}  vehicles={len(batch):02d}  "
                  f"avg_speed={avg:.1f} km/h  kafka={'on' if producer else 'off'}")

            time.sleep(3)

        except KeyboardInterrupt:
            print("\n🛑 Simulator stopped.")
            break
        except Exception as exc:
            print(f"❌ Error: {exc}")
            time.sleep(5)


if __name__ == "__main__":
    main()
