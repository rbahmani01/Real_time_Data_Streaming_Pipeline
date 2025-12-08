import json
import random
import time
import math
from datetime import datetime, timezone
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError

# High-throughput config
NUM_HOUSES = 10_000
TOPIC = "sensor_raw"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=(3, 4, 0),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_house_event(house_id: int) -> dict:
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()

    # --- Time-of-day as float hours, 0–24 ---
    h = now.hour + now.minute / 60 + now.second / 3600

    # --- Per-house deterministic RNG (stable over time) ---
    house_rng = random.Random(house_id)

    # ------------------------------------------------------------------
    # 1) PV panel size and production (PV_panel_kw)
    # ------------------------------------------------------------------
    # Each house has either 6 kW or 8 kW of PV
    pv_size_kw = 6.0 if house_rng.random() < 0.5 else 8.0
    SYSTEM_EFF = 0.80  # ~80% of nameplate capacity

    # Simple solar production curve:
    # - 0 at night
    # - start ~07:00, end ~19:00
    # - max at solar noon ~13:00
    if 7 <= h <= 19:
        # map [7,19] -> [0,1]
        x = (h - 7.0) / (19.0 - 7.0)
        # bell-ish curve: 0 at sunrise/sunset, 1 at noon
        pv_profile = math.sin(math.pi * x)
        pv_profile = max(0.0, pv_profile)
        pv_panel_kw = pv_size_kw * pv_profile * SYSTEM_EFF

        # small variability per event
        pv_panel_kw *= (1.0 + random.uniform(-0.05, 0.05))
    else:
        pv_panel_kw = 0.0

    # ------------------------------------------------------------------
    # 2) EV charging profile (EV_charging_kw)
    # ------------------------------------------------------------------
    # Each house charges once per day, 1–2 hours, continuous.
    # We pick a deterministic start time between 17:00 and 21:00.
    ev_start_hour = house_rng.uniform(17.0, 21.0)  # early evening arrival
    ev_duration_h = house_rng.uniform(1.0, 2.0)    # 1–2 hours

    # compute time difference from start, taking wraparound into account
    t_since_start = h - ev_start_hour
    if 0 <= t_since_start < ev_duration_h:
        ev_charging_kw = 7.4  # all chargers are 7.4 kW
    else:
        ev_charging_kw = 0.0

    # ------------------------------------------------------------------
    # 3) Base household load (without EV, without PV) -> load profile
    # ------------------------------------------------------------------
    # This is "normal" house consumption pattern (kW):
    # - low at night
    # - morning peak around 7–9
    # - small midday bump around lunch
    # - big evening peak around 18–22
    base_rng_scale = house_rng.uniform(0.8, 1.2)  # some houses use more

    # baseline all-day consumption
    base_load = 0.25

    # morning peak around 7.5h
    morning_peak = math.exp(-0.5 * ((h - 7.5) / 1.0) ** 2)  
    # midday bump around 13h
    midday_peak = math.exp(-0.5 * ((h - 13.0) / 1.5) ** 2)
    # evening peak around 19.5h
    evening_peak = math.exp(-0.5 * ((h - 19.5) / 1.5) ** 2)

    house_load_kw = (
        base_load
        + 1.0 * morning_peak
        + 0.4 * midday_peak
        + 1.6 * evening_peak
    )

    # scale per house + some short-term noise
    house_load_kw *= base_rng_scale
    house_load_kw *= (1.0 + random.uniform(-0.08, 0.08))  # ±8%

    # ------------------------------------------------------------------
    # 4) Net power from grid (power_kw)
    # ------------------------------------------------------------------
    # Think of this as the power measured at the grid connection:
    #   grid = base_load + EV - PV
    power_kw = house_load_kw + ev_charging_kw - pv_panel_kw

    # ------------------------------------------------------------------
    # 5) Voltage (stay as before, small noise)
    # ------------------------------------------------------------------
    voltage = 230 + random.uniform(-3, 3)

    return {
        "house_id": house_id,
        "timestamp": now_iso,
        "power_kw": round(power_kw, 3),
        "ev_charging_kw": round(ev_charging_kw, 3),
        "pv_panel_kw": round(pv_panel_kw, 3),
        "voltage": round(voltage, 1),
    }


def main():
    producer = create_producer()
    house_ids = list(range(1, NUM_HOUSES + 1))

    print(f"Starting HIGH-THROUGHPUT producer for {NUM_HOUSES} houses...")
    print("Best effort ~10k msg/sec. Ctrl+C to stop.\n")

    batch_idx = 0
    try:
        while True:
            batch_idx += 1
            start = time.time()

            for house_id in house_ids:
                event = generate_house_event(house_id)
                producer.send(TOPIC, event)

            producer.flush()

            elapsed = time.time() - start
            msgs = NUM_HOUSES
            rate = msgs / elapsed if elapsed > 0 else float("inf")

            sleep_time = max(0.0, 1.0 - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            print(
                f"[batch {batch_idx}] sent {msgs} messages in {elapsed:.3f}s "
                f"(~{rate:,.0f} msg/s), sleep={sleep_time:.3f}s"
            )

    except KeyboardInterrupt:
        print("\nKeyboardInterrupt received. Flushing and closing producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed. Goodbye.")


if __name__ == "__main__":
    main()
