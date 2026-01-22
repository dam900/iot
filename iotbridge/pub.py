import json
import aiomqtt
import asyncio
import sys

import logging

logging.basicConfig(level=logging.DEBUG)

msg = {
#   "sensor_id": "WS-PL-042-XT",
#   "timestamp": "2026-01-15T20:22:15.482Z",
#   "status": "online",
  "readings": {
    "temperature": {
      "value": 62.5,
      "unit": "°C",
      "threshold_exceeded": False
    },
    # "vibration_frequency": {
    #   "value": 142.8,
    #   "unit": "Hz"
    # },
    # "power_consumption": {
    #   "current": 4.2,
    #   "voltage": 230.1,
    #   "unit": "kW"
    # },
    "operating_hours": 1240.5
  },
#   "location": {
#     "factory_hall": "H3",
#     "section": "Assembly-Line-A"
#   },
#   "metadata": {
#     "firmware_version": "2.4.1-stable",
#     "last_calibration": "2025-11-20"
#   }
}

async def main():   
    logging.info("Attempting to connect to MQTT broker at localhost:1883...")

    try:
        async with aiomqtt.Client("localhost", port=1883) as client:
            logging.info("✅ Connected successfully!")
            await client.publish("elo", json.dumps(msg), qos=1)
            logging.info("📤 Message published to 'elo'.")
            

    except aiomqtt.MqttError as e:
        logging.error(f"❌ MQTT Error: {e}")
    except Exception as e:
        logging.error(f"❌ General Error: {e}")

if __name__ == "__main__":
    if sys.platform.lower() == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())