import asyncio
import logging
import os
import sys
import json
from typing import Any
import aiomqtt
from dotenv import load_dotenv
from azure.iot.device.aio import IoTHubDeviceClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def azure_to_mqtt_bridge(device_client, mqtt_client):
    """Listens for messages from Azure and forwards them to MQTT."""
    logging.info("Listening for C2D messages from Azure...")
    try:
        while True:
            # Receive message from Azure
            message = await device_client.receive_message()
            payload_str = message.data.decode("utf-8")
            logging.info(f"☁️ Azure -> Bridge: {payload_str}")

            try:
                data: dict[str, Any] = json.loads(payload_str)
                # Expecting JSON like: {"topic": "sensors/livingroom", "msg": "hello"}
                target_topic = data.get("topic")
                msg_payload = data.get("data", payload_str)

                to_send = {"data": msg_payload}

                if target_topic:
                    await mqtt_client.publish(target_topic, payload=json.dumps(to_send), qos=1)
                    logging.info(f"   ➡️ Forwarded to MQTT topic: {target_topic}")
                else:
                    logging.warning("   ⚠️ Azure message missing 'topic' field.")
            except json.JSONDecodeError:
                logging.error("   ❌ Failed to decode Azure message as JSON.")

    except Exception as e:
        logging.error(f"Error in Azure-to-MQTT bridge: {e}")

async def mqtt_to_azure_bridge(device_client, mqtt_client):
    """Listens for MQTT messages on 'azure/#' and forwards them to Azure."""
    logging.info("Subscribing to MQTT topic 'azure/#'...")
    await mqtt_client.subscribe("azure/#")
    
    async for message in mqtt_client.messages:
        topic_name = str(message.topic)
        payload_decode = message.payload.decode()
        logging.info(f"🏠 MQTT -> Bridge: {topic_name}")

        clean_topic = topic_name.replace("azure/", "", 1)

        azure_payload = {
            "topic": clean_topic,
            "data": payload_decode
        }

        await device_client.send_message(json.dumps(azure_payload))
        logging.info(f"   ➡️ Forwarded to Azure IoT Hub")

async def main():
    load_dotenv()
    conn_str = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
    
    if not conn_str:
        logging.error("No Azure connection string found in .env file.")
        return

    # Initialize Clients
    device_client = IoTHubDeviceClient.create_from_connection_string(conn_str)
    
    try:
        await device_client.connect()
        logging.info("✅ Connected to Azure IoT Hub")

        async with aiomqtt.Client("localhost", port=1883) as mqtt_client:
            logging.info("✅ Connected to MQTT Broker")
            await asyncio.gather(
                azure_to_mqtt_bridge(device_client, mqtt_client),
                mqtt_to_azure_bridge(device_client, mqtt_client)
            )

    except Exception as e:
        logging.error(f"Main Loop Error: {e}")
    finally:
        await device_client.shutdown()

if __name__ == "__main__":
    if sys.platform.lower() == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBridge stopped by user.")