import aiomqtt
import asyncio
import sys

import logging

logging.basicConfig(level=logging.DEBUG)


async def main():   
    logging.info("Attempting to connect to MQTT broker at localhost:1883...")

    try:
        async with aiomqtt.Client("localhost", port=1883) as client:
            logging.info("✅ Connected successfully!")
            await client.subscribe("*")
            logging.info("Subscribed to all topics. Listening for messages...")
            async for message in client.messages:
                logging.info(f"📩 Message received on topic '{message.topic}': {message.payload.decode()}")
            

    except aiomqtt.MqttError as e:
        logging.error(f"❌ MQTT Error: {e}")
    except Exception as e:
        logging.error(f"❌ General Error: {e}")

if __name__ == "__main__":
    if sys.platform.lower() == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
