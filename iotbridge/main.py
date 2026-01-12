import aiomqtt
import asyncio
import sys
import enum
import logging

logging.basicConfig(level=logging.DEBUG)

def do_process(topic: str) -> bool:
    unwanted_topics = ["azure","bluetooth"]
    if topic.split("/")[0] in unwanted_topics:
        return False
    return True

async def receive_mqtt():
    logging.info("Attempting to connect to MQTT broker at localhost:1883...")
    try:
        async with aiomqtt.Client("localhost", port=1883) as client:
            logging.info("✅ Connected successfully!")
            await client.subscribe("#")
            logging.info("Subscribed to all topics. Listening for messages...")
            async for message in client.messages:
                logging.info(f"📩 Message received on topic '{message.topic}': {message.payload.decode()}")
                if (do_process(str(message.topic))):
                    await client.publish("azure/" + str(message.topic), message.payload, qos=1)

    except aiomqtt.MqttError as e:
        logging.error(f"❌ MQTT Error: {e}")
    except Exception as e:
        logging.error(f"❌ General Error: {e}")

async def main():   
    await receive_mqtt()

if __name__ == "__main__":
    if sys.platform.lower() == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
