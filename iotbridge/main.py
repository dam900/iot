import aiomqtt
import asyncio
import sys

async def main():
    print("Attempting to connect to MQTT broker at localhost:1883...")

    try:
        # We specify the hostname (localhost) and port (1883)
        async with aiomqtt.Client("localhost", port=1883) as client:
            print("✅ Connected successfully!")
            

    except aiomqtt.MqttError as e:
        print(f"❌ MQTT Error: {e}")
    except Exception as e:
        print(f"❌ General Error: {e}")

if __name__ == "__main__":
    # Windows specific fix for asyncio policies if needed
    if sys.platform.lower() == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
