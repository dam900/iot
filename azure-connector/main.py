import asyncio
import os
from dotenv import load_dotenv
from azure.iot.device.aio import IoTHubDeviceClient

async def main():
    print("Hello from azure-connector!")
    load_dotenv()
    azure_connection_string = os.getenv("IOTHUB_DEVICE_CONNECTION_STRING")
    device_client = IoTHubDeviceClient.create_from_connection_string(azure_connection_string)

    # Connect the device client.
    await device_client.connect()

    # Send a single message
    print("Sending message...")
    await device_client.send_message("This is a message that is being sent")
    print("Message successfully sent!")

    # finally, shut down the client
    await device_client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
