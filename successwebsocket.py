import asyncio
import websockets
from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka consumer configuration
kafka_conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'group.id': 'websocket1-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(kafka_conf)
topic_name = 'MostafaHamdySuccessTopic' 
consumer.subscribe([topic_name])

connected_clients = set()

async def consume_kafka_and_send():
    while True:
        try:
            msg = consumer.poll(timeout=1.0)  

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            # Send the Kafka message to all connected WebSocket clients
            message = msg.value().decode('utf-8')
            print(f"Received from Kafka: {message}")

            if connected_clients:  
                await asyncio.gather(*[client.send(message) for client in connected_clients])

        except Exception as e:
            print(f"Error in Kafka consumer: {e}")

# WebSocket handler for new clients
async def websocket_handler(websocket, path):
    # Register client
    connected_clients.add(websocket)
    try:
        # Keep the connection open and wait for any message from the client
        async for message in websocket:
            print(f"Received from WebSocket client: {message}")
            # You can handle any client message here if needed
    except websockets.exceptions.ConnectionClosed:
        print("Client disconnected")
    finally:
        # Unregister client
        connected_clients.remove(websocket)

# Main function to start the WebSocket server and Kafka consumer loop
async def main():
    # Run WebSocket server
    server = await websockets.serve(websocket_handler, "localhost", 8766)

    # Run the Kafka consumer in parallel
    kafka_task = asyncio.create_task(consume_kafka_and_send())

    # Keep the server running indefinitely
    await asyncio.gather(server.wait_closed(), kafka_task)

# Run the asyncio event loop
asyncio.run(main())
