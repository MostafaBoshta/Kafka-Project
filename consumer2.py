from confluent_kafka import Consumer, KafkaError, Producer
import requests 
from PIL import Image 
import os
import random
import event_trigger
from server import socketio
topic = 'MostafaHamdy-1'
group = topic + '-group1'
consumer = Consumer({
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit': True,    
    'group.id': group,
    'auto.offset.reset': 'earliest'
})

producer  = Producer({'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094' , 'client.id': 'MostafaHamdy-1'})
success_topic = 'MostafaHamdySuccessTopic'
error_topic = 'MostafaHamdyerrorTopic'

def black_and_white(id):
    image_path = os.path.join('images', f'{id}.jpg')
    image = Image.open(image_path)
    bw = image.convert('L')
    bw.save(image_path)

consumer.subscribe([topic])

try:
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            producer.produce(error_topic, key='error', value=f'{message.error()}')
            producer.flush()
        else:

            msg = message.value().decode('utf-8')
            black_white = black_and_white(msg)
            print(msg)
            requests.put('http://127.0.0.1:5000/object/' + msg)
            socketio.emit('refresh', {})
            producer.produce(success_topic, key='success', value=f'Success BW {msg}')
            producer.flush()
except:
    print('Unsupported image format')
    consumer.close()

