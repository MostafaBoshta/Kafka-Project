from confluent_kafka import Consumer, KafkaError, Producer
import server
import random
import event_trigger
from server import socketio
# import image_classification as im
producer  = Producer({'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094' , 'client.id': 'MostafaHamdy-1'})
topic = 'MostafaHamdy-1'
success_topic = 'MostafaHamdySuccessTopic'
error_topic = 'MostafaHamdyerrorTopic'
group = topic + '-group'
consumer = Consumer({
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'enable.auto.commit': True,    
    'group.id': group,
    'auto.offset.reset': 'earliest'
})

def detect_object(id):
    # classifier = im.classify()
    # obj = classifier.classify_image('images/' + id + '.jpg')
    # print(obj)
    # return obj
    return random.choice(['cat', 'dog', 'car', 'person', 'tree'])

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
            import requests 
            msg = message.value().decode('utf-8')
            print(msg)
            requests.put('http://127.0.0.1:5000/object/' + msg, json={"object": detect_object(msg)})
            socketio.emit('refresh', {})
            producer.produce(success_topic, key='success', value=f'Success Classification {msg}')
            producer.flush()
except:
    consumer.close()    