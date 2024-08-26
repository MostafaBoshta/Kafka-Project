from confluent_kafka import Producer
import server
producer  = Producer({'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',})

producer.produce('MostafaHamdy-1', key='key1', value=server.upload_file[id])
producer.flush()