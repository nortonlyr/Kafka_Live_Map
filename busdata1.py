from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import time

# Extract coordinate from Geojson)
input_file = open('bus1_nyc.json')
json_arr = json.load(input_file)
coords = json_arr['features'][0]['geometry']['coordinates']
# print(coords)

#uuid
def generate_uuid():
    return uuid.uuid4()

#Kafka Producer
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geodata_final']
producer = topic.get_sync_producer()


#Message
data = {}
data['busline'] = '00001'

def generate_checkpoint(coord):
    i = 0
    while i < len(coords):
        data['key'] = data['busline'] + '_' + str(generate_uuid())
        data['timestamp'] = str(datetime.utcnow())
        data['latitude'] = coords[i][1]
        data['longitude'] = coords[i][0]

        message = json.dumps(data)
        print(message)
        producer.produce(message.encode('ascii'))
        time.sleep(1)


        # if bus reaches last coordinate, start from beginning
        if i == len(coords)-1:
            i=0
        else:
            i += 1

generate_checkpoint(coords)
