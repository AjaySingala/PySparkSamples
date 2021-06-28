import json
import requests

from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_server = ["sandbox-hdp.hortonworks.com:6667"],
    value_serializer = (lambda x: json.dumps(x).encode('utf-8'))
)

for i in range(50):
    res = requests.get('api.open-notify.org/iss-now.json')
    data = json.loads(res.content.decode('utf-8'))
    print(data)

    producer.send('issTopic', value = data)
    sleep(5)
    producer.flush()
