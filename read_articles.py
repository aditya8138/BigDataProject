import os
import json
import time
from kafka import KafkaProducer


def connect_kafka_producer():
    _producer = None
    try:
         _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)

    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


if __name__== "__main__":

    while True:
        time.sleep(1) # every 5 seconds
    #/Users/aditya/news-please-repo/data/2019/05/03/
        dir = '/Users/aditya/news-please-repo/data/2019/05/07/vanguardia.com/'
        # for root, dirs, files in os.walk('articles'):
        data={}
        for root, dirs, files in os.walk(dir):
            # folder where all files get stored
            for file in files:
                with open(os.path.join(root, file), "r") as auto:
                    try:
                        data = json.loads(auto.read())
                        #print(data)
                        if (data != ""):
                            prod = connect_kafka_producer();
                            print(json.dumps(data))
                            publish_message(prod, 'cheetos', json.dumps(data))
                            time.sleep(1)
                            if prod is not None:
                                prod.close()
                        os.remove(os.path.join(root, file))
                    except ValueError as e:
                        continue

