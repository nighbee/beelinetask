import json 
import time 
import uuid 
import random 
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker


fake= Faker()

KAFKA_TOPIC= "telecom_events" 
KAFKA_SERVER= 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER, 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
) 

def generate_event():

    event_type = random.choices(
        ['data_session', 'call', 'sms', 'balance_recharge'],
        weights=[0.60, 0.25, 0.10, 0.05],
        k=1
    )[0]

    event = { 
        "event_id": str(uuid.uuid4()), 
        "msisdn": fake.msisdn(),
        "region": random.choice(['Almaty', 'Astana', 'Pavlodar', 'Taraz']), 
        "cell_tower_id": random.randint(1000,9999), 
        "timestamp": datetime.utcnow().isoformat()+"Z", 
        "event_type": event_type, 
    }

    if event_type  == 'call' or event_type == 'sms':
        event['event_subtype']= random.choice(['incoming', 'outgoing'])
        if event_type== 'call': 
            event['duration_seconds']= random.randint(5, 599)
    else: 
        event['event_subtype']= None 
    

    if event_type == 'data_session': 
        event['uration_seconds'] = random.randint(60, 3600)
        event['data_mb'] = round(random.uniform(5.0,500.0), 2)
    

    if event_type == 'balance_recharge': 
        event['amount'] = round(random.uniform(100.0, 1000.0), 2)   
    
    return event


def main(): 


    # cycle of data generation and sending to kafka

    print("staring session")
    while True: 
        try: 
            telecom_event = generate_event()

            producer.send(KAFKA_TOPIC, telecom_event)
            print(f"event sent: {telecom_event}")
            time.sleep(random.uniform(0.1, 1.0))

        except Exception as e:
            print (f"error occured: {e}")
            time.sleep(5)

if __name__ == "__main__": 
    main()