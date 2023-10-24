import datetime
import json
import random
from time import sleep
import uuid
import requests
import socket
import os
import redis
from confluent_kafka import Producer
from pymongo import MongoClient
from bson import BSON

def sending_or_saving_data(record,producer,kafka_topic,collection):
    collection.insert_many([{'record':record.replace('\n','')}])
    return producer.produce(kafka_topic,key='taxi_data',value=record.replace('\n',''))

def main():
    if os.getenv('DATA_SOURCE')=='auto':
        r = redis.Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), username=os.getenv('REDIS_USER'),password=os.getenv('REDIS_PASS'), decode_responses=True)

        data_region_file = 'regions/'+os.getenv('REGION')+'_region.json'
        print('Region selected:', os.getenv('REGION'))
        region = os.getenv('REGION')
        
        taxi_raw_collection = MongoClient(r.get('MONGO_URI'))[r.get('MONGO_DB')][r.get('MONGO_RAW_COLLECTION')]
        taxi_collection = MongoClient(r.get('MONGO_URI'))[r.get('MONGO_DB')][r.get('MONGO_TAXI_COLLECTION')]

        kafka_broker = {'bootstrap.servers': r.get('KAFKA_BOOTSTRAP_SERVERS'),
                        'security.protocol':'SASL_SSL',
                        'sasl.mechanisms':'PLAIN',
                        'sasl.username':r.get('SASL_USERNAME'),
                        'sasl.password':r.get('SASL_PASSWORD'),
                        }

        kafka_topic = r.get('KAFKA_TOPIC')
        api_key = r.get('GOOGLE_MAPS_KEY')
        

        with open(data_region_file,encoding='utf-8') as f:
            region_coordinates = json.load(f)

        print(region_coordinates)

        max_lat = region_coordinates['northeast']['lat']
        max_lng = region_coordinates['northeast']['lng']

        min_lat = region_coordinates['southwest']['lat']
        min_lng = region_coordinates['southwest']['lng']
        
        fuel_consumption_per_m = region_coordinates["fuel_consumption_per_m"]
        full_fuel = region_coordinates["full_fuel"]
        fuel_cost = region_coordinates["fuel_cost"]
        trip_cost_per_m = region_coordinates["trip_cost_per_m"]

        available_fuel = full_fuel

        producer = Producer(kafka_broker)

        while True:
            origin=f"{round(random.uniform(min_lat,max_lat),8)}%2C{round(random.uniform(min_lng,max_lng),8)}"
            destination=f"{round(random.uniform(min_lat,max_lat),8)}%2C{round(random.uniform(min_lng,max_lng),8)}"

            response = requests.get(f"https://maps.googleapis.com/maps/api/directions/json?destination={destination}&origin={origin}&key={api_key}")
            
            # with open(f"data/trips_{socket.gethostname()}.jsonl",'+a',encoding='utf-8') as f:
            #     f.write(os.getenv('REGION')+','+origin+','+destination+','+str(response.json())+'\n')
            
            taxi_raw_collection.insert_many([{"taxi_id":socket.gethostname(),"region":os.getenv('REGION'),"origin":origin,"destination":destination,"record":str(response.json())}])

            trip_id = uuid.uuid5(uuid.NAMESPACE_URL,datetime.datetime.now().__str__())

            print(f'New trip {trip_id}')

            try:
                sending_or_saving_data(f'START [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [{response.json()["routes"][0]["legs"][0]["start_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["start_location"]["lng"]}] to [{response.json()["routes"][0]["legs"][0]["end_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["end_location"]["lng"]}]\n',producer,kafka_topic,taxi_collection)
                print(f'START [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [{response.json()["routes"][0]["legs"][0]["start_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["start_location"]["lng"]}] to [{response.json()["routes"][0]["legs"][0]["end_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["end_location"]["lng"]}]')
                for s,i in enumerate(response.json()['routes'][0]['legs'][0]['steps']):
                    if available_fuel < i["distance"]["value"] * fuel_consumption_per_m:
                        sending_or_saving_data(f'FUEL [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [COST {round((full_fuel - available_fuel)*fuel_cost)}]\n',producer,kafka_topic,taxi_collection)
                        print(f'FUEL [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [COST {round((full_fuel - available_fuel)*fuel_cost)}]')
                        available_fuel = full_fuel
                    sleep(round(i['duration']['value']/60))
                    if s==len(response.json()['routes'][0]['legs'][0]['steps'])-1:
                        sending_or_saving_data(f'END [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters] [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds] [TOTAL_DISTANCE {response.json()["routes"][0]["legs"][0]["distance"]["value"]} meters] [COST {response.json()["routes"][0]["legs"][0]["distance"]["value"]*trip_cost_per_m}]\n',producer,kafka_topic,taxi_collection)
                        print(f'END [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters] [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds] [TOTAL_DISTANCE {response.json()["routes"][0]["legs"][0]["distance"]["value"]} meters] [COST {response.json()["routes"][0]["legs"][0]["distance"]["value"]*trip_cost_per_m}]')
                    else:
                        sending_or_saving_data(f'WAYPOINT [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters]  [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds]\n',producer,kafka_topic,taxi_collection)
                        print(f'WAYPOINT [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters]  [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds]')
                    available_fuel -= i["distance"]["value"] * fuel_consumption_per_m
            except Exception as e:
                print(e)

            sleep(random.uniform(20,40))
    elif os.getenv('DATA_SOURCE')=='db':
        lines = []
        with open(f"{os.getenv('REGION')}.jsonl") as f:
            for i in f:
                lines.append(i)
        
        while True:

            line_number = random.randint(0,len(lines)-1)
            line = lines[line_number]

            region,origin,destination,response = line.split(',')

            trip_id = uuid.uuid5(uuid.NAMESPACE_URL,datetime.datetime.now().__str__())

            print(f'New trip {trip_id}')

            try:
                sending_or_saving_data(f'START [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [{origin.split("%2C")[0]},{origin.split("%2C")[1]}] to [{destination.split("%2C")[0]},{destination.split("%2C")[1]}]\n',producer,kafka_topic,taxi_collection)
                print(f'START [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [{origin.split("%2C")[0]},{origin.split("%2C")[1]}] to [{destination.split("%2C")[0]},{destination.split("%2C")[1]}]')
                for s,i in enumerate(response['routes'][0]['legs'][0]['steps']):
                    if available_fuel < i["distance"]["value"] * fuel_consumption_per_m:
                        sending_or_saving_data(f'FUEL [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [COST {round((full_fuel - available_fuel)*fuel_cost)}]\n',producer,kafka_topic,taxi_collection)
                        print(f'FUEL [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [COST {round((full_fuel - available_fuel)*fuel_cost)}]')
                        available_fuel = full_fuel
                    sleep(round(i['duration']['value']/60))
                    if s==len(response['routes'][0]['legs'][0]['steps'])-1:
                        sending_or_saving_data(f'END [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters] [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds] [TOTAL_DISTANCE {response["routes"][0]["legs"][0]["distance"]["value"]} meters] [COST {response["routes"][0]["legs"][0]["distance"]["value"]*trip_cost_per_m}]\n',producer,kafka_topic,taxi_collection)
                        print(f'END [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters] [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds] [TOTAL_DISTANCE {response["routes"][0]["legs"][0]["distance"]["value"]} meters] [COST {response["routes"][0]["legs"][0]["distance"]["value"]*trip_cost_per_m}]')
                    else:
                        sending_or_saving_data(f'WAYPOINT [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters]  [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds]\n',producer,kafka_topic,taxi_collection)
                        print(f'WAYPOINT [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters]  [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds]')
                    available_fuel -= i["distance"]["value"] * fuel_consumption_per_m
            except Exception as e:
                print(e)
            # sleep(random.uniform(20,40))


if __name__=="__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(e)
