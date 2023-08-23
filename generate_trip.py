import datetime
import json
import random
import sys
from time import sleep
import uuid
import requests
import socket
import os
from confluent_kafka import Producer

def sending_or_saving_data(t,record,producer,kafka_topic):
    if t=="file":
        f = open(f"{os.getenv('REGION')}_{socket.gethostname()}.txt", "+a")
        f.write(record)
    elif t=='kafka':
        return producer.produce(kafka_topic,key='taxi_data',value=record.replace('\n',''))

def main():

    data_region_file = 'regions/'+os.getenv('REGION')+'_region.json'
    print('Region selected:', os.getenv('REGION'))
    region = os.getenv('REGION')
    # kafka_broker = {'bootstrap.servers': os.getenv('KAFKA_SERVERS')}
    kafka_broker = {'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),#"pkc-921jm.us-east-2.aws.confluent.cloud:9092"
                    'security.protocol':'SASL_SSL',
                    'sasl.mechanisms':'PLAIN',
                    'sasl.username':os.getenv('SASL_USERNAME'),#'JAWKMHWCRWOC6LF5'
                    'sasl.password':os.getenv('SASL_PASSWORD'),#'rcOEehmt0FUXueMAoblh5XR908TMi6AT5bHtX7GycRLVXwLjfdXBpOSzByoiQ8gm'
                    }
    
    kafka_topic = os.getenv('KAFKA_TOPIC')#"taxi_data"
    api_key = os.getenv('GOOGLE_MAPS_KEY')#'AIzaSyDaEPpoPEi3BmsA3K2sHBkEiC1e4ILPqpI'
    mode  = os.getenv('MODE')
    # print(kafka_broker)

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
        with open(f"trips_{socket.gethostname()}.jsonl",'+a',encoding='utf-8') as f:
            f.write(str(response.json())+'\n')

        trip_id = uuid.uuid5(uuid.NAMESPACE_URL,datetime.datetime.now().__str__())
        
        print(f'New trip {trip_id}')
        
        try:
            
            sending_or_saving_data(mode,f'START [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [{response.json()["routes"][0]["legs"][0]["start_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["start_location"]["lng"]}] to [{response.json()["routes"][0]["legs"][0]["end_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["end_location"]["lng"]}]\n',producer,kafka_topic)
            print(f'START [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [{response.json()["routes"][0]["legs"][0]["start_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["start_location"]["lng"]}] to [{response.json()["routes"][0]["legs"][0]["end_location"]["lat"]},{response.json()["routes"][0]["legs"][0]["end_location"]["lng"]}]')
            for s,i in enumerate(response.json()['routes'][0]['legs'][0]['steps']):
                if available_fuel < i["distance"]["value"] * fuel_consumption_per_m:
                    sending_or_saving_data(mode,f'FUEL [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [COST {round((full_fuel - available_fuel)*fuel_cost)}]\n',producer,kafka_topic)
                    print(f'FUEL [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [COST {round((full_fuel - available_fuel)*fuel_cost)}]')
                    available_fuel = full_fuel
                sleep(round(i['duration']['value']/60))
                if s==len(response.json()['routes'][0]['legs'][0]['steps'])-1:
                    sending_or_saving_data(mode,f'END [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters] [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds] [TOTAL_DISTANCE {response.json()["routes"][0]["legs"][0]["distance"]["value"]} meters] [COST {response.json()["routes"][0]["legs"][0]["distance"]["value"]*trip_cost_per_m}]\n',producer,kafka_topic)
                    print(f'END [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters] [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds] [TOTAL_DISTANCE {response.json()["routes"][0]["legs"][0]["distance"]["value"]} meters] [COST {response.json()["routes"][0]["legs"][0]["distance"]["value"]*trip_cost_per_m}]')
                else:
                    sending_or_saving_data(mode,f'WAYPOINT [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters]  [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds]\n',producer,kafka_topic)
                    print(f'WAYPOINT [TAXI_ID {socket.gethostname()}] [REGION {region}] [TRIP_ID {trip_id}] [DISTANCE_TRAVELLED {i["distance"]["value"]} meters]  [REACHED {i["end_location"]["lat"]},{i["end_location"]["lng"]}] [DURATION {i["duration"]["value"]} seconds]')
                available_fuel -= i["distance"]["value"] * fuel_consumption_per_m
        except Exception as e:
            print(e)
            mode = 'file'
        sleep(random.uniform(20,40))


if __name__=="__main__":
    main()