from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from sense_hat import SenseHat
import time
import json
from datetime import datetime

# AWS IoT Core configuration

ENDPOINT = ""
PORT = 8883
SENSOR_TOPIC = "raspi/data"
DEVICE_CERT = "certs/device.pem.crt"
PRIVATE_KEY = "certs/a-private.pem.key"
ROOT_CERT = "certs/AmazonRootCA1.pem"
QOS_LEVEL = 1
sense = SenseHat()
sense.clear()



# Define function to collect weather data

def get_weather(id):
    temperature = float(sense.get_temperature() * (9 / 5) + 32)
    humidity = float(sense.get_humidity())
    pressure = float(sense.get_pressure())
    timestamp = str(datetime.now())
    weather = {'ID': id, 'Temperature': temperature, 'Humidity': humidity, 'Pressure': pressure, 'Timestamp': timestamp}

    return weather

# Initialize MQTT client

client = AWSIoTMQTTClient('raspberry_pi_client')
# Configure client endpoint, port information, certs

client.configureEndpoint(ENDPOINT, PORT)
client.configureCredentials(ROOT_CERT, PRIVATE_KEY, DEVICE_CERT)
client.configureOfflinePublishQueueing(-1)
client.configureDrainingFrequency(2)
client.configureConnectDisconnectTimeout(10)
client.configureMQTTOperationTimeout(5)

# Connect

print('Connecting to endpoint ' + ENDPOINT)
client.connect()

# Loop through metrics, publish every 15 seconds

id = 0
metrics = ['Temperature', 'Humidity', 'Pressure']

while True:
    for metric in metrics:
        data = {key: get_weather(id)[key] for key in (metric, 'ID', 'Timestamp')}
        sub_topic = SENSOR_TOPIC.split('/')[0] + '/metrics/' + metric
        client.publish(sub_topic, json.dumps(data), QOS_LEVEL)
        print(f'The {metric} is {data[metric]}. Published message on topic {sub_topic}')

    client.publish(SENSOR_TOPIC, json.dumps(get_weather(id)), QOS_LEVEL)

    print('Published message on topic ' + SENSOR_TOPIC)
    id += 1
    time.sleep(30)

