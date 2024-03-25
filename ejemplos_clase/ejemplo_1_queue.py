import time
import json
import random
import threading
import signal
from queue import Queue
import paho.mqtt.client as paho
from dotenv import dotenv_values

config = dotenv_values()

# ----------------------
# Aquí crear los callbacks de MQTT Local
# Aquí crear el callback on_connect_local
def on_connect_local(client, userdata, flags, rc):
    if rc == 0:
        print("Mqtt Local conectado")

        # Aquí Suscribirse a los topicos locales deseados
        client.subscribe("actuadores/volar")
        client.subscribe("actuadores/luces/1")
        client.subscribe("actuadores/motores/#")
        client.subscribe("actuadores/joystick")
        client.subscribe("sensores/gps")
        client.subscribe("sensores/inerciales")
    else:
        print(f"Mqtt Local connection faild, error code={rc}")


# Aquí crear el callback on_message_local recibir paquete mqtt local
def on_message_local(client, userdata, message):
    queue = userdata["queue"]#client_local.user_data_set({"queue": queue_local}) 
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))
    
    # Agregar cada nuevo mensaje a la queue (put)
    queue.put({"topico": topico, "mensaje": mensaje})


# ----------------------
# ----------------------
# Aquí crear los callbacks de MQTT Remoto

def on_connect_remoto(client, userdata, flags, rc):
    if rc == 0:
        print("Mqtt Remoto conectado")

        # Aquí Suscribirse a los topicos remotos deseados
    else:
        print(f"Mqtt Remoto connection faild, error code={rc}")

# ----------------------

if __name__ == "__main__":  
    # Paso1 crear una queue para el cliente local --> queue_local
    queue_local = Queue()
    #queue_remoto = Queue() para el remoto

    # ----------------------
    # Aquí conectarse a MQTT remoto
    random_id = random.randint(1, 999)
    client_remoto = paho.Client(f"gps_mock_remoto_{random_id}")
    client_remoto.on_connect = on_connect_remoto
    # Configurar las credenciales del broker remoto
    client_remoto.username_pw_set(config["DASHBOARD_MQTT_USER"], config["DASHBOARD_MQTT_PASSWORD"])
    client_remoto.connect(config["DASHBOARD_MQTT_BROKER"], int(config["DASHBOARD_MQTT_PORT"]))
    client_remoto.loop_start()


    # Aquí conectarse a MQTT local
    # crear una queue para el cliente local --> queue_local

    client_local = paho.Client("gps_mock_local")
    client_local.on_connect = on_connect_local
    client_local.on_message = on_message_local

    # Paso 2 Dejar disponible la queue dentro de "user_data"
    client_local.user_data_set({"queue": queue_local})

    client_local.connect(config["BROKER"], int(config["PORT"]))
    client_local.loop_start()

    # ----------------------
    # El programa principal quedará a la espera
    # de que llegue un nuevo mensaje MQTT
    while True:
        msg = queue_local.get(block=True)
        # Hay datos para leer, los consumo e imprimo en consola
        #topico = msg['topico']
        #mensaje = msg['mensaje']
        #print(f"{msg['topico']}: {msg['mensaje']}")
        print(msg)
    
    client_local.disconnect()
    client_local.loop_stop()

    client_remoto.disconnect()
    client_remoto.loop_stop()