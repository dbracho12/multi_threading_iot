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


# Aquí crear el callback on_message_local (Productor) 
#produce los topicosde on_connect_local
def on_message_local(client, userdata, message):
    queue = userdata["queue"]
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))

    queue.put({"topico": topico, "mensaje": mensaje})


# Aquí crear el callback procesamiento_local (Consumidor) y mandar a la nube.
# Aquí crear el callback procesamiento_local
def procesamiento_local(name, flags, client_local, client_remoto):
    print("Comienza thread", name)
    queue = client_local._userdata["queue"]

    while flags["thread_continue"]:
        # Queue de python ya resuelve automaticamente el concepto
        # de consumidor con "get".
        # En este caso el sistema esperará (block=True) hasta que haya
        # al menos un item disponible para leer        
        msg = queue.get(block=True)

        # Sino hay nada por leer, vuelvo a esperar por otro mensaje
        if msg is None:
            continue

        # Hay datos para leer, los consumo e imprimo en consola
        print(f"mensaje recibido en thread {name}:")
        print(f"{msg['topico']}: {msg['mensaje']}")
        topico = msg['topico']
        mensaje = msg['mensaje']
        
        topico_remoto = config["DASHBOARD_TOPICO_BASE"] + topico
        print("enviando el topico a la nube",topico_remoto)
        print("enviando el mensaje",mensaje)

        client_remoto.publish(topico_remoto, mensaje)

    print("Termina thread", name)
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

# Flags que almacenaremos para todos los threads en comun
# como por ejemplo el flag que indica que debe terminarse el thread
# Recordar que no se debe pasar al threads variables tipo bool, int o string,
# siempre usar un objeto (como en este caso un diccionario)
flags = {"thread_continue": True}
def finalizar_programa(sig, frame):
    global flags
    print("Señal de terminar programa")    
    flags["thread_continue"] = False


if __name__ == "__main__":    
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
    queue_local = Queue()

    client_local = paho.Client("gps_mock_local")
    client_local.on_connect = on_connect_local
    client_local.on_message = on_message_local

    # Dejar disponible la queue dentro de "user_data"
    client_local.user_data_set( 
        {
            "queue": queue_local,
        }
    )

    client_local.connect(config["BROKER"], int(config["PORT"]))
    client_local.loop_start()

    # Capturar el finalizar programa forzado


    # El programa principal solo armará e invocará threads
    print("Lanzar thread de procesamiento de MQTT local")
    thread_consumidor_remoto = threading.Thread(target=procesamiento_local, args=("procesamiento_local", flags, client_local, client_remoto), daemon=True)
    thread_consumidor_remoto.start()


    # ----------------------
    # El programa principal queda a la espera de que se desee
    # finalizar el programa
    while flags["thread_continue"]:
        pass

    print("Comenzando la finalización de los threads...")
    # Se desea terminar el programa, desbloqueamos el thread
    # con un mensaje vacio y se lo espera con un join

    client_local.disconnect()
    client_local.loop_stop()

    client_remoto.disconnect()
    client_remoto.loop_stop()