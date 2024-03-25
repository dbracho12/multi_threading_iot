# 1 Definir la funciones.
import time
import json
import random
import threading
import signal
from queue import Queue
import paho.mqtt.client as paho
from dotenv import dotenv_values

config = dotenv_values()

#----------------------------- Funsion callback local Topicoas local-----------------#
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

#----------------------------- Funsion callback on_message_local-----------------#
def on_message_local(client, userdata, message):
    queue_local = userdata["queue_local"]
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))

    queue_local.put({"topico": topico, "mensaje": mensaje})

#----------------------------- Funsion callback procesamiento_local-----------------#
def procesamiento_local(name, flags, client_local, client_remoto):
    print("Comienza thread", name)
    queue = client_local._userdata["queue_local"]

    while flags["thread_continue"]:
        # Queue de python ya resuelve automaticamente el concepto
        # de consumidor con "get".
        # En este caso el sistema esperará (block=True) hasta que haya
        # al menos un item disponible para leer        
        msg = queue_local.get(block=True)

        # Sino hay nada por leer, vuelvo a esperar por otro mensaje
        if msg is None:
            continue

        # Hay datos para leer y consumir
        topico = msg['topico']
        mensaje = msg['mensaje']
        
        topico_remoto = config["DASHBOARD_TOPICO_BASE"] + topico
        client_remoto.publish(topico_remoto, mensaje)

    print("Termina thread", name)

#----------------------------- Funsion callback on_connect_remoto-----------------#
def on_connect_remoto(client, userdata, flags, rc):
    if rc == 0:
        print("Mqtt Remoto conectado")
    
        # Aquí Suscribirse a los topicos remotos deseados
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/volar")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/luces/1")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/motores/#")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "actuadores/joystick")
        client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "keepalive/request")
        #client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "sensores/gps")
        #client.subscribe(config["DASHBOARD_TOPICO_BASE"] + "sensores/inerciales")
    else:
        print(f"Mqtt Remoto connection faild, error code={rc}")

#----------------------------- Funsion callback on_message_remoto-----------------#
def on_message_remoto(client, userdata, message):
    queue_remoto = userdata["queue_remoto"]
    topico = message.topic
    mensaje = str(message.payload.decode("utf-8"))

    queue_remoto.put({"topico": topico,"mensaje": mensaje})


#----------------------------- Funsion callback procesamiento_remoto-----------------#
def procesamiento_remoto(name,queue_remoto,client_local, flags):
    print(name,"thread comienza")
    while True:
        datos = queue_remoto.get(block=True)
        topico = datos["topico"]
        mensaje = datos["mensaje"]
        
        # Quitar la parte del tópico que corresponde al dashboard y el usuario
        topico_local = topico.replace(config["DASHBOARD_TOPICO_BASE"], '')
        # Agregar el destintivo de que el mensaje viene del dashboard
        topico_local = "dashboardiot/" + topico_local
        #--------analisis del keepalive-------------#
        if topico == "monitoreo/keepalive/request":
            client_remoto.publish("keepalive/ack", 1)
        print("Dato recibido de espacio -topico:", topico_local)
        print("Dato recibido del espacio -mensaje:",mensaje)

        # Enviar el mensaje al cliente MQTT local para que otros
        # acutadores o sensores estén al tanto de lo recibido
        client_local.publish(topico_local, mensaje)

#----------------------------- Funsion callback finalizar_programa---------------#

flags = {"thread_continue": True}
def finalizar_programa(sig, frame):
    global flags
    print("Señal de terminar programa")    
    flags["thread_continue"] = False

if __name__ == "__main__":    
    # ---------------------- conectarse a MQTT remoto ----------------------#
    queue_remoto = Queue()

    random_id = random.randint(1, 999)
    client_remoto = paho.Client(f"gps_mock_remoto_{random_id}")
    client_remoto.on_connect = on_connect_remoto
    client_remoto.on_message = on_message_remoto
    # Configurar las credenciales del broker remoto
    client_remoto.username_pw_set(config["DASHBOARD_MQTT_USER"], config["DASHBOARD_MQTT_PASSWORD"])
    client_remoto.user_data_set( 
        {
            "queue_remoto": queue_remoto,
        }
    )

    client_remoto.connect(config["DASHBOARD_MQTT_BROKER"], int(config["DASHBOARD_MQTT_PORT"]))
    client_remoto.loop_start()

    # ---------------------- conectarse a MQTT local ----------------------#
    queue_local = Queue()

    client_local = paho.Client("gps_mock_local")
    client_local.on_connect = on_connect_local
    client_local.on_message = on_message_local
    client_local.user_data_set( 
        {
            "queue_local": queue_local,
        }
    )

    client_local.connect(config["BROKER"], int(config["PORT"]))
    client_local.loop_start()

     # ----------Capturar el finalizar programa forzado -------------------------#
    signal.signal(signal.SIGINT, finalizar_programa)

     #--------Se invocará threads para el procc_local y procc_remoto -----------#
    print("Lanzar thread de procesamiento de MQTT local")
    thread_procesamiento_local = threading.Thread(target=procesamiento_local, args=("procesamiento_local", flags, client_local, client_remoto), daemon=True)
    thread_procesamiento_local.start()

    thread_procesamiento_remoto = threading.Thread(
        target=procesamiento_remoto,args=("procc_remoto", queue_remoto,client_local,flags),
        daemon =True
    )
    thread_procesamiento_remoto.start()
    
    # ----------------------
    # El programa principal queda a la espera de que se desee
    # finalizar el programa
    while flags["thread_continue"]:
        # busy loop
        time.sleep(0.5)
    
    # ----------------------
    print("Comenzando la finalización de los threads...")
    # Se desea terminar el programa, desbloqueamos los threads
    # con un mensaje vacio
    queue_local.put(None)
    queue_remoto.put(None)
    
    # No puedo finalizar el programa sin que hayan terminado los threads
    # el "join" espera por la conclusion de cada thread, debo lanzar el join
    # por cada uno
    thread_procesamiento_local.join()

    client_local.disconnect()
    client_local.loop_stop()

    client_remoto.disconnect()
    client_remoto.loop_stop()
