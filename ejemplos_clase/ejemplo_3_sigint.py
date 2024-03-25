
import time
import threading
import signal
from queue import Queue


def producir_datos(name, queue,flags):
    print("Comienza thread", name,flags)
    while flags["thread_continue"]:
        # Queue de python ya resuelve automaticamente el concepto
        # de productor con "put", si el consumidor está usando el buffer
        # el sistema esperara en "put" hasta tener disponible el recurso
        # para ingresar un nuevo dato (producir)
        msg = input("Productor: Ingrese un texto cualquiera:\n").lower()
        queue.put(msg)

    print("Termina thread", name)


def consumir_datos(name, queue, flags):
    print("Comienza thread", name,flags)
    msg = ""
    while flags["thread_continue"]:
        # Queue de python ya resuelve automaticamente el concepto
        # de consumidor con "get".
        # En este caso el sistema esperará (block=True) hasta que haya
        # al menos un item disponible para leer        
        msg = queue.get(block=True)

        # Hay datos para leer, los consumo e imprimo en consola
        print("Consumidor:", msg)

    print("Termina thread", name)


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
    # Buffer que utilizaremos para almacenar la información
    # a compartir entre los threads.
    # Python simplemente en queue todo el mecanismo de proteccion
    # de lock para casos como estos (productor & consumidor)
    queue = Queue()

    print("Lanzar thread producir_datos")
    thread1 = threading.Thread(target=producir_datos, args=("producir_datos", queue,flags), daemon=True)
    thread1.start()

    print("Lanzar thread consumir_datos")
    thread2 = threading.Thread(target=consumir_datos, args=("consumir_datos", queue,flags), daemon=True)
    thread2.start()

    # Capturar el finalizar programa forzado
    # Capturar el finalizar programa forzado
    signal.signal(signal.SIGINT, finalizar_programa)

    while flags["thread_continue"]:
        time.sleep(0.5)

    # No puedo finalizar el programa sin que hayan terminado los threads
    # el "join" espera por la conclusion de cada thread, debo lanzar el join
    # por cada uno
    print("Espero que termine thread producir_datos")
    thread1.join()
    print("Espero que termine thread consumir_datos")
    thread2.join()
