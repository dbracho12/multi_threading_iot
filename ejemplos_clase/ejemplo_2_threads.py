
import time
import threading


def thread_one(name):
    print(name, "comienza")
    for i in range(10):
        print(name, "duerme")
        time.sleep(1)
    print(name, "termina")


def thread_two(name):
    print(name, "comienza")
    for i in range(2):
        print(name, "duerme")
        time.sleep(1)
    print(name, "termina")


if __name__ == "__main__":
    
    print("Lanzar thread one")
    thread1 = threading.Thread(target=thread_one, args=("thread_uno",), daemon=True)
    thread1.start()

    print("Lanzar thread two")
    thread2 = threading.Thread(target=thread_two, args=("thread_dos",), daemon=True)
    thread2.start()

    # No puedo finalizar el programa sin que hayan terminado los threads
    # el "join" espera por la conclusion de cada thread, debo lanzar el join
    # por cada uno
    print("Espero que termine thread one")
    thread1.join()
    print("Espero que termine thread two")
    thread2.join()