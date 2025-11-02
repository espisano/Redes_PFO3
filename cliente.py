import socket
import json
import time

HOST = '127.0.0.1'  
PORT = 65432        
def send_task(task):

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            
            s.connect((HOST, PORT))
            
            task_json = json.dumps(task)
            s.sendall(task_json.encode('utf-8'))

            print(f"-> Tarea enviada: {task_json}")

            data = s.recv(1024)
            response = json.loads(data.decode('utf-8'))
            
            print(f"<- Respuesta recibida: {response}")
            return response

    except ConnectionRefusedError:
        print(f"Error: Conexión rechazada. Asegúrate de que el servidor esté ejecutándose en {HOST}:{PORT}.")
        return None
    except Exception as e:
        print(f"Error inesperado durante la comunicación: {e}")
        return None


if __name__ == '__main__':
    
    tarea_suma = {
        "id": 101,
        "operacion": "sumar",
        "operandos": [45, 12],
        "tiempo_simulado": 2 
    }
    
    tarea_resta = {
        "id": 102,
        "operacion": "restar",
        "operandos": [100, 30]
    }
    
    tarea_invalida = {
        "id": 103,
        "operacion": "multiplicar", 
        "operandos": [5, 5]
    }
    
    print("\n--- Ejecutando Tarea de Suma ---")
    send_task(tarea_suma)
    
    print("\n--- Ejecutando Tarea de Resta ---")
    send_task(tarea_resta)

    print("\n--- Ejecutando Tarea Inválida ---")
    send_task(tarea_invalida)