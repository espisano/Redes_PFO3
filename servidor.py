import socket
import threading
import json
import queue
import time
import random
from concurrent.futures import ThreadPoolExecutor

HOST = '127.0.0.1'  
PORT = 65432        
MAX_WORKERS = 5     
TASK_TIMEOUT = 10   

#SIMULACIÓN DE COLA DE MENSAJES (RabbitMQ)
task_queue = queue.Queue()
worker_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
results_cache = {}
cache_lock = threading.Lock() 

print(f"[{threading.current_thread().name}] Arquitectura distribuida inicializada.")
print(f"[{threading.current_thread().name}] Cola de Tareas (RabbitMQ simulado) lista.")
print(f"[{threading.current_thread().name}] Pool de Workers (Servidores Workers) con {MAX_WORKERS} hilos listo.")


def worker_function(task_data):
    thread_name = threading.current_thread().name
    task_id = task_data.get('id', 'N/A')
    
    try:
        # Simula acceso a Almacenamiento Distribuido (PostgreSQL/S3)
        print(f"[{thread_name}] Tarea {task_id}: Accediendo a Almacenamiento Distribuido y procesando...")
        sleep_time = random.uniform(1.0, 3.0) 
        time.sleep(sleep_time) 

        operacion = task_data.get('operacion')
        operandos = task_data.get('operandos', [])
        resultado = None

        if operacion == 'sumar' and len(operandos) == 2:
            resultado = operandos[0] + operandos[1]
        elif operacion == 'restar' and len(operandos) == 2:
            resultado = operandos[0] - operandos[1]
        else:
            resultado = f"Operación '{operacion}' no soportada. (POSTGRESQL/S3: Datos no encontrados)"
            
        final_result = {
            "estado": "completado", 
            "resultado": resultado, 
            "procesado_por": thread_name,
            "tiempo_simulado": round(sleep_time, 2)
        }
        
        # Guardar el resultado en la caché (simula almacenamiento distribuido)
        with cache_lock:
            results_cache[task_id] = final_result
            
        print(f"[{thread_name}] Tarea {task_id} completada y resultado guardado.")
        
    except Exception as e:
        error_result = {"estado": "error_worker", "detalle": str(e)}
        with cache_lock:
            results_cache[task_id] = error_result
        print(f"[{thread_name}] ERROR al procesar tarea {task_id}: {e}")


def task_consumer_loop():

    while True:
        try:
            task = task_queue.get(timeout=1)
            task_id = task.get('id')
            
            worker_pool.submit(worker_function, task)
            
            print(f"[ConsumerThread] Tarea {task_id} consumida de cola y enviada al Pool de Workers.")
            
            task_queue.task_done()

        except queue.Empty:
            continue
        except Exception as e:
            print(f"[ConsumerThread] Error en el bucle del consumidor: {e}")
            break


def handle_client_connection(conn, addr):

    thread_name = threading.current_thread().name

    try:
        data = conn.recv(1024)
        if not data:
            return

        task_json = data.decode('utf-8')
        task = json.loads(task_json)
        
        task_id = task.get('id', threading.get_ident())
        task['id'] = task_id 

        print(f"[{thread_name}] Tarea {task_id} recibida. Publicando en la cola...")
        task_queue.put(task) 

        result = None
        start_time = time.time()

        while time.time() - start_time < TASK_TIMEOUT:
            with cache_lock:
                if task_id in results_cache:
                    result = results_cache.pop(task_id) 
                    break
            time.sleep(0.1) 

        if result:
            response_to_send = json.dumps(result).encode('utf-8')
            conn.sendall(response_to_send)
            print(f"[{thread_name}] Resultado de Tarea {task_id} enviado a {addr}.")
        else:
            error_msg = {"estado": "error", "detalle": f"Timeout. La tarea {task_id} no se completó en {TASK_TIMEOUT} segundos."}
            conn.sendall(json.dumps(error_msg).encode('utf-8'))
            
    except json.JSONDecodeError:
        error_msg = {"estado": "error", "detalle": "Formato JSON inválido."}
        conn.sendall(json.dumps(error_msg).encode('utf-8'))
    except Exception as e:
        print(f"[{thread_name}] Error en la comunicación: {e}")
    finally:
        conn.close()
        print(f"[{thread_name}] Conexión con {addr} cerrada.")


def start_server():
    """Función principal que inicializa el consumidor y el socket del distribuidor."""
    
    consumer_thread = threading.Thread(
        target=task_consumer_loop,
        name="ConsumerThread",
        daemon=True
    )
    consumer_thread.start()
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(5)
        print(f"[{threading.current_thread().name}] Servidor Distribuidor escuchando en {HOST}:{PORT}...")

        while True:
            try:
                conn, addr = s.accept()
                
                client_thread = threading.Thread(
                    target=handle_client_connection, 
                    args=(conn, addr),
                    daemon=True
                )
                client_thread.start()
                
            except KeyboardInterrupt:
                print(f"[{threading.current_thread().name}] Deteniendo servidor...")
                break
            except Exception as e:
                print(f"[{threading.current_thread().name}] Error en el bucle principal: {e}")
                break

    worker_pool.shutdown(wait=False)
    print(f"[{threading.current_thread().name}] Servidor y Workers detenidos.")


if __name__ == '__main__':
    start_server()