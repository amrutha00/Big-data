import threading
import time
import socket
import json
import random
import sys
from queue import Queue


# import threading

class Worker_details:
    def __init__(self, worker_id, slots, ip_addr, port):
        self.worker_id = worker_id
        self.no_of_slots = slots
        self.ip_addr = ip_addr
        self.port = port

    def increment_slot(self):
        """
            Given the worker id, update its number of available slots
        """
        mutex.acquire()
        self.no_of_slots += 1
        mutex.release()

    def decrement_slot(self):
        """
            Given the worker id, update its number of available slots
        """
        mutex.acquire()
        self.no_of_slots -= 1
        mutex.release()


class Master:
    def __init__(self, algo):
        """
            Initialises the job_pool, wait_queue lists and the workers dictionary
        """
        self.job_pool = []
        self.wait_queue = Queue()
        self.workers = []  # changed to list
        self.algo = algo
        self.tasks_completed = set()
        task_mutex = threading.Lock() #for tasks_completed 
        mutex = threading.Lock() #for slots

    def parse_job(self, job):
        """
            Argument(s): the job text
            This function reads the json text, converts it into a python dictionary \
            and appends this dictinary to the job_pool
        """
        content = json.loads(job)
        self.job_pool.append(content)

    def read_config(self, config):
        """
            Argument(s): config text
            This function reads the json text, creates a dictionary for each worker. \	#change to object for each worker
            Each worker is indexed by its id in the self.workers dicitonary
        """
        content = json.loads(config)
        for worker in content['workers']:
            obj = Worker_details(worker['worker_id'], worker['slots'], '127.0.0.1', worker['port'])
            self.workers.append(obj)
        # self.workers[worker['worker_id']] = dict()
        # self.workers[worker['worker_id']]['slots'] = worker['slots']
        # self.workers[worker['worker_id']]['ip_addr'] = 127.0.0.1 #worker['ip_addr']
        # self.workers[worker['worker_id']]['port'] = worker['port']
        self.config_workers = self.workers.copy()  # what is this

    def get_available_workers(self):
        """
            returns a list of worker ids which have at least one slot available
        """
        available_workers = []
        mutex.acquire()
        for i in self.workers:
            if self.workers[i].no_of_slots > 0:
                available_workers.append(i)
        mutex.release()
        return available_workers

    def find_worker(self):
        """
            Finds a worker based on the algorithm provided as an argument
        """
        if self.algo == 'random':
            available_workers = self.get_available_workers()
            choice = random.choice(available_workers)
            return choice
        elif self.algo == 'round-robin':
            raise NotImplementedError
        else:
            raise NotImplementedError

    def schedule_task(self, task):
        """
            Schedules the given task to one of the workers
        """
        worker_id = self.find_worker()
        port_number = self.workers[worker_id].port
        task = json.dumps(task)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(("localhost", port_number))
            s.send(task.encode())

        self.workers[worker_id].decrement_slot()

    def listen_for_worker_updates(self):
        rec_port = 5001
        rec_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rec_socket.bind(('', rec_port))
        rec_socket.listen(1)
        while True:
            connectionSocket, addr = rec_socket.accept()
            message = connectionSocket.recv(2048).decode()
            message = json.loads(message)
            # message = message.split(" ")
            task_mutex.acquire()
            self.tasks_completed.add(message[1])  # message[1] has task_id
            task_mutex.release()
            mutex.acquire()
            # self.workers[message[0]]['slots'] += 1 #message[0] has worker_id,increment the slot
            self.workers[message[0]].increment_slot()
            mutex.release()
            connectionSocket.close()

    def dependency_wait_mapper(self, job):
        """
            This function waits till all map tasks of the job have been finished
            I think this function only listens for updates from the workers
        """
        mappers = set()
        for task in job["map_tasks"]:
            mappers.add(task['task_id'])
        n = len(mappers)
        while n:
            tempSet = self.tasks_completed.copy()
            for i in mappers:
                if i in tempSet:
                    task_mutex.acquire()
                    self.tasks_completed.remove(i)
                    task_mutex.release()
                    n -= 1

    def dependency_wait_reducer(self, job):
        reducers = set()
        for task in job["reduce_tasks"]:
            mappers.add(task['task_id'])
        n = len(reducers)
        while (n):
            tempSet = self.tasks_completed.copy()
            for i in reducers:
                if i in tempSet:
                    task_mutex.acquire()
                    self.tasks_completed.remove(i)
                    task_mutex.release()
                    n -= 1

    def schedule_all_tasks(self, job):
        """
            Schedules map and reduce tasks of the given job
        """
        for task in job["map_tasks"]:
            self.schedule_task(task)
        self.dependency_wait_mapper(job)
        for task in job["reduce_tasks"]:
            self.schedule_task(task)
        self.dependency_wait_reducer(job)

    def listen_for_job_requests(self):
        """
            This function listens for job requests and
            adds the jobs received to the wait queue
        """
        rec_port = 5000
        rec_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rec_socket.bind(('', rec_port))
        rec_socket.listen(1)
        while True:
            connectionSocket, addr = rec_socket.accept()  # what is serverSocket?
            message = connectionSocket.recv(2048).decode()
            message = json.loads(message)
            self.wait_queue.put(message)  # added self
            connectionSocket.close()

    def schedule_jobs(self):
        """
            Dequeues jobs from the wait queue and calls schedule_all_tasks for each job
            Doubt: Idk how often this should dequeue from the wait queue
            Should there be a function to check for unused slots? We need to discuss this
        """
        while True:
            while len(self.get_available_workers()) == 0:
                pass
            job = self.wait_queue.get()
            self.schedule_all_tasks(job)
            time.sleep(1)


def main():
    config_file = open(sys.argv[1], "r")
    algo = sys.argv[2]
    masterProcess = Master(algo)
    masterProcess.read_config(config_file.read())
    config_file.close()
    t1 = threading.Thread(target=Master.listen_for_job_requests)
    t2 = threading.Thread(target=Master.listen_for_worker_updates)
    masterProcess.schedule_jobs()

    t1.start()
    t2.start()
    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
