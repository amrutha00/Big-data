import threading
import time
import socket
import json
import random
import sys
from queue import Queue
import logging

mutex = threading.Lock() #for slots
task_mutex = threading.Lock() #for tasks_completed 

class Worker_details:
    """
        Class to store worker details
        Methods: increment_slot
                 decrement_slot

    """
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
            Initialises Master attributes
            self.sem will tell us the number of slots available.
            
        """
        self.job_pool = []
        self.wait_queue = Queue()
        self.workers = dict()  # changed to dictionary
        self.algo = algo
        self.tasks_completed = set()
        
        
    
    """
    we are not using this at all!
    Let's delete this
    def parse_job(self, job):
            Argument(s): the job text
            This function reads the json text, converts it into a python dictionary \
            and appends this dictinary to the job_pool
        content = json.loads(job)
        self.job_pool.append(content)
    """

    def read_config(self, config):
        """
            Argument(s): config text
            This function reads the json text, creates a worker_details object for each worker 
            and appends the object to the workers list
        """
        content = json.loads(config)
        summ = 0
        for worker in content['workers']:
            obj = Worker_details(worker['worker_id'], worker['slots'], '127.0.0.1', worker['port'])
            summ += int(worker['slots'])
            # self.workers.append(obj)
            self.workers[worker['worker_id']] = obj
        self.config_workers = self.workers.copy()  # self.config_workers will store the original configuration details
        self.sem = threading.BoundedSemaphore(summ) # Initialising the counter with the number of slots

    def get_available_workers(self):
        """
            returns a list of worker ids which have at least one slot available
        """
        available_workers = []
        mutex.acquire()
        temp = self.workers
        mutex.release()
        for i in temp:
            if temp[i].no_of_slots > 0:
                available_workers.append(i)
        
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
        self.sem.acquire(blocking=True)
        worker_id = self.find_worker()
        port_number = self.workers[worker_id].port
        task = json.dumps(task)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.workers[worker_id].ip_addr, port_number))
            s.send(task.encode())

        self.workers[worker_id].decrement_slot()


    def listen_for_worker_updates(self):
        rec_port = 5001
        rec_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rec_socket.bind(('', rec_port))
        rec_socket.listen(3)
        while True:
            connectionSocket, addr = rec_socket.accept()
            message = connectionSocket.recv(2048).decode()
            message = json.loads(message)
            logging.debug('Received {}'.format(message))
            task_mutex.acquire()
            self.tasks_completed.add(message[1])  # message[1] has task_id
            logging.debug('Added {} to tasks completed'.format(message[1]))
            task_mutex.release()
            # self.workers[message[0]]['slots'] += 1 #message[0] has worker_id,increment the slot
            self.workers[message[0]].increment_slot()
            logging.debug('incremented slot for {}'.format(message[0]))
            self.sem.release()
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
            reducers.add(task['task_id'])
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
        logging.debug('Completed job {}'.format(job['job_id']))

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
            self.sem.acquire(blocking=True)
            job = self.wait_queue.get()
            logging.debug('Started job with job id {}'.format(job['job_id']))
            self.schedule_all_tasks(job)
            time.sleep(1)


def main():
    logging.basicConfig(filename="yacs.log", level=logging.DEBUG,
    format='%(filename)s:%(funcName)s:%(message)s:%(asctime)s')
    config_file = open(sys.argv[1], "r")
    algo = sys.argv[2]
    masterProcess = Master(algo)
    masterProcess.read_config(config_file.read())
    config_file.close()
    t1 = threading.Thread(target=Master.listen_for_job_requests, args=[masterProcess])
    t2 = threading.Thread(target=Master.listen_for_worker_updates, args=[masterProcess])
    t3 = threading.Thread(target=Master.schedule_jobs, args=[masterProcess])
    # masterProcess.schedule_jobs()
    # print("Hello")
    t3.start()
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()
    t3.join()


if __name__ == "__main__":
    main()
