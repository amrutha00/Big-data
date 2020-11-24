import threading
import socket
import json
import random
import sys
from queue import Queue
import logging
import copy

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
            self.workers[worker['worker_id']] = obj

        # self.config_workers will store the original configuration details
        self.config_workers = copy.deepcopy(self.workers)  

        # Initialising the counter with the number of slots
        self.sem = threading.BoundedSemaphore(summ)

    def get_available_workers(self):
        """
            Returns a list of worker ids which have at least one slot available
        """
        available_workers = []
        mutex.acquire()
        temp = copy.deepcopy(self.workers)
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
        # Waiting until there's at least one slot available
        self.sem.acquire(blocking=True)

        # Getting the worker id with available slots based on the scheduling algorithm
        worker_id = self.find_worker()
        port_number = self.workers[worker_id].port
        task = json.dumps(task)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.workers[worker_id].ip_addr, port_number))
            s.send(task.encode())

        self.workers[worker_id].decrement_slot()


    def listen_for_worker_updates(self):
        """
            Listens for updates from the workers
            Adds the task received to the tasks_completed set
            Increments the sempahore to indicate that a slot is available
        """
        rec_port = 5001
        rec_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        rec_socket.bind(('', rec_port))
        rec_socket.listen(10)
        while True:
            connectionSocket, addr = rec_socket.accept()
            message = connectionSocket.recv(2048).decode()
            message = json.loads(message)
            task_mutex.acquire()
            self.tasks_completed.add(message[1])  # message[1] has task_id
            task_mutex.release()
            self.workers[message[0]].increment_slot()
            self.sem.release()
            connectionSocket.close()

    def dependency_wait_mapper(self, job):
        """
            This function waits till all map tasks of the job have been finished
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
        """
            This function waits till all reduce tasks of the job have been finished
        """
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

        # Wait for all map tasks to complete
        self.dependency_wait_mapper(job)

        for task in job["reduce_tasks"]:
            self.schedule_task(task)

        # Wait for all reduce tasks to complete
        self.dependency_wait_reducer(job)

        # The job has been completed
        logging.debug('Completed job {}'.format(job['job_id']))
        # print("Number of threads is {} after completing {}".format(threading.active_count(), job['job_id']))

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
            connectionSocket, addr = rec_socket.accept()  
            message = connectionSocket.recv(2048).decode()
            message = json.loads(message)
            self.wait_queue.put(message)  
            connectionSocket.close()

    def schedule_job(self):
        """
            This function waits until a slot is available, after which
            it dequeus from the wait queue and schedules all tasks of that job
        """
        self.sem.release()
        # self.sem.acquire(blocking=True)
        # self.sem.release()
        job = self.wait_queue.get()
        logging.debug('Started job with job id {}'.format(job['job_id']))
        self.schedule_all_tasks(job)

    def schedule_jobs(self):
        """
            Whenever there's a job waiting in the wait_queue, 
            this function creates a new thread for each job and runs it
        """
        while True:
            if (self.wait_queue.qsize() != 0):
                self.sem.acquire(blocking=True)
                threading.Thread(target=Master.schedule_job,args=[self]).start()

def main():

    # Logging configuration
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
    t1.start()
    t2.start()
    t3.start()
    
    t1.join()
    t2.join()
    t3.join()


if __name__ == "__main__":
    main()
