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
dep_mutex = threading.Lock() # for deleting / adding to dependency pool
exec_mutex = threading.Lock() # for deleting / adding to execution pool

"""
    These comments are NOT FINAL
    These are meant for us to understand the code when we get confused 
    We shall modify the comments later
"""

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

        # Tasks completed is a set of task ids that have been completed
        self.tasks_completed = set()

        # Execution pool is a dictionary containing information about the running jobs
        # Key: job_id
        # Value: The job dictionary
        self.execution_pool = dict()

        # This will help us keep track of dependencies and job completion of the jobs running
        # Each element will have the following format:
        # [ { map_task_1_id, map_task_2_id, ... } , { reduce_task_1_id, reduce_task_2_id, ... }, job_id ]
        self.dependency_pool = list()

        self.running_reduce_jobs = set()

        # For round robin
        self.worker_ids = list()
        self.last_used_worker = None
        self.no_of_workers = 0

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
            self.worker_ids.append(worker['worker_id'])
            self.no_of_workers += 1

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
        available_workers = self.get_available_workers()
        if self.algo == 'random':
            choice = random.choice(available_workers)
            self.last_used_worker = choice
            return choice
        elif self.algo == 'round-robin':
            if self.last_used_worker == None:
                self.last_used_worker = available_workers[0]
                return available_workers[0]

            i = 0
            while (self.worker_ids[i] != self.last_used_worker):
                i = (i + 1)%(self.no_of_workers)
            i = (i + 1)%(len(self.worker_ids))
            while (self.worker_ids[i] not in available_workers):
                i = (i + 1)%(self.no_of_workers)
            self.last_used_worker = self.worker_ids[i]
            return self.worker_ids[i]

        else:
            minn = available_workers[0]
            for worker in available_workers:
                if (self.workers[worker].no_of_slots < self.workers[minn].no_of_slots):
                    minn = worker
            self.last_used_worker = minn
            return minn



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

    def schedule_reduce_tasks(self, job_id):
        """
            Given job id of a job currently running,
            this function schedules all the reduce tasks of the job
        """
        reduce_tasks = self.execution_pool[job_id]['reduce_tasks'].copy()
        for task in reduce_tasks:
            self.schedule_task(task)
        self.running_reduce_jobs.add(job_id)



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

    def update_dependencies(self):
        """
            This function essentially goes through all the tasks and 
            updates the dependency pool

            Also checks for each job if all its map tasks have been completed, 
            in which case it would schedule all reduce tasks
            If both map and reduce tasks have been completed, updates dependency_pool
            and execution_pool accordingly, and logs the completion of the job

            Big point: ****
            self.running_reduce_jobs
            Why this? 
            Consider the case when a job's map tasks have been completed.
            In that case, we would schedule all its reduce tasks
            But becase this thread is in an infinite loop,
            there are chances that it will go through the dependency_pool
            again and find this job's reduce tasks 'unscheduled' (happens when reduce
            tasks don't complete before the next iteration of this infinite loop)
            So it would schedule them again unnecessarily!
            We need a set of jobs whose reduce tasks are currently running 
            in order to avoid this
            Hence this set 

        """
        while True:

            # Getting the tasks completed at that instant
            # Tasks completed during this iteration will be considered in the next iteration
            tasks_completed_copy = self.tasks_completed.copy()

            for task in tasks_completed_copy:
                for job_dep in self.dependency_pool:

                    # if the task is one of the map tasks of this particular job,
                    # update the dependency_pool accordingly
                    # Also remove the task from the tasks_completed set
                    if (task in job_dep[0]):
                        dep_mutex.acquire()
                        job_dep[0].remove(task)
                        dep_mutex.release()

                        task_mutex.acquire()
                        self.tasks_completed.remove(task)
                        task_mutex.release()
                        break
                    # Same thing as above, but for reduce tasks
                    elif (task in job_dep[1]):
                        dep_mutex.acquire()
                        job_dep[1].remove(task)
                        dep_mutex.release()

                        task_mutex.acquire()
                        self.tasks_completed.remove(task)
                        task_mutex.release()
                        break


            dep_pool_copy = self.dependency_pool.copy()

            # This part is for either
            #   a) scheduling reduce tasks
            #   b) removing the job from the execution pool and logging its completion
            for job_dep in dep_pool_copy:
                if len(job_dep[0]) == 0:
                    if len(job_dep[1]) == 0:
                        # When the job has been completed
                        dep_mutex.acquire()
                        self.dependency_pool.remove(job_dep)
                        dep_mutex.release()
                        exec_mutex.acquire()
                        del self.execution_pool[job_dep[2]]
                        exec_mutex.release()
                        logging.debug("Completed job {}".format(job_dep[2]))
                        self.running_reduce_jobs.remove(job_dep[2])
                    elif job_dep[2] not in self.running_reduce_jobs:
                        # All map tasks have been completed
                        self.schedule_reduce_tasks(job_dep[2])




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

    def schedule_map_tasks(self, job):
        for task in job['map_tasks']:
            self.schedule_task(task)

    def schedule_job(self):
        """
            This function waits until a slot is available, after which
            it dequeus from the wait queue and adds the job to the execution pool
        """
        self.sem.release()
        job = self.wait_queue.get()
        logging.debug('Started job with job id {}'.format(job['job_id']))
        mapTasks = set()
        for task in job['map_tasks']:
            mapTasks.add(task['task_id'])
        redTasks = set()
        for task in job['reduce_tasks']:
            redTasks.add(task['task_id'])

        dep_mutex.acquire()
        self.dependency_pool.append(list((mapTasks, redTasks, job['job_id'])))
        dep_mutex.release()
        exec_mutex.acquire()
        self.execution_pool[job['job_id']] = job
        exec_mutex.release()
        self.schedule_map_tasks(job)


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
    t4 = threading.Thread(target=Master.update_dependencies, args=[masterProcess])

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()


if __name__ == "__main__":
    main()
