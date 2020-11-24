#!/usr/bin/python

import threading
from time import sleep
import socket
import datetime
import sys
import json
import logging

class Worker:
    def __init__(self,port,worker_id):
        """
            Attributes:
                self.port: port number of the worker
                self.worker_id: worker_id of the worker
                self.pool: dictionary containing all the currently runnning tasks
                           key: task_id
                           value: remaining duration 
                self.server_port: port number to use when sending messages to the master
        """
        self.port=port
        self.worker_id=worker_id
        self.pool=dict()
        self.server_port=5001
    
   
    def listen_master(self):
        """
            This function listens for tasks to do.
            The worker acts like the server during the communication

        """
        recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind(('',self.port)) # listens on the specified port
        recv_socket.listen(5) 
        while True:
            conn_socket, addr = recv_socket.accept()
            task = conn_socket.recv(2048).decode() #recv the msg for task launch by master
            self.execution_pool(task) #add task to the execution pool
            conn_socket.close()
        
        
    def update_master(self,task=None):
        """
            This is called when a task is completed.
            In this case, the worker file acts as a client and sends the message to the master 
            indicating the completion of the given task
            message = [worker_id, task_id]
        """
        if (task):
            message1 = list((self.worker_id, task))
            message = json.dumps(message1) # Converting the list to string 
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(('localhost', self.server_port))
                s.send(message.encode())

    
    def execution_pool(self,task): 
        """
            This function adds the given task to the execution pool.
            key = task_id
            value = remaining time of the task (initialised to its duration)
        """
        task=json.loads(task)
        task_id = task['task_id']
        remain_time = task['duration']
        self.pool[task_id]=remain_time
        logging.debug('Started task {}'.format(task_id))


    def task_monitor(self):
        """
            This function updates the remaining times of the tasks in the execution pool.
            If any of the tasks has been completed, it updates the master about it.
            tempSet is used to get the set of keys in the execution pool at that instant.
        """
        tempList = []
        for task_id in self.pool.keys():
            self.pool[task_id]-=1    #reduce time by 1 unit every clock cycle --sleep the thread for 1s

            if self.pool[task_id]==0: #the task execution is completed
                logging.debug('Completed task {}'.format(task_id))
                self.update_master(task_id) #t2 must update the master about the status of the task completion
                tempList.append(task_id)
        for i in tempList:
            self.pool.pop(i)


    def clock(self):
        """
            Simulates a clock
        """
        while True:
            self.task_monitor()
            sleep(1)

if __name__ == "__main__": 

    logging.basicConfig(filename="yacs.log", level=logging.DEBUG,
    format='%(filename)s:%(funcName)s:%(message)s:%(asctime)s')

    port=int(sys.argv[1])
    worker_id=int(sys.argv[2])
    worker=Worker(port,worker_id)

    t1 = threading.Thread(target=worker.listen_master) 
    t2 = threading.Thread(target=worker.clock) 

  
    t1.start() 
    t2.start() 
    t1.join() 
    t2.join() 
  




