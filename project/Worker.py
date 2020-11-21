#!/usr/bin/python

import threading
import time
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
    
   
    def listen_master(self): #the worker acts like the server
        """
            This function listens for tasks to do.

        """
        #rec_port=self.port
        recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind(('',self.port))#listens on the specified port
        recv_socket.listen(1)
	
        while True:
            conn_socket, addr = recv_socket.accept()
            task = conn_socket.recv(2048).decode() #recv the msg for task launch by master
            self.execution_pool(task) #add task to the execution pool
            #self.logs('LAUNCHING',task,time.time()) #check if wall clock or user time???
            recv_socket.send("task received".encode()) #Is this necessary?
            recv_socket.close()
        
        
    def update_master(self,task=None): #msg is the task_id
        #server_port=self.server_port
        
        recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind(('localhost',self.port))
        recv_socket.connect(('localhost',self.server_port))#connects to the server

        if task:
            #exec_time=get the task_completion time from log file
            message=[self.worker_id,task,exec_time]
            message=json.dumps(message)
            recv_socket.send(message.encode()) #send the task_id of the task that is completed
            recv_socket.close()
        

    
    def execution_pool(self,task): #adds it to execution pool,with task_id and remaining time
        # task=task.decode() #We've already done this in listen_master
        task=json.loads(task)
        # (task_id,remain_time)=list(task.items())[0] #I didnt understand this, so i added the following two lines
        task_id = task['task_id']
        remain_time = task['duration']
        self.pool[task_id]=remain_time
        logging.debug('Started task {}'.format(task_id))


    def task_monitor(self):
        """
            With a time interval of one second, this function 
            updates the remaining times of the tasks in the execution pool.
            If any of the tasks has been completed, it updates the master about it
        """
        while True:
            for task_id in self.pool.keys():
                self.pool[task_id]-=1    #reduce time by 1 unit every clock cycle --sleep the thread for 1s

                if self.pool[task_id]==0: #the task execution is completed
                    #self.logs('COMPLETED',task_id,time.time())
                    logging.debug('Completed task {}'.format(task_id))
                    self.update_master(task_id) #t2 must update the master about the status of the task completion
                    self.pool.pop(task_id) #remove the task from the execution pool
            time.sleep(1) #every one second decrement the time of all the tasks

   

if __name__ == "__main__": 

    logging.basicConfig(filename="yacs.log", level=logging.DEBUG,
    format='%(filename)s:%(funcName)s:%(message)s:%(asctime)s')

    port=int(sys.argv[1])
    worker_id=int(sys.argv[2])
    worker=Worker(port,worker_id)

    t1 = threading.Thread(target=worker.listen_master) 
    t2 = threading.Thread(target=worker.task_monitor) 
  
    t1.start() 
    t2.start() 
    t1.join() 
    t2.join() 
  




