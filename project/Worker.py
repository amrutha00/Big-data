#!/usr/bin/python

import threading
import time
import socket
import datetime
import sys
import json



class Worker:
    def __init__(self,port,worker_id):
        self.port=port
        self.worker_id=worker_id
        self.pool=dict()
        self.server_port=5001
    
   
    def listen_master(self): #the worker acts like the server
        
        #rec_port=self.port
        recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind(('',self.port))#listens on the specified port
        recv_socket.listen(1)
	
        while True:
            conn_socket, addr = recv_socket.accept()
            task = conn_socket.recv(2048).decode() #recv the msg for task launch by master
            self.execution_pool(task) #add task to the execution pool
            #self.logs('LAUNCHING',task,time.time()) #check if wall clock or user time???
            recv_socket.send("task received")
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
        task=task.decode()
        task=json.loads(task)
        (task_id,remain_time)=list(task.items())[0]
        self.pool[task_id]=remain_time


    def task_monitor(self):
        while True:
            for task_id in self.pool.keys():
                self.pool[task_id]-=1    #reduce time by 1 unit every clock cycle --sleep the thread for 1s

                if self.pool[task_id]==0: #the task execution is completed
                    #self.logs('COMPLETED',task_id,time.time())
                    self.update_master(task_id) #t2 must update the master about the status of the task completion
                    self.pool.pop(task_id) #remove the task from the execution pool
            time.sleep(1) #every one second decrement the time of all the tasks

    '''
    def getdate(self):
        d=datetime.date.today()
        curr_date = today.strftime("%d:%m:%Y")
        return curr_date

    def logs(self,msg,t,ptime):
        #check if wall clock or user time???
        f = open("logs.txt", "a+") 
        if msg=='LAUNCHING':
            date=self.getdate()
            f.write(task_id," ",msg," ",date," "ptime)
            
        else:
            date=self.getdate()
            f.write(task_id," ",msg," ",date," "ptime)
        
        f.close()
    '''      



if __name__ == "__main__": 
    # creating thread 
    port=int(sys.argv[1])
    worker_id=int(sys.argv[2])
    worker=Worker(port,worker_id)

    t1 = threading.Thread(target=worker.listen_master) 
    t2 = threading.Thread(target=worker.task_monitor) 
  
    t1.start() 
    t2.start() 
    t1.join() 
    t2.join() 
  




