#!/usr/bin/python

import threading
import time
import socket
import datetime



class Worker:
    def __init__(self,port,worker_id):
        self.port=port
        self.worker_id=worker_id
        self.pool=dict()
        self.server_port=5001
    '''
    def connection(self,lock):
        lock.acquire() 
        rec_port=self.port
        server_port=self.server_port
		recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind('',rec_port())#listens on the specified port
		recv_socket.connect(('localhost',server_port))#connects to the server
	'''

    
    def listen_master(self):#acts as the server listening and servicing requests
        rec_port=self.port
       	recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        recv_socket.bind('',rec_port())#listens on the specified port
        recv_socket.listen(1)
	while True:
		conn_socket, addr = recv_socket.accept()
		task = conn_socket.recv(2048).decode() #recv the msg for task launch by master
		self.execution_pool(task) #add task to the execution pool
            #self.logs('LAUNCHING',task,time.time()) #check if wall clock or user time???
		recv_socket.close()
        
        
    def update_master(self,message=None):#client--sending msgs to server
        server_port=self.server_port
	recv_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
	recv_socket.connect(('localhost',server_port))#connects to the server
	if message:
            recv_socket.send(message.encode()) #send the task_id of the task that is completed
            recv_socket.close()

    
    def execution_pool(self,task): #adds it to execution pool,with task_id and remaining time
        (task_id,remain_time)=list(task.items())[0]
        self.pool[task_id]=remain_time


    def task_monitor(self):
        for task_id in self.pool.keys():
            self.pool[task_id]-=1       #reduce time by 1 unit every clock cycle --sleep the thread for 1s
            if self.pool[task_id]==0: #the task execution is completed
                #self.logs('COMPLETED',task_id,time.time())
                self.update_master([self.worker_id,task_id]) #t2 must update the master about the status of the task completion
                self.pool.pop(task_id) #remove the task from the execution pool
    



if __name__ == "__main__": 
    # creating thread 
    port=int(sys.argv[1])
    worker_id=int(sys.argv[2])
    worker=Worker(port,worker_id)
    #lock=threading.Lock()
    t1 = threading.Thread(target=worker.listen_master) 
    t2 = threading.Thread(target=worker.update_master) 
  
    t1.start() 
    t2.start() 
    t1.join() 
    t2.join() 
  




