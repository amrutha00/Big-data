import threading
import time
import socket
import json
import random

class Master:
	def __init__(self, algo):
		"""
			Initialises the job_pool, wait_queue lists and the workers dictionary
		"""
		self.job_pool = []
		self.wait_queue = []
		self.workers = dict()
		self.algo = algo

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
			This function reads the json text, creates a dictionary for each worker. \
			Each worker is indexed by its id in the self.workers dicitonary
		"""
		content = json.loads(config)
		for worker in content['workers']:
			self.workers[worker['worker_id']] = dict()
			self.workers[worker['worker_id']]['slots'] = worker['slots']
			self.workers[worker['worker_id']]['port'] = worker['port']

	def get_available_workers(self):
		"""
			returns a list of worker ids which have at least one slot available
		"""
		raise NotImplementedError

	def decrement_slot(self, worker_id):
		"""
			Given the worker id, update its number of available slots
		"""
		raise NotImplementedError

	def find_worker(self):
		"""
			Finds a worker based on the algorithm provided as an argument
		"""
		if (self.algo == 'random'):
			available_workers = self.get_available_workers()
			choice = random.choice(available_workers)
			return choice
		elif (self.algo == 'round-robin'):
			raise NotImplementedError
		else:
			raise NotImplementedError

	def schedule_task(self, task):
		"""
			Schedules the given task to one of the workers
		"""
		worker_id = self.find_worker()
		port_number = self.workers[worker_id]['port']
		masterSocket = socket(AF_INET, SOCK_DGRAM)
		to_name = "localhost"
		masterSocket.sendto(task.encode(), (to_name, port_number))
		masterSocket.close()
		self.decrement_slot(worker_id)

	def dependency_wait(self, job):
		"""
			This function waits till all map tasks of the job have been finished
			I think this function only listens for updates from the workers
		"""
		raise NotImplementedError

	def schedule_all_tasks(self, job):
		"""
			Schedules map and reduce tasks of the given job
		"""
		for task in job["map_tasks"]:
			self.schedule_task(task)
		self.dependency_wait(job)
		for task in job["reduce_tasks"]:
			self.schedule_task(task)
		# do something to listen for updates regarding reducers

	def listen_for_job_requests(self):
		"""
			This function listens for job requests and
			adds the jobs received to the wait queue
		"""
		raise NotImplementedError

	def schedule_jobs(self):
		"""
			Dequeues jobs from the wait queue and calls schedule_all_tasks for each job
			Doubt: Idk how often this should dequeue from the wait queue
			Should there be a function to check for unused slots? We need to discuss this
		"""
		raise NotImplementedError













