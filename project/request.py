import socket
import json

job = dict()
job['job_id'] = 10
job['map_tasks'] = list()
maptask1 = dict()
maptask1['task_id'] = 1
maptask1['duration'] = 10
job['map_tasks'].append(maptask1)
maptask2 = dict()
maptask2['task_id'] = 2
maptask2['duration'] = 10
job['map_tasks'].append(maptask2)

redtask = dict()
redtask['task_id'] = 3
redtask['duration'] = 10
job['reduce_tasks'] = list()
job['reduce_tasks'].append(redtask)

# print(json.dumps(job))

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
	s.connect(('localhost', 5000))
	message = json.dumps(job)
	s.send(message.encode())
