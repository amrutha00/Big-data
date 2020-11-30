from datetime import datetime
import statistics
import sys



n = int(sys.argv[1])

task_completion_times = []

d = dict()

for i in range(1, n + 1):
	file = open("worker" + str(i) + ".log", "r")
	for line in file.readlines():
		temp = line.split(";")
		# print(temp)
		if (temp[2].split()[0] == "Started"):
			d[temp[2].split()[2]] = datetime.strptime(temp[3][:-2],'%Y-%m-%d %H:%M:%S,%f')
		else:
			time_elapsed = datetime.strptime(temp[3][:-2], '%Y-%m-%d %H:%M:%S,%f') - d[temp[2].split()[2]]
			task_completion_times.append(str(time_elapsed).split(":")[-1])
	file.close()

task_completion_times = [float(i) for i in task_completion_times]
print("Mean:", float(sum(task_completion_times))/len(task_completion_times), "Median:", statistics.median(task_completion_times))

start_times = dict()
end_times = dict()
algos = dict()
per_algos = dict()

file = open("masterlog.log", "r")

lines = file.readlines()
file.close()

for line in lines:
	temp = line.split(";")
	a = temp[2].split()
	if (a[0] == 'Completed'):
		if (a[1] == 'task'):
			job_id = a[2][0]
			if (a[2][2] == 'R'):
				b = a[4] + " " + a[5]
				end_times[job_id] = datetime.strptime(b,'%Y-%m-%d %H:%M:%S.%f')
	elif (a[0] == 'Started'):
		job_id = a[-1]
		start_times[job_id] = datetime.strptime(temp[4][:-2],'%Y-%m-%d %H:%M:%S,%f')
		algos[job_id] = temp[3]


jobs = start_times.keys()
for job in start_times:
	if algos[job] not in per_algos:
		per_algos[algos[job]] = []
		per_algos[algos[job]].append((end_times[job] - start_times[job]).total_seconds())
	else:
		per_algos[algos[job]].append((end_times[job] - start_times[job]).total_seconds())

for algo in per_algos:
	print(algo, float(sum(per_algos[algo]))/len(per_algos[algo]), statistics.median(per_algos[algo]))


