#!/usr/bin/env python

"""
taskSet.py - parser for task set from JSON file
"""

import json
import random
from time import sleep

import numpy as np
from matplotlib import pyplot as plt

READY = 0
RUNNING = 1
BLOCKED = 2
FINISHED = 3
MISSED = 4

NP_PRIORITY = -2
NPP_PRIORITY = -1

NPP = 0
HLP = 1
PIP = 2

RM = 0
DM = 1
EDF = 2


def plot_tasks(scheduler):
    tasks = scheduler.task_set.tasks
    tasks = {k: v for k, v in reversed(sorted(tasks.items(), key=lambda item: item[1].period))}
    fig, ax = plt.subplots()
    y_label = np.array(list(tasks.keys()))[::-1]
    c_width = y_label.size / (y_label.size + 1)
    color_map = scheduler.task_set.all_resources.copy()
    color_map.pop(0)
    for k in color_map.keys():
        color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        color_map[k] = tuple([x / 255 for x in color])
    for task_id in y_label:
        color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        color_map[f"task {task_id}"] = tuple([x / 255 for x in color])

    ax.set_ylim(0, y_label.size)
    ax.set_xlim(scheduler.start_time, scheduler.end_time)
    ax.set_xlabel('Time')
    ax.set_ylabel('Tasks')
    ax.set_yticks([(i + 1) * c_width for i in range(y_label.size)])
    ax.set_yticklabels([f"task {i}" for i in y_label])

    task_intervals = {}
    for interval in scheduler.intervals:
        x_range = (interval.start, interval.end - interval.start)
        dic = task_intervals.setdefault(interval.job.task.id, {})
        ranges = dic.setdefault(interval.semaphore_id, [])
        ranges.append(x_range)

    for i in range(y_label.size):
        dic = task_intervals.get(y_label[i])
        if dic:
            for s, r in dic.items():
                ax.broken_barh(r, ((i + 0.75) * c_width, c_width / 2),
                               facecolors=color_map[s] if s else color_map[f"task {y_label[i]}"])

    ax.set_title(
        'Task set with scheduler: ERTS HW3 and policy: {0}'.format(scheduler.resource_access_protocol))

    plt.show()
    plt.savefig(f"doc/result/task_set_{scheduler.resource_access_protocol}.png")


class TaskSetJsonKeys(object):
    # Task set
    KEY_TASKSET = "taskset"

    # Task
    KEY_TASK_ID = "taskId"
    KEY_TASK_PERIOD = "period"
    KEY_TASK_WCET = "wcet"
    KEY_TASK_DEADLINE = "deadline"
    KEY_TASK_OFFSET = "offset"
    KEY_TASK_SECTIONS = "sections"

    # Schedule
    KEY_SCHEDULE_START = "startTime"
    KEY_SCHEDULE_END = "endTime"

    # Release times
    KEY_RELEASETIMES = "releaseTimes"
    KEY_RELEASETIMES_JOBRELEASE = "timeInstant"
    KEY_RELEASETIMES_TASKID = "taskId"


class TaskSetIterator:
    def __init__(self, taskSet):
        self.taskSet = taskSet
        self.index = 0
        self.keys = iter(taskSet.tasks)

    def __next__(self):
        key = next(self.keys)
        return self.taskSet.tasks[key]


class TaskInstanceInterval:
    def __init__(self, job, start, end=-1):
        self.job = job
        self.semaphore_id = job.get_resource_held()
        if self.semaphore_id is None:
            self.semaphore_id = job.get_resource_waiting()
        self.start = start
        self.end = end

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"interval [{self.start}, {self.end}) task {self.job.task.id} job {self.job.id} semaphore {self.semaphore_id}"


class TaskSetDefinition(object):
    def __init__(self, data):
        self.jobs = None
        self.tasks = {}
        self.all_resources = {}
        self.parse_data_to_tasks(data)

    def parse_data_to_tasks(self, data):

        for taskData in data[TaskSetJsonKeys.KEY_TASKSET]:
            task = Task(taskData)

            self.all_resources.update(task.get_all_task_resources())

            if task.id in self.tasks:
                print("Error: duplicate task ID: {0}".format(task.id))
                return

            if task.period < 0 and task.deadline < 0:
                print("Error: aperiodic task must have positive relative deadline")
                return

            self.tasks[task.id] = task

    def __contains__(self, elt):
        return elt in self.tasks

    def __iter__(self):
        return iter(self.tasks.values())

    def __len__(self):
        return len(self.tasks)

    def get_task_by_id(self, taskId):
        return self.tasks[taskId]

    def print_tasks(self):
        print("Task Set:")
        for task in self:
            print(task)

    def buildJobReleases(self, data):
        jobs = []

        if TaskSetJsonKeys.KEY_RELEASETIMES in data:  # necessary for sporadic releases
            for jobRelease in data[TaskSetJsonKeys.KEY_RELEASETIMES]:
                releaseTime = float(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_JOBRELEASE])
                taskId = int(jobRelease[TaskSetJsonKeys.KEY_RELEASETIMES_TASKID])

                job = self.getTaskById(taskId).spawnJob(releaseTime)
                jobs.append(job)
        else:
            scheduleStartTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
            scheduleEndTime = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])
            for task in self:
                t = max(task.offset, scheduleStartTime)
                while t < scheduleEndTime:
                    job = task.spawnJob(t)
                    if job is not None:
                        jobs.append(job)

                    if task.period >= 0:
                        t += task.period  # periodic
                    else:
                        t = scheduleEndTime  # aperiodic

        self.jobs = jobs

    def __contains__(self, elt):
        return elt in self.tasks

    def __iter__(self):
        return TaskSetIterator(self)

    def __len__(self):
        return len(self.tasks)

    def getTaskById(self, taskId):
        return self.tasks[taskId]

    def printTasks(self):
        print("\nTask Set:")
        for task in self:
            print(task)

    def printJobs(self):
        print("\nJobs:")
        for task in self:
            for job in task.get_task_instance_to_run():
                print(job)

    def generate_gantt_chart(self):
        fig, ax = plt.subplots()
        ax.set_xlabel("Time")
        ax.set_ylabel("Tasks")
        ax.set_yticks([(i + 1) for i in range(len(self.tasks))])
        ax.set_yticklabels([f"Task {task.id}" for (id, task) in self.tasks.items()])
        ax.grid(True, which='both', axis='x', linestyle='--', linewidth=1)
        # set the x-splitter to be the 5
        ax.set_xticks(np.arange(0, 100, 5))

        for (id, task) in self.tasks.items():  # Plotting the jobs
            for job in task.get_task_instance_to_run():
                release_time = job.releaseTime
                deadline = job.deadline
                execution_time = task.sections[0][1]  # Assuming all sections have the same execution time
                ax.broken_barh([(release_time, execution_time)], (task.id - 0.4, 0.8), facecolors="tab:blue")
                ax.plot([deadline, deadline], [task.id - 0.2, task.id + 0.2], color="red")

        plt.show()


class Task(object):
    def __init__(self, task_dict):
        self.id = int(task_dict[TaskSetJsonKeys.KEY_TASK_ID])
        self.period = float(task_dict[TaskSetJsonKeys.KEY_TASK_PERIOD])
        self.wcet = float(task_dict[TaskSetJsonKeys.KEY_TASK_WCET])
        self.deadline = float(task_dict.get(TaskSetJsonKeys.KEY_TASK_DEADLINE, self.period))
        self.offset = float(task_dict.get(TaskSetJsonKeys.KEY_TASK_OFFSET, 0.0))
        self.sections = task_dict[TaskSetJsonKeys.KEY_TASK_SECTIONS]

        self.job_count = 0
        self.spawn = self.offset

    def get_all_task_resources(self):
        resources = {}
        for section in self.sections:
            if section[0] != 0:
                resources[section[0]] = None
        return resources

    def spawn_job(self, time):
        if time != self.spawn:
            return None

        self.job_count += 1
        self.spawn += self.period
        return TaskInstance(self, self.job_count, time)

    def __str__(self):
        return f"task {self.id}: (Φ,T,C,D,∆) = " \
               f"({self.offset}, {self.period}, {self.wcet}, {self.deadline}, {self.sections}))"


class TaskInstance(object):

    def __init__(self, task, jobId, arrive):
        self.task = task
        self.id = jobId
        self.arrive = arrive
        self.deadline = task.deadline + arrive
        self.state = READY

        self.total_runtime = 0

        self.it = iter(task.sections)
        section = next(self.it, None)
        self.semaphore_id = section[0]
        self.section_time = section[1]
        self.section_runtime = 0

        self.original_priority = None
        self.priority = None

    def get_resource_held(self):
        if self.semaphore_id != 0 and self.section_runtime != 0:
            return self.semaphore_id
        return None

    def get_resource_waiting(self):
        if self.semaphore_id != 0 and self.section_runtime == 0:
            return self.semaphore_id
        return None

    def get_remaining_section_time(self):
        return self.section_time - self.section_runtime

    def execute(self, resources: dict):
        print(f"[{self.task.id}:{self.id}:{self.semaphore_id}] run {self.total_runtime}:{self.section_runtime}")
        if self.is_completed():
            return False

        if self.semaphore_id != 0 and resources.get(self.semaphore_id) != self:
            return True

        self.total_runtime += 1
        self.section_runtime += 1

        if self.get_remaining_section_time() == 0:
            self.section_runtime = 0
            resources[self.semaphore_id] = None
            if self.priority != NP_PRIORITY:
                self.priority = self.original_priority

            section = next(self.it, None)
            if section is None:
                return False
            self.semaphore_id = section[0]
            self.section_time = section[1]

        return False

    def execute_to_completion(self):
        return self.task.wcet - self.total_runtime

    def is_completed(self):
        return self.total_runtime == self.task.wcet

    def __str__(self):
        return f"[{self.task.id}:{self.id}] released at {self.arrive} -> deadline at {self.deadline}"


class TaskInstanceQueue(object):

    def __init__(self):
        self.running_set = set()
        self.finished_set = set()
        self.is_feasible = True

    def update_jobs(self, time, intervals):
        print(f"update_jobs {time}")
        for job in self.running_set.copy():
            if job.deadline <= time or job.is_completed():
                job.state = FINISHED if job.is_completed() else MISSED
                if job.state == MISSED:
                    self.is_feasible = False
                if job.is_completed():
                    intervals[-1].end = time
                self.running_set.remove(job)
                self.finished_set.add(job)

    def add_job(self, job):
        print(f"add_job {job}")
        self.running_set.add(job)

    def get_total_runtime(self):
        total_runtime = 0
        for job in self.finished_set:
            total_runtime += job.total_runtime

        for job in self.running_set:
            total_runtime += job.total_runtime

        return total_runtime


class Scheduler(object):
    def __init__(self, data, algorithm=DM, resource_access_protocol=NPP, preemptive=True):
        self.task_set = TaskSetDefinition(data)
        self.job_queue = TaskInstanceQueue()
        self.intervals = []
        self.algorithm = algorithm
        self.resource_access_protocol = resource_access_protocol
        self.preemptive = preemptive
        self.timer = 0
        self.start_time = float(data[TaskSetJsonKeys.KEY_SCHEDULE_START])
        self.end_time = float(data[TaskSetJsonKeys.KEY_SCHEDULE_END])

    def print_interval(self):
        for interval in self.intervals:
            print(interval)

    def get_highest_priority_task_instance_resource(self, hp_job, sem_id):
        tasks = list(self.task_set.tasks.values())
        highest_priority = hp_job.original_priority

        for job in self.job_queue.running_set:
            tasks.remove(job.task)
            if sem_id in job.task.get_all_task_resources().keys():
                highest_priority = min(highest_priority, self.get_task_priority(job))

        for task in tasks:
            if sem_id in task.get_all_task_resources().keys():
                job = TaskInstance(task, -1, task.spawn)
                highest_priority = min(highest_priority, self.get_task_priority(job))

        return highest_priority

    def get_running_task_instance(self):
        if self.job_queue.running_set == set():
            return None

        running_jobs = list(filter(lambda job: job.state == RUNNING, self.job_queue.running_set))
        hp_running_job = list(filter(lambda job: job.priority != job.original_priority, running_jobs))

        if hp_running_job:
            return hp_running_job

        if running_jobs:
            return running_jobs[0]
        return None

    def get_task_instance_to_run(self) -> [TaskInstance, None]:
        if self.job_queue.running_set == set():
            return None

        running_job = self.get_running_task_instance()
        hp = min(self.job_queue.running_set, key=lambda job: job.priority).priority

        hpr_jobs = list(
            filter(lambda job: job.get_resource_held() is not None,
                   list(filter(lambda job: job.priority == hp, self.job_queue.running_set))))
        if hpr_jobs:
            hp_job = hpr_jobs[0]
        else:
            hp_job = list(filter(lambda job: job.priority == hp, self.job_queue.running_set))[0]

        if isinstance(running_job, TaskInstance):
            if running_job.priority == hp_job.priority:
                hp_job = running_job
            else:
                running_job.state = READY

        if not self.preemptive:
            hp_job.priority = NP_PRIORITY

        if running_job != hp_job:
            if self.intervals and self.intervals[-1].end == -1:
                self.intervals[-1].end = self.start_time + self.timer
            interval = TaskInstanceInterval(hp_job, self.start_time + self.timer)
            self.intervals.append(interval)
        elif hp_job.section_runtime == 0 and self.intervals[-1].end == -1:
            self.intervals[-1].end = self.start_time + self.timer
            interval = TaskInstanceInterval(hp_job, self.start_time + self.timer)
            self.intervals.append(interval)

        hp_job.state = RUNNING
        return hp_job

    def get_task_priority(self, job):
        if self.algorithm == RM:
            return job.task.period
        elif self.algorithm == DM:
            return job.task.deadline
        elif self.algorithm == EDF:
            return job.deadline
        else:
            raise Exception("Invalid algorithm")

    def schedule_tasks(self):
        print("Scheduling tasks...")
        self.job_queue.update_jobs(self.start_time + self.timer, self.intervals)

        for task in self.task_set.tasks.values():
            print("Task: ", task.id, " has ", task.get_all_task_resources().keys(), " resources")
            job = task.spawn_job(self.start_time + self.timer)
            if job:
                print("Adding job: ", job.id, " to queue")
                job.original_priority = self.get_task_priority(job)
                job.priority = job.original_priority
                self.job_queue.add_job(job)

        hp_job = self.get_task_instance_to_run()

        if self.start_time + self.timer == self.end_time:
            print("End of schedule")
            if self.intervals and self.intervals[-1].end == -1:
                self.intervals[-1].end = self.start_time + self.timer
            return

        if not hp_job:
            return

        is_blocked = hp_job.execute(self.task_set.all_resources)

        if is_blocked:
            print("Job: ", hp_job.id, " is blocked")
            semaphore_id = hp_job.get_resource_waiting()
            print("Job: ", hp_job.id, " is waiting for resource: ", semaphore_id)
            self.check_condition(hp_job, semaphore_id)

    def check_condition(self, hp_job, semaphore_id):
        if semaphore_id and self.task_set.all_resources[semaphore_id] is None:
            print("Resource: ", semaphore_id, " is free")
            self.task_set.all_resources[semaphore_id] = hp_job
            hp_job.execute(self.task_set.all_resources)
            if self.resource_access_protocol == NPP:
                hp_job.priority = NPP_PRIORITY
                hp_job.priority = self.get_highest_priority_task_instance_resource(hp_job, semaphore_id)

    def get_utilization(self):
        print("Calculating utilization...")
        return self.job_queue.get_total_runtime() / (self.end_time - self.start_time)


def main():
    file_path = "data/json/HLP_taskset.json"

    with open(file_path) as json_data:
        data = json.load(json_data)

    scheduler = Scheduler(data, resource_access_protocol=HLP)
    print("Scheduler: ", scheduler.algorithm, " ", scheduler.resource_access_protocol)

    while scheduler.start_time + scheduler.timer <= scheduler.end_time:
        scheduler.schedule_tasks()
        scheduler.timer += 1
        sleep(0.1)

    scheduler.print_interval()

    # Declaring a figure "gnt"
    plot_tasks(scheduler)
    print("Utilization: ", scheduler.get_utilization())


if __name__ == "__main__":
    main()
