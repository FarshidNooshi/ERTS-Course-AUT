#!/usr/bin/env python

"""
taskSet.py - parser for task set from JSON file
"""

import json
from threading import Thread, Lock
from time import sleep

import numpy as np
from matplotlib import pyplot as plt


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


class TaskSet(object):
    def __init__(self, data):
        self.jobs = None
        self.tasks = None
        self.parseDataToTasks(data)
        self.buildJobReleases(data)

    def parseDataToTasks(self, data):
        taskSet = {}

        for taskData in data[TaskSetJsonKeys.KEY_TASKSET]:
            task = Task(taskData)

            if task.id in taskSet:
                print("Error: duplicate task ID: {0}".format(task.id))
                return

            if task.period < 0 and task.relativeDeadline < 0:
                print("Error: aperiodic task must have a positive relative deadline")
                return

            taskSet[task.id] = task

        self.tasks = taskSet

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
            for job in task.getJobs():
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
            for job in task.getJobs():
                release_time = job.releaseTime
                deadline = job.deadline
                execution_time = task.sections[0][1]  # Assuming all sections have the same execution time
                ax.broken_barh([(release_time, execution_time)], (task.id - 0.4, 0.8), facecolors="tab:blue")
                ax.plot([deadline, deadline], [task.id - 0.2, task.id + 0.2], color="red")

        plt.show()


class Task(object):
    def __init__(self, taskDict):
        self.id = int(taskDict[TaskSetJsonKeys.KEY_TASK_ID])
        self.period = float(taskDict[TaskSetJsonKeys.KEY_TASK_PERIOD])
        self.wcet = float(taskDict[TaskSetJsonKeys.KEY_TASK_WCET])
        self.relativeDeadline = float(
            taskDict.get(TaskSetJsonKeys.KEY_TASK_DEADLINE, taskDict[TaskSetJsonKeys.KEY_TASK_PERIOD]))
        self.offset = float(taskDict.get(TaskSetJsonKeys.KEY_TASK_OFFSET, 0.0))
        self.sections = taskDict[TaskSetJsonKeys.KEY_TASK_SECTIONS]

        self.lastJobId = 0
        self.lastReleasedTime = 0.0

        self.jobs = []
        self.locks = []  # Locks for synchronization

        for _ in range(len(self.sections)):
            self.locks.append(Lock())

    def getAllResources(self):
        resources = set()
        for section in self.sections:
            semaphoreId = section[0]
            resources.add(semaphoreId)
        return resources

    def spawnJob(self, releaseTime):
        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime:
            print("INVALID: release time of job is not monotonic")
            return None

        if self.lastReleasedTime > 0 and releaseTime < self.lastReleasedTime + self.period:
            print("INVALID: release times are not separated by period")
            return None

        self.lastJobId += 1
        self.lastReleasedTime = releaseTime

        job = Job(self, self.lastJobId, releaseTime)

        self.jobs.append(job)
        return job

    def getJobs(self):
        return self.jobs

    def getJobById(self, jobId):
        if jobId > self.lastJobId:
            return None

        job = self.jobs[jobId - 1]
        if job.id == jobId:
            return job

        for job in self.jobs:
            if job.id == jobId:
                return job

        return None

    def getUtilization(self):
        return self.wcet / self.period

    def processSection(self, sectionIndex):
        semaphoreId, sectionTime = self.sections[sectionIndex]

        # Acquire the semaphore lock
        semaphore = self.locks[semaphoreId]
        semaphore.acquire()

        # Process the section
        # TODO: Implement section processing logic

        # Release the semaphore lock
        semaphore.release()

    def __str__(self):
        return "task {0}: (Φ,T,C,D,∆) = ({1}, {2}, {3}, {4}, {5})".format(
            self.id, self.offset, self.period, self.wcet, self.relativeDeadline, self.sections)


class Job(object):
    def __init__(self, task, jobId, releaseTime):
        self.task = task
        self.id = jobId
        self.releaseTime = releaseTime
        self.deadline = self.releaseTime + self.task.relativeDeadline
        self.currentSectionIndex = 0
        self.remainingSectionTime = self.task.sections[self.currentSectionIndex][1]

    def getResourceHeld(self):
        resources = set()
        for sectionIndex in range(self.currentSectionIndex + 1):
            semaphoreId = self.task.sections[sectionIndex][0]
            resources.add(semaphoreId)
        return resources

    def getResourceWaiting(self):
        resources = set()
        for sectionIndex in range(self.currentSectionIndex + 1, len(self.task.sections)):
            semaphoreId = self.task.sections[sectionIndex][0]
            resources.add(semaphoreId)
        return resources

    def execute(self, time):
        self.remainingSectionTime -= time
        if self.remainingSectionTime <= 0:
            self.currentSectionIndex += 1
            if self.currentSectionIndex < len(self.task.sections):
                self.remainingSectionTime = self.task.sections[self.currentSectionIndex][1]

    def executeToCompletion(self):
        totalExecutionTime = 0
        while not self.isCompleted():
            self.execute(1)
            totalExecutionTime += 1
        return totalExecutionTime

    def isCompleted(self):
        return self.currentSectionIndex >= len(self.task.sections)

    def __str__(self):
        return "[{0}:{1}] released at {2} -> deadline at {3}".format(
            self.task.id, self.id, self.releaseTime, self.deadline)


class JobQueue(object):
    def __init__(self):
        self.queue = []
        self.lock = Lock()

    def enqueue(self, job):
        with self.lock:
            self.queue.append(job)

    def dequeue(self):
        with self.lock:
            if len(self.queue) > 0:
                return self.queue.pop(0)
            else:
                return None

    def size(self):
        with self.lock:
            return len(self.queue)


class Scheduler(Thread):
    def __init__(self, taskSet, resourceAllocationProtocol="NPP"):
        super(Scheduler, self).__init__()
        self.taskSet = taskSet
        self.jobQueue = JobQueue()
        self.isRunning = False
        self.globalTime = 0.0
        self.resourceAllocationProtocol = resourceAllocationProtocol

    def startScheduling(self):
        self.isRunning = True
        self.start()

    def stopScheduling(self):
        self.isRunning = False
        self.join()

    def run(self):
        while self.isRunning:
            # Schedule jobs here based on the selected scheduling algorithm
            job = self.jobQueue.dequeue()
            if job:
                self.scheduleJob(job)
            sleep(5)

    def addJob(self, job):
        self.jobQueue.enqueue(job)

    def scheduleJob(self, job):
        # Determine the earliest deadline job in the queue
        earliestDeadlineJob = None
        releasedJobs = [j for j in self.jobQueue.queue if j.releaseTime <= self.globalTime]
        for queuedJob in releasedJobs:
            if earliestDeadlineJob is None or queuedJob.deadline < earliestDeadlineJob.deadline:
                earliestDeadlineJob = queuedJob

        if earliestDeadlineJob is None or job.deadline < earliestDeadlineJob.deadline:
            # Execute the job immediately
            self.globalTime += job.executeToCompletion()
        else:
            # Determine the available resources for the job
            heldResources = job.getResourceHeld()
            waitingResources = job.getResourceWaiting()

            # HLP: Check if any queued job holds a resource that the current job is waiting for
            if self.resourceAllocationProtocol == "HLP":
                for queuedJob in releasedJobs:
                    queuedJobHeldResources = queuedJob.getResourceHeld()
                    if waitingResources.intersection(queuedJobHeldResources):
                        # Wait for the queued job to finish
                        while not queuedJob.isCompleted():
                            pass  # Idle loop until the queued job completes

                        # Execute the job
                        self.globalTime += job.executeToCompletion()
                        return

            # NPP: Check if the current job holds a resource that any queued job is waiting for
            elif self.resourceAllocationProtocol == "NPP":
                for queuedJob in releasedJobs:
                    queuedJobWaitingResources = queuedJob.getResourceWaiting()
                    if heldResources.intersection(queuedJobWaitingResources):
                        # Wait for the queued job to finish
                        while not queuedJob.isCompleted():
                            pass  # Idle loop until the queued job completes

                        # Execute the job
                        self.globalTime += job.executeToCompletion()
                        return
            else:
                raise Exception("Invalid resource allocation protocol")

            # No resource conflicts, execute the job immediately
            self.globalTime += job.executeToCompletion()


def main():
    JSON_FILE = "data/json/NPP-taskset.json"
    # Load task set from JSON file
    with open(JSON_FILE) as file:
        data = json.load(file)

    # Create task set
    taskSet = TaskSet(data)

    # Create scheduler and start scheduling
    scheduler = Scheduler(taskSet, resourceAllocationProtocol="NPP")

    # Start scheduling (run the thread)
    scheduler.startScheduling()

    # Add jobs to the scheduler
    for task in taskSet:
        for job in task.getJobs():
            scheduler.addJob(job)

    # Stop scheduling
    scheduler.stopScheduling()

    # Print task set information
    taskSet.printTasks()
    taskSet.printJobs()
    taskSet.generate_gantt_chart()


if __name__ == "__main__":
    main()
