import random

from HW1.util.job import Job
from HW1.util.time_range import TimeRange

RUNNING = 0  # Currently executing on the processor
READY = 1  # Ready to run but task of higher or equal priority is currently running
BLOCKED = 2  # Task is waiting for some condition to be met to move to READY state
SUSPENDED = 3  # Task is waiting for some other task to unsuspend

INTERRUPT = 0  # Task type is interrupt
PERIODIC = 1  # Task type is periodic
APERIODIC = 2  # Task type is aperiodic
SPORADIC = 3  # Task type is sporadic


class Task(object):
    """Task class

    Attributes:
        priority (int): Priority of the task
        name (str): Name of the task
        state (int): Current state of the task
        task_type (int): Type of the task
        act_time (int): Activation time of the task
        period (int): Period of the task
        wcet (int): Worst case execution time of the task
        deadline (int): Deadline of the task
        job_history (list[Job]): List of jobs that have run
        job (Job): Current job that is running
        feasible (bool): If the task is feasible
    """

    def __init__(self,
                 priority: int,
                 name: str,
                 state: int,
                 task_type: int,
                 act_time: int,
                 period: int,
                 wcet: int,
                 deadline: int):
        self.priority = priority
        self.name = name
        self.state = state
        self.task_type = task_type
        self.act_time = act_time
        self.period = period
        self.wcet = wcet
        self.deadline = deadline
        self.color = (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        self.color = tuple([x / 255 for x in self.color])

        self.job_history = []
        self.job = None
        self.feasible = True

    def change_state(self, state: int):
        self.state = state

    def change_priority(self, priority: int):
        self.priority = priority

    def update_job(self, time):
        if self.state == RUNNING:
            self.job.run_time += 1
            if self.job and self.job.run_time == self.wcet:
                self.kill_job(time)
                is_preemptive = True

    def stop_job(self, time):
        if self.job:
            if len(self.job.time_ranges) == 0:
                self.job.time_ranges.append(TimeRange(time, time))
            elif self.state == RUNNING:
                self.job.time_ranges[-1].end = time
            self.state = READY

    def run_job(self, time):
        if self.job and self.state == READY:
            self.state = RUNNING
            self.job.time_ranges.append(TimeRange(time))

    def kill_job(self, time):
        if self.job:

            if self.job.run_time < self.wcet:
                self.feasible = False

            self.stop_job(time)
            self.job_history.append(self.job)
            self.job = None

    def update_task(self, time):

        is_preemptive = False

        if self.state == BLOCKED or self.state == SUSPENDED:
            return False

        if self.state == RUNNING:
            self.job.run_time += 1
            if self.job and self.job.run_time == self.wcet:
                self.kill_job(time)
                is_preemptive = True

        if self.task_type == PERIODIC and (time - self.act_time) % self.period == 0:
            self.kill_job(time)
            self.job = Job(act_time=time, deadline=self.deadline)
            return True

        if time == self.act_time:
            self.kill_job(time)
            self.job = Job(act_time=time, deadline=self.deadline)
            return True

        if self.job and self.job.get_absolute_deadline() <= time:
            self.kill_job(time)
            return True


        return is_preemptive

    def get_history(self, max_time):
        history = []
        for job in self.job_history:

            if job.run_time < self.wcet and job.time_ranges[-1].end != max_time:
                history.append(
                    (job.time_ranges[-1].end, job.time_ranges[-1].end,
                     "Abort {}, Remaining Execution Time is {}".format(self.name, self.wcet - job.run_time)
                     )
                )
            if job.run_time > 0:
                for t in job.time_ranges:
                    history.append((t.start, t.end, self.name))

        return history + [(t.start, t.end, self.name) for t in self.job.time_ranges] if self.job else history

    def get_run_time(self):
        return sum([job.run_time for job in self.job_history])
