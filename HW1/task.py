from HW1.util.job import Job
from HW1.util.time_range import time_range

RUNNING = 0  # Currently executing on the processor
READY = 1  # Ready to run but task of higher or equal priority is currently running
BLOCKED = 2  # Task is waiting for some condition to be met to move to READY state
SUSPENDED = 3  # Task is waiting for some other task to unsuspend

INTERRUPT = 0  # Task type is interrupt
PERIODIC = 1  # Task type is periodic
APERIODIC = 2  # Task type is aperiodic
SPORADIC = 3  # Task type is sporadic


class Task(object):
    """Task Object Class

    Attributes:
        priority (int): Priority of the task
        name (str): Name of the task
        state (int): State of the task
        task_type (int): Type of the task
        act_time (int): Activation time of the task
        period (int): Period of the task
        wcet (int): Worst case execution time of the task
        deadline (int): Deadline of the task
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

        self.job_history = []
        self.job = None
        self.feasible = True

    def change_state(self, state: int):
        self.state = state

    def change_priority(self, priority: int):
        self.priority = priority

    def task_job_runtime(self, time):

        is_preemptive = False

        if self.state == BLOCKED or self.state == SUSPENDED:
            return False

        if self.state == RUNNING:
            self.job.run_time += 1
            if self.job and self.job.run_time == self.wcet:
                self.__kill_job(time)
                is_preemptive = True

        if self.task_type == PERIODIC and (time - self.act_time) % self.period == 0:
            self.__kill_job(time)
            self.job = Job(act_time=time, deadline=self.deadline)
            return True

        if time == self.act_time:
            self.__kill_job(time)
            self.job = Job(act_time=time, deadline=self.deadline)
            return True

        if self.job and self.job.get_absolute_deadline() <= time:
            self.__kill_job(time)
            return True

        return is_preemptive

    def __kill_job(self, time):
        if self.job:

            if self.job.run_time < self.wcet:
                self.feasible = False

            self.stop_job(time)
            self.job_history.append(self.job)
            self.job = None

    def stop_job(self, time):
        if self.job:
            if len(self.job.time_ranges) == 0:
                self.job.time_ranges.append(time_range(time, time))
            elif self.state == RUNNING:
                self.job.time_ranges[-1].finish = time
            self.state = READY

    def run_job(self, time):
        if self.job and self.state == READY:
            self.state = RUNNING
            self.job.time_ranges.append(time_range(time))

    def get_history(self, max_time):
        history = []
        for job in self.job_history:

            if job.run_time > 0:
                for t in job.time_ranges:
                    history.append((t.start, t.finish, self.name))

            if job.run_time < self.wcet and job.time_ranges[-1].finish != max_time:
                history.append(
                    (job.time_ranges[-1].finish, job.time_ranges[-1].finish,
                     "Abort {}, Remaining Execution Time is {}".format(self.name, self.wcet - job.run_time)
                     )
                )

        if self.job:
            for t in self.job.time_ranges:
                history.append((t.start, t.finish, self.name))
        return history

    def get_run_time(self):
        run_time = 0
        for job in self.job_history:
            run_time += job.run_time
        return run_time
