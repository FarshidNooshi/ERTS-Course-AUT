from HW1.task import Task, RUNNING, PERIODIC
from HW1.task_set import TaskSet
from HW1.util.enum import SCHEDULING_ALG_RM, SCHEDULING_ALG_DM, SCHEDULING_ALG_EDF
from HW1.util.time_range import TimeRange
from matplotlib import pyplot as plt


class Scheduler:
    """Scheduler Class

    Attributes:
        task_set (TaskSet): Task set to be scheduled
    """

    def __init__(self, task_set: TaskSet, max_time: int, algorithm, preemptive):
        self.task_set = task_set
        self.time = 0
        self.max_time = max_time
        self.preemptive = preemptive
        self.algorithm = algorithm
        self.completed_tasks = []
        self.free_times = []
        self.free_counter = 0

    def reset(self):
        self.time = 0

    def generate_parameters(self):
        run_time = 0
        feasible = True
        for task in self.task_set.get_all_tasks():
            self.completed_tasks.extend(task.get_history(self.max_time))
            feasible = task.feasible and feasible
            run_time += task.get_run_time()

        for t in self.free_times:
            self.completed_tasks.append((t.start, t.end, "no task"))

        self.task_set.set_utility(float(run_time) / self.max_time)
        self.task_set.set_feasible(feasible)
        self.completed_tasks.sort(key=lambda x: x[0] + x[1], reverse=False)
        plt.figure()
        plt.title("Scheduling")
        plt.xlabel("Time")
        plt.ylabel("Task")
        for task in self.task_set.get_all_tasks():
            for t in task.get_history(self.max_time):
                plt.barh(task.name, width=t[1]-t[0], left=t[0], color=task.color)
        plt.savefig(f"img/scheduling_{self.algorithm}_{self.preemptive}.png")
        return self.completed_tasks, self.task_set.feasible, self.task_set.utility

    def get_running_task(self) -> Task | None:
        for task in self.task_set.get_all_tasks():
            if task.state == RUNNING:
                return task
        return None

    def get_task_job(self) -> list[Task]:
        job_set = []
        for task in self.task_set.get_all_tasks():
            if task.job:
                job_set.append(task)
        return job_set

    def schedule(self):
        """Schedule the next task to run

        Returns:
            Task: The next task to run, or None if no tasks are ready
        """
        running_task = self.get_running_task()
        task_set = self.get_task_job()

        if running_task:
            if not self.preemptive:
                return running_task
            running_task.stop_job(self.time)
        elif task_set:
            running_task = task_set[0]

        for task in task_set:
            running_task = self.algorithm_select(running_task, task)
        return running_task

    def algorithm_select(self, running_task, task):

        if self.algorithm == SCHEDULING_ALG_EDF:
            if task.job.get_absolute_deadline() < running_task.job.get_absolute_deadline():
                running_task = task

        if self.algorithm == SCHEDULING_ALG_DM:
            if task.task_type == PERIODIC and running_task.task_type == PERIODIC:
                if task.deadline < running_task.deadline:
                    running_task = task

            elif task.priority < running_task.priority:
                running_task = task

        if self.algorithm == SCHEDULING_ALG_RM:
            if task.task_type == PERIODIC and running_task.task_type == PERIODIC:
                if task.period < running_task.period:
                    running_task = task

            if task.priority < running_task.priority:
                running_task = task

        return running_task

    def update_free_times(self, hp_task, running_task):
        if (self.time == 0 and running_task is None and hp_task is None) or (running_task and hp_task is None):
            self.free_times.append(TimeRange(self.time))
        elif (running_task is None and hp_task) or (self.time == self.max_time and not running_task and not hp_task):
            self.free_times[-1].end = self.time

    def do_step(self, is_preemptive):
        if is_preemptive:
            hp_task = self.schedule()
            if hp_task:
                hp_task.run_job(self.time)
        hp_task = self.get_running_task()
        return hp_task

    def run(self):
        all_tasks = self.task_set.get_all_tasks()
        is_preemptive = False

        running_task = self.get_running_task()

        for task in all_tasks:
            is_preemptive = task.update_task(time=self.time) or is_preemptive

        hp_task = self.do_step(is_preemptive)

        if self.time == 118:
            print(hp_task)

        self.update_free_times(hp_task, running_task)

        if self.time == self.max_time:
            running_task = self.get_running_task()
            if running_task and running_task.job:
                running_task.stop_job(self.time)
                running_task.job_history.append(running_task.job)
                running_task.job = None
        else:
            self.time += 1

    def set_task_set(self, task_set: TaskSet):
        self.task_set = task_set
