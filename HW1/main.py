import csv

from RTOS import RTOS
from task import Task
from task_set import TaskSet


class Main:
    def __init__(self, duration):
        self.rtos = RTOS(duration)
        self.task_set = TaskSet()
        self.duration = duration

    def run(self):
        # create tasks and add them to task set
        self.read_tasks_from_csv('tasks1.csv')

        # schedule tasks using EDF algorithm
        self.rtos.set_task_set(self.task_set)
        self.rtos.run(self.duration)

    def read_tasks_from_csv(self, filename):
        with open(filename, 'r') as csvfile:
            # header in csv file: priority,name,  state, type, act_time, period, wcet, deadline
            task_header = csv.reader(csvfile, delimiter=',', )

            next(task_header, None)  # skip the headers
            for row in task_header:
                print(row)
                priority, name, state, task_type, act_time, period, wcet, deadline = row
                task = Task(
                    priority=int(priority),
                    name=name,
                    state=int(state),
                    task_type=int(task_type),
                    act_time=int(act_time),
                    period=int(period),
                    wcet=int(wcet),
                    deadline=int(deadline)
                )
                self.task_set.add_task(task)


if __name__ == '__main__':
    main = Main(120)
    main.run()
