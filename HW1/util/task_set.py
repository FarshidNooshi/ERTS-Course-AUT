from HW1.task import Task


class TaskSet:
    """Task Set Class

    Attributes:
        tasks (list): List of Task objects
        utility (float): Utility of the task set
        feasible (bool): Whether the task set is feasible
    """

    def __init__(self, tasks=None):
        if tasks is None:
            tasks = []
        self.tasks = tasks
        self.utility = 0
        self.feasible = False

    def add_task(self, task):
        self.tasks.append(task)

    def remove_task(self, task):
        self.tasks.remove(task)

    def get_task_by_name(self, name):
        for task in self.tasks:
            if task.name == name:
                return task
        return None

    def get_all_tasks(self) -> list[Task]:
        return self.tasks

    def set_feasible(self, feasible):
        self.feasible = feasible

    def set_utility(self, utility):
        self.utility = utility
