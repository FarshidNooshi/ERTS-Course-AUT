from HW1.util.enum import SCHEDULING_ALG_RM, SCHEDULING_ALG_DM, SCHEDULING_ALG_EDF
from printer import TaskSetPrinter as Printer
from schedular import Scheduler


class RTOS:
    """Real-Time Operating System Class"""

    def __init__(self, duration=500, task_set=None):
        """Initialize the RTOs instance
        
        Args:
            task_set (TaskSet): The task set to run on the operating system
        """
        self.task_set = task_set
        self.scheduler = Scheduler(self.task_set, max_time=duration, algorithm=SCHEDULING_ALG_RM, preemptive=False)
        self.printer = Printer()

    def run(self, duration):
        """Run the task set on the operating system for a specified duration
        
        Args:
            duration (int): The duration to run the task set for
        
        Returns:
            List of Task: The completed task set after running on the operating system for the specified duration
        """
        completed_tasks = []
        for i in range(duration):
            # self.scheduler.schedule()
            self.scheduler.run()
            completed_tasks += self.scheduler.completed_tasks

        self.printer.print_schedule(completed_tasks)

        history, feasible, utility = self.scheduler.get_result()
        self.printer.print_result(history, feasible, utility)

        return completed_tasks

    def set_task_set(self, task_set):
        """Set the task set for the operating system
        
        Args:
            task_set (TaskSet): The task set to run on the operating system
        """
        self.task_set = task_set
        self.scheduler.set_task_set(task_set)
