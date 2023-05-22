from task import Task
class TaskSet:
    """Task Set Class
    
    Attributes:
        tasks (list): List of Task objects
        utility (float): Utility of the task set
        self.feasible (bool): Whether the task set is feasible
    """
    def __init__(self, tasks=[]):
        
        self.tasks = tasks
        self.utility = 0
        self.feasible = False
        
        
        
    def add_task(self, task):
        """Add a task to the task set
        
        Args:
            task (Task): Task object to add
        """
        self.tasks.append(task)
        
    def remove_task(self, task):
        """Remove a task from the task set
        
        Args:
            task (Task): Task object to remove
        """
        self.tasks.remove(task)
        
    def get_task_by_name(self, name) -> Task:
        """Get a task from the task set by name
        
        Args:
            name (str): Name of the task to get
        
        Returns:
            Task: The task object with the given name, or None if not found
        """
        for task in self.tasks:
            if task.name == name:
                return task
        return None
    
    def get_all_tasks(self) -> list:
        """Get a list of all tasks in the task set
        
        Returns:
            list: List of all Task objects in the task set
        """
        return self.tasks
    def set_feasible(self, feasible):
        """Set the feasibility of the task set
        
        Args:
            feasible (bool): Whether the task set is feasible
        """
        self.feasible = feasible
    def set_utility(self, utility):
        """Set the utility of the task set
        
        Args:
            utility (float): Utility of the task set
        """
        self.utility = utility
    
