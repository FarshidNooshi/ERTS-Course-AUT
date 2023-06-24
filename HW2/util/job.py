class Job:

    def __init__(self, act_time, deadline: int):
        self.act_time = act_time
        self.deadline = deadline
        self.time_ranges = []
        self.run_time = 0

    def get_absolute_deadline(self):
        return self.act_time + self.deadline
