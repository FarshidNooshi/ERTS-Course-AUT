import csv

tasks = [
    {"priority": 1, "name": "Task1", "state": 1, "type": 1, "act_time": 1, "period": 20, "wcet": 5, "deadline": 20},
    {"priority": 2, "name": "Task2", "state": 1, "type": 1, "act_time": 2, "period": 50, "wcet": 10, "deadline": 50},
    {"priority": 3, "name": "Task3", "state": 1, "type": 1, "act_time": 3, "period": 100, "wcet": 20, "deadline": 100},
    {"priority": 2, "name": "Task4", "state": 1, "type": 3, "act_time": 20, "period": 0, "wcet": 4, "deadline": 35},
    {"priority": 4, "name": "Task5", "state": 1, "type": 3, "act_time": 20, "period": 0, "wcet": 4, "deadline": 35},
]

with open("tasks2.csv", mode="w", newline="") as csv_file:
    fieldnames = tasks[0].keys()
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    writer.writeheader()
    for task in tasks:
        writer.writerow(task)
