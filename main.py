import sys
if len(sys.argv) < 2:
    print("Usage: python a.py <task_name>")
    sys.exit(1)

task_name = sys.argv[1]
print(f"Task name: {task_name}")

import yaml

file_path = '/Users/raghavsk/Desktop/qe-infra-engine/tasks/tasks.yml'

with open(file_path, 'r') as file:
    data = yaml.safe_load(file)
    
import argparse
parser = argparse.ArgumentParser(description="A tool to run a task")
parser.add_argument("task_name", help="Name of the task")
data = data["tasks"]
for task in data:
    if task_name == task:
        task_data = data[task_name]
        path = task_data["path"]
        params = task_data["params"]
        for argument in params:
            argument_params = params[argument]
            for arg_name, arg_info in params.items():
                flags = arg_info.get('flags', [])
                help_text = arg_info.get('help', "")
                action = arg_info.get('action', None)
                arg_type = eval(arg_info.get('type', str))
                choices = eval(arg_info.get('choices', "None"))
                const = None
                parser.add_argument(*flags,
                                    dest=arg_name,
                                    type=arg_type,
                                    help=help_text,
                                    action=action,
                                    choices=choices,
                                    const=const)

args = parser.parse_args()
print(vars(args))
print(args)