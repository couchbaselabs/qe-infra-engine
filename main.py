import os
import sys
# Adding project path into the sys paths for scanning all the modules
script_dir = os.path.dirname(os.path.realpath(__file__))
project_paths = [
    os.path.join(script_dir, "util", "ssh_util")
]
for project_path in project_paths:
    if project_path not in sys.path:
        sys.path.append(project_path)

import yaml
import logging.config
import datetime
import argparse
import json
from tasks.task_builder import TaskBuilder

def create_log_file(output_directory):
    logging_conf_path = os.path.join(script_dir, "logging.conf")
    logging_conf = open(logging_conf_path)

    temp_log_conf_path = os.path.join(output_directory, "temp_logging.conf")
    temp_log_conf = open(temp_log_conf_path, "w")

    log_file_path = os.path.join(output_directory, "tasks.log")

    for line in logging_conf:
        line = line.replace("@@FILENAME@@", log_file_path)
        temp_log_conf.write(line)

    temp_log_conf.close()
    logging_conf.close()
    logging.config.fileConfig(temp_log_conf_path)

def parse_arguments():
    if len(sys.argv) < 2:
        print("Usage: python main.py <task_name>")
        sys.exit(1)

    tasks_file_path = os.path.join(script_dir, "tasks", "tasks.yml")

    with open(tasks_file_path, 'r') as file:
        tasks_data = yaml.safe_load(file)

    if sys.argv[1] not in tasks_data:
        print("Usage: python main.py <task_name>")
        raise ValueError(f"Given task {sys.argv[1]} not found")

    task_name = sys.argv[1]
    print(f"Task name: {task_name}")

    parser = argparse.ArgumentParser(description="A tool to run a task")
    parser.add_argument("task_name", help="Name of the task")
    
    argument_data = tasks_data[task_name]

    params = argument_data["params"]
    print(params)
    for arg_name, arg_info in params.items():
        flags = arg_info.get('flags', [])
        help_text = arg_info.get('help', "")
        action = arg_info.get('action', None)
        arg_type = eval(arg_info.get('type', str))
        nargs = arg_info.get('nargs', None)
        parser.add_argument(*flags,
                            dest=arg_name,
                            type=arg_type,
                            help=help_text,
                            action=action,
                            nargs=nargs)

    args = parser.parse_args()
    print(vars(args))
    return task_name, vars(args)

def fetch_and_run_task(task_name, params, output_dir):
    task = TaskBuilder.fetch_task(task_name, params)
    task.execute()
    json_result = task.generate_json_result()
    
    local_file_path = os.path.join(output_dir, f"result.json")
    with open(local_file_path, "w") as json_file:
        json.dump(json_result, json_file)

def main():

    task_name, params = parse_arguments()

    current_time = datetime.datetime.now()
    timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
    output_dir_name = f"results_{timestamp_string}"
    output_dir = os.path.join(script_dir, output_dir_name)

    if not os.path.exists(output_dir):
        print(f"Creating directory {output_dir}")
        try:
            os.makedirs(output_dir)
        except Exception as e:
            print(f"Error creating directory {output_dir} : {e}")
            return

    create_log_file(output_dir)

    fetch_and_run_task(task_name, params, output_dir)

if __name__ == "__main__":
    main()