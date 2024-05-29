import os
import sys
# Adding project path into the sys paths for scanning all the modules
script_dir = os.path.dirname(os.path.realpath(__file__))
project_paths = [
    os.path.join(script_dir, ".."),
    os.path.join(script_dir, "..", "util", "ssh_util")
]
for project_path in project_paths:
    if project_path not in sys.path:
        sys.path.append(project_path)

import logging.config
import datetime
from qe_infra_rest_client.app import fetch_app

def create_log_file(output_directory):
    logging_conf_path = os.path.join(script_dir, "..", "logging.conf")
    logging_conf = open(logging_conf_path)

    temp_log_conf_path = os.path.join(output_directory, "temp_logging.conf")
    temp_log_conf = open(temp_log_conf_path, "w")

    log_file_path = os.path.join(output_directory, "tasks.log")

    for line in logging_conf:
        line = line.replace("@@FILENAME@@", log_file_path)
        temp_log_conf.write(line)

    temp_log_conf.close()
    logging_conf.close()
    logging.config.fileConfig(temp_log_conf_path, disable_existing_loggers=False)

if __name__ == '__main__':
    current_time = datetime.datetime.now()
    timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
    output_dir_name = f"results_{timestamp_string}"
    output_dir = os.path.join(script_dir, "..", output_dir_name)

    if not os.path.exists(output_dir):
        print(f"Creating directory {output_dir}")
        try:
            os.makedirs(output_dir)
        except Exception as e:
            print(f"Error creating directory {output_dir} : {e}")
            raise e

    create_log_file(output_dir)
    app = fetch_app()
    app.run(debug=True)