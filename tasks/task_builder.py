import os
import yaml
import importlib.util

class TaskBuilder:
    @staticmethod
    def fetch_task(task_name, params):
        """
            Creates and fetches the task with the given name
            Args:
            params (dict, required): The dictionary with the required fields for the task
            task_name (str, required) : The task name of the task to be fetched
        """
        # Fetch tasks.yml
        script_dir = os.path.dirname(os.path.realpath(__file__))
        tasks_file_path = os.path.join(script_dir, "tasks.yml")
        with open(tasks_file_path, 'r') as file:
            tasks_data = yaml.safe_load(file)

        if task_name not in tasks_data:
            raise ValueError(f"Task {task_name} not found")

        task = tasks_data[task_name]

        if "class" not in task:
            raise ValueError(f"Class for the task {task_name} not found")
        class_name = task["class"]

        if "path" not in task:
            raise ValueError(f"Path for the task {task_name} not found")
        path = os.path.join(script_dir, "..", task["path"])

        if not os.path.exists(path):
            raise ValueError(f"Path for the task {task_name} not found : {path}")

        spec = importlib.util.spec_from_file_location(class_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        class_object = getattr(module, class_name)
        instance = class_object(params)
        return instance
