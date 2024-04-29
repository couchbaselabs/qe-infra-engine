import concurrent.futures
import logging.config
import random
import time

logging_conf_path = "logging.conf"
logging.config.fileConfig(logging_conf_path)
logger = logging.getLogger("tasks")

class Task:
    def execute(self, params):
        # logger.info("Task.execute() started", params)
        callback = params["task"]
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            futures = [executor.submit(callback) for _ in range(10)]
            for future in futures:
                logger.info("Finished err")
                logger.error("hhhhhh {}".format(repr(future.exception())))
                if future.exception():
                    print("Exception bhai Exception")
                else:
                    print("No Exception bhai No Exception")
            for count, future in enumerate(concurrent.futures.as_completed(futures)):
                # logger.error(future.exception)
                logger.info(count)
                result = future.result()
                logger.info
                logger.error("hhhhhh {}".format(repr(future.exception())))
        # logger.info("Task.execute() finished", params)

class T1(Task):
    count_err = 0
    count_done = 0
    def execute(self, params):
        # logger.info("T1.execute() started", params)
        try:
            super().execute(params=params)
        except:
            pass
        # logger.info("T1.execute() finished", params)
        logger.info("done {}".format(self.count_done))
        logger.info("error {}".format(self.count_err))
    
    def hello(self):
        # logger.info("T1.hello()")
        # params = {"task" : self.fetch_sub_task("bye")}
        # self.execute(params)
        a = random.choice([1, 2])
        if a ==1:
         logger.error("hello error")
         self.count_err +=1
         raise Exception("hello")
        time.sleep(10)
        logger.info("Hello finished")
        self.count_done += 1
        return "Hello"

    def bye(self):
        # logger.info("T1.hello()")
        pass
    
    def fetch_sub_task(self, name):
        d = {"hello" : self.hello, "bye" :  self.bye}
        return d[name]

t = T1()
subtask = t.fetch_sub_task("hello")
params = {"task":subtask}
t.execute(params)