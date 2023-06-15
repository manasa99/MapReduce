import os
import socketserver
import configparser
import socket
import logging
import logging.config
import threading
import time
import datetime
import uuid
from socket import timeout
from src.utils.fileHandler import FileHandler
import src.utils.fileHandler as fileHandler
import src.utils.variables as variables
import shutil
from multiprocessing import Process
from pathlib import Path


import importlib


class WorkerServer:
    def __init__(self, worker_address, status=None, last_seen=time.time(), task_id=None, fails=0, marker=True, role=None):
        self.worker_address = worker_address
        self.status = status
        self.last_seen = last_seen
        self.task_id = task_id
        self.fails = fails
        self.marker = marker
        self.role = role

    def get(self, item):
        return self.__dict__[item]

    def set(self, item, value):
        self.__dict__[item] = value

    def __str__(self):
        res = ""
        for i in [self.worker_address, self.status, self.role, self.marker, self.fails, self.last_seen]:
            res += " : " + str(i)
        return res


class Task:
    def __init__(self, msg, task_dir: str, status=variables.queued):
        self.workers = None
        self.task_dir = task_dir
        self.input_file, self.app_name, self.output_loc_name = msg
        self.status = status
        self.__id__ = uuid.uuid4()
        self.child_tasks = []
        self.mapped_data = {}

    def add_child_task(self, worker_address):
        self.child_tasks += [worker_address]

    def set_status(self, status):
        self.status = status

    def get_status(self):
        return self.status

    def get_id(self):
        return self.__id__

    def reset(self):
        self.child_tasks = []
        self.mapped_data = {}
        self.status = variables.queued
        self.workers = None



# have a worker_server class
# that has all methods with respect to its status, its current task, its progress

class Master(socketserver.ThreadingMixIn, socketserver.TCPServer, Process):

    def __init__(self, server_address, RequestHandlerClass, cfg_file_path: str):
        self.time_to_wait = None
        self.config_path = cfg_file_path
        self.heartbeat_time = None
        self.application_interface = None
        self.task_dir = None
        self.load_config()
        self.active_task = None
        self.barrier_thread = None
        self.worker_servers = {}
        self.config = None
        self.task = None
        super().__init__(server_address, RequestHandlerClass)
        self.check_thread = threading.Thread(target=self.check_worker_server)
        self.check_thread.start()
        self.tasks = []
        self.tasks_thread = threading.Thread(target=self.task_handler)
        self.tasks_thread.start()
        # self.mapped_data = {}

    def run(self):
        self.serve_forever()

    def load_config(self):
        """
        This method reads and loads the config for the server from the given config file
        :return:
        """
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        self.task_dir = self.config.get(variables.common, variables.data_dir)
        self.application_interface = self.config.get(variables.common, variables.application_interface)
        self.heartbeat_time = self.config.getint(variables.common, variables.heartbeat_time)
        self.time_to_wait = self.heartbeat_time * 3
        log_file_path = self.config.get(variables.common, variables.log_file_path)
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        logging.info(self.task_dir)
        return True

    def create_chunks(self, dir_name, curr_workers):
        return fileHandler.chunk_file_by_lines(self.active_task.input_file, curr_workers, dir_name)

    def fire_task(self, curr_workers):
        dir_name = os.path.join(self.task_dir, str(self.active_task.get_id()))
        files = self.create_chunks(dir_name, len(curr_workers))
        # map protocol : task map <file_chunk_path> <file_chunk_ptr> <application_name>
        for ind, val in enumerate(curr_workers):
            try:
                task_str = f"{variables.task} {variables.map} {files[ind][0]} {str(files[ind][1])} {self.active_task.app_name}"
                # task_str = "task map " + files[ind][0] + " " + str(files[ind][1]) + " " + self.active_task.app_name
                self.active_task.add_child_task(val)
                self.send_msg(val, task_str)
                self.active_task.set_status(variables.mapping)
            except Exception as e:
                logging.warning(e)
                self.reinstantiate_task()
                break

    def reinstantiate_task(self):
        self.active_task.reset()
        self.tasks.insert(0, self.active_task)
        self.clear(False)

    def barrier(self, desired_status):
        while True:
            if self.active_task is None:
                break
            if all([self.worker_servers[i].status == desired_status for i in self.active_task.child_tasks
                    if self.worker_servers[i].role == variables.mapper]):
                self.active_task.set_status(desired_status)
                return True
            time.sleep(self.heartbeat_time)

    def reduce_to_files(self):
        for i in self.grouper():
            fn = os.path.join(self.task_dir, str(self.active_task.get_id()), variables.red_file_head + str(i[0][0]))
            logging.info(fn)
            FileHandler(fn, [j[0] + " " + j[1] for j in i]).write_file()
            # self.mapped_data[fn] = None
            self.active_task.mapped_data[fn] = None

    def send_to_reducer(self):
        while True:
            try:
                x = -1
                # for i in self.mapped_data:
                for i in self.active_task.mapped_data:
                    # if self.mapped_data[i] is not None:
                    if self.active_task.mapped_data[i] is not None:
                        continue
                    active_workers = [w for w in self.worker_servers if
                                      self.worker_servers[w].marker and self.worker_servers[w].role == variables.reducer]
                    x = x + 1 if x < len(active_workers) - 1 else 0
                    logging.info(f"Trying to reduce {i}")
                    logging.info(active_workers)
                    logging.info(":".join([str(self.worker_servers[j]) for j in active_workers]))
                    msg = f"{variables.task} {variables.reduce} {i} {self.active_task.app_name}"
                    self.send_msg(active_workers[x], msg)
                    # self.send_msg(active_workers[x], "task reduce " + i + " " + self.active_task.app_name)
                break
            except Exception as e:
                logging.warning(e)
                time.sleep(self.heartbeat_time)

    def grouper(self):
        final_list = []
        fp = os.path.join(self.task_dir, str(self.active_task.get_id()))
        for file in os.listdir(fp):
            if file.endswith(variables.map_chunk_tail):
                logging.info(f"reading file to map for task : {self.active_task.get_id()}")
                logging.info(f"file : {os.path.join(fp, file)}")
                final_list.extend([i.split() for i in FileHandler(os.path.join(fp, file)).read_file()])
        final_list.sort(key=lambda x: x[0])
        curr = 0
        while final_list:
            if curr + 1 < len(final_list):
                if final_list[curr] == final_list[curr + 1]:
                    curr += 1
                else:
                    yield final_list[:curr + 1]
                    final_list = final_list[curr + 1:]
                    curr = 0
            else:
                yield final_list
                final_list = []

    def reset_from_mapped(self):
        for i in self.worker_servers:
            if self.worker_servers[i].marker and self.worker_servers[i].role == variables.mapper:
                self.send_msg(i, variables.change_status)

    def send_msg(self, addr, msg):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(addr)
            s.sendall(msg.encode())

    def clear(self, val):
        if self.active_task:
            if val:
                shutil.rmtree(os.path.join(self.task_dir, str(self.active_task.get_id())))
            self.active_task = None

    def task_handler(self):
        """
        check the worker handlers status if all workers are free and if a new task can be picked up
        :return:
        """
        while True:
            curr_workers = [k for k, v in self.worker_servers.items() if v.marker and v.role == variables.mapper]
            check_curr_workers = all([self.worker_servers[k].status == variables.up for k in curr_workers])
            if not curr_workers or not check_curr_workers or not self.tasks or self.active_task is not None:
                logging.info("Conditions not met to fire a task")
                time.sleep(self.heartbeat_time)
            else:

                self.active_task = self.tasks.pop(0)
                logging.info(f"Firing task {self.active_task.get_id()}")
                print("Firing task: ", self.active_task.get_id())
                try:
                    self.fire_task(curr_workers)
                except Exception as e:
                    logging.error(e)
                    time.sleep(self.time_to_wait)
                if self.active_task and self.active_task.get_status() == variables.mapping:
                    self.active_task.set_status(variables.reducing)
                    self.barrier_thread = threading.Thread(target=self.barrier,
                                                           kwargs={variables.desired_status: variables.mapped})
                    self.barrier_thread.start()
                    self.barrier_thread.join()
                    self.reset_from_mapped()
                    self.reduce_to_files()
                    self.send_to_reducer()
                    while True:
                        # if any([value is None for value in self.mapped_data.values()]):
                        if any([value is None for value in self.active_task.mapped_data.values()]):
                            time.sleep(self.heartbeat_time)
                        else:
                            # FileHandler(self.active_task.output_loc_name, self.mapped_data.values()).write_file()
                            FileHandler(self.active_task.output_loc_name, self.active_task.mapped_data.values()).write_file()
                            break
                    self.active_task.set_status(variables.complete)
                    print("Task Completed: ", self.active_task.get_id())
                    logging.info(f"Task Completed: {self.active_task.get_id()}")
                    self.clear(True)

    def add_worker_server(self, worker_address, role):
        self.worker_servers[worker_address] = WorkerServer(worker_address, role=role)

    def check_worker_server(self):
        while True:
            for worker_address, worker_dict in self.worker_servers.items():
                last_seen_time = worker_dict.get(variables.last_seen)
                current_time = time.time()
                if current_time - last_seen_time > self.time_to_wait:
                    logging.warning(f"No heartbeat received from {worker_address} within {self.time_to_wait} seconds.")
                    self.worker_servers[worker_address].fails += 1
                    self.worker_servers[worker_address].status = variables.error
                    self.worker_servers[worker_address].marker = False
                else:
                    self.worker_servers[worker_address].marker = True
                [logging.info(self.worker_servers[i]) for i in self.worker_servers]
            time.sleep(self.heartbeat_time)

    def create_task(self, task):
        """
        :param task: assigned task
        Validates task(checks if apllication present)
        Creates task instance
        Adds to Task queue
        :return: Created task's id / False if task creation failed
        """
        try:
            lib_dict = importlib.import_module(task[1])
            if self.application_interface in str(lib_dict.__dict__["App"]):
                logging.info("Given task has valid App")
            else:
                logging.warning("App not valid")
                raise Exception("App not valid")
        except Exception as e:
            logging.error(e)
            return False
        # check given file exits
        if not os.path.isfile(task[0]):
            logging.warning("No such file exists")
            return False

        created_task = Task(task, self.task_dir)
        self.tasks += [created_task]
        logging.info("Task added successfully")
        return created_task.get_id()


class RequestHandler(socketserver.StreamRequestHandler):

    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)

    def handle(self):
        """
        handle() gets request checks if worker:
        connect -> adds worker to worker servers
        status  -> updates status of already existing worker, used for heartbeat
        protocol for worker:
            connect <role> <address>
            <status> <address>
            mapped <address>
            reduced <task> <address> <reduced_result>
        protocol for client:
            task input_file mapredfunc output_location

        :return:
        """
        msg = self.rfile.readline().strip().decode()
        if not msg:
            return
        msg = msg.split()
        logging.info(msg)
        if msg[0] == variables.connect:
            worker_address = (msg[2], int(msg[3]))
            role = msg[1]
            logging.info("worker added")
            self.server.add_worker_server(worker_address, role)
            while True:
                try:
                    # Wait for heartbeats from the worker server.
                    msg = self.rfile.readline().strip().decode()
                    msg = msg.split()
                    logging.info(msg)
                    if not msg:
                        break
                    if msg[0] in (variables.up, variables.mapping, variables.mapped, variables.reducing):
                        worker_address = (msg[1], int(msg[2]))
                        logging.info(f"{worker_address} : {msg}")
                        self.server.worker_servers[worker_address].status = msg[0]
                        self.server.worker_servers[worker_address].last_seen = time.time()
                    elif msg[0] == variables.reduced:
                        # self.server.mapped_data[msg[1]] = msg[2] + " " + " ".join(msg[3:])
                        self.server.active_task.mapped_data[msg[1]] = msg[2] + " " + " ".join(msg[3:])
                        logging.info(msg)
                except (ConnectionResetError, OSError, Exception) as e:
                    logging.info("Worker Terminated.")
                    logging.error(e)
                    break
        else:
            try:
                if msg[0] == variables.show:
                    if msg[1] == variables.workers:
                        active_workers = [(k, v.role) for k, v in self.server.worker_servers if v.marker]
                        logging.info(f"Active workers: {str(active_workers)}")
                    if msg[1] == variables.tasks:
                        tasks = [self.server.tasks]
                        logging.info(f"Tasks in queue: {str(tasks)}")
                    if msg[1] == variables.task:
                        task_id = self.server.active_task.get_id()
                        logging.info(f"Active task: {str(task_id)}")

                if msg[0] == variables.task:
                    client_address = (msg[1], int(msg[2]))
                    id = self.server.create_task(msg[3:])
                    if id:
                        print("Task Created with id: ", id)
                        msg = f"Task created: {id}\r\n"
                        logging.info(msg)
                        self.server.send_msg(client_address, msg)
                    else:
                        msg = f"Task not created \r\n"
                        self.server.send_msg(client_address, msg)

            except Exception as e:
                logging.error(e)
                return
