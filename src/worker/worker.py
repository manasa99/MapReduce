import importlib
import socketserver
import socket
import src.utils.variables as variables
import configparser
import threading
import logging
import logging.config
import time
from src.utils.fileHandler import FileHandler
from multiprocessing import Process


class Worker(socketserver.ThreadingMixIn, socketserver.TCPServer, Process):
    def __init__(self, server_address, RequestHandlerClass, cfg_file_path: str, role: str):
        self.cfg_file_path = cfg_file_path
        self.config = None
        self.master_address = None
        self.status = None
        self.heartbeat_time = None
        self.try_to_connect = None
        self.wait_time = None
        self.s = None
        self.role = role
        self.load_config()
        self.address_string = None

        super().__init__(server_address, RequestHandlerClass)

    def run(self):
        # super(P, self).__init__()
        self.serve_forever()

    def load_config(self):
        """
        This method reads and loads the config for the server from the given config file
        :return:
        """
        self.config = configparser.ConfigParser()
        self.config.read(self.cfg_file_path)
        host = self.config.get(variables.common, variables.master_host)
        port = self.config.getint(variables.common, variables.master_port)
        self.master_address = (host, port)
        self.heartbeat_time = self.config.getint(variables.common, variables.heartbeat_time)
        self.try_to_connect = self.config.getint(variables.common, variables.try_to_connect)
        self.wait_time = self.config.getint(variables.common, variables.wait_time)
        self.status = variables.up
        log_file_path = self.config.get(variables.common, variables.log_file_path)
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        return True

    def format_msg(self, msg):
        return msg + "\r\n"

    def serve_forever(self):
        # Connect to the master server.
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        while self.try_to_connect:
            try:
                self.s.connect(self.master_address)
                break
            except ConnectionRefusedError:
                logging.warning("Failed to connect to the master server")
                #print("Failed to connect to the master server")
                if self.try_to_connect > 0:
                    time.sleep(self.wait_time)
                    pass
                else:
                    exit(0)
            self.try_to_connect -= 1

        self.address_string = str(self.server_address[0]) + " " + str(self.server_address[1])
        msg = self.format_msg(variables.connect + " " + self.role + " " + self.address_string)
        self.s.sendall(msg.encode())
        logging.info("sending heartbeats to the master server.")
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()

        super().serve_forever()

    def process_request(self, request, client_address):
        # Override the process_request method to set the request handler class.
        self.RequestHandlerClass = RequestHandler
        return super().process_request(request, client_address)

    def send_heartbeat(self):
        while True:
            try:
                msg = self.format_msg(self.status + " " + self.address_string)
                self.s.sendall(msg.encode())
                time.sleep(self.heartbeat_time)
            except Exception as e:
                logging.warning(e)
                exit(0)

    def process_task(self, task):
        # Process the task here.
        logging.info(f"Task received to process")
        self.status = variables.busy
        if task[0] == variables.map:
            self.status = variables.mapping
            im = importlib.import_module(task[3])
            im_obj = im.Application(lines=FileHandler(task[1]).read_file(), file_ptr=int(task[2]))
            # #print("/".join(task2.split("/")[:-1]))
            FileHandler(task[1] + variables.map_chunk_tail, im_obj.map_data()).write_file()
            self.status = variables.mapped

        elif task[0] == variables.reduce:
            self.status = variables.reducing
            im = importlib.import_module(task[2])
            mapped_data = [(i.split()[0], (i.split()[1])) for i in FileHandler(task[1]).read_file()]
            im_obj = im.Application(mapped_data=mapped_data)
            # #print("/".join(task2.split("/")[:-1]))
            res = im_obj.reduce_data()
            while True:
                try:
                    logging.info(f"{variables.reduced} {task[1]} {res}")
                    msg = f"{variables.reduced} {task[1]} {res}"
                    self.s.sendall(self.format_msg(msg).encode())
                    # self.s.sendall(self.format_msg("reduced "+task[1]+" "+res).encode())
                    #print("sent")
                    break
                except Exception as e:
                    logging.warning(e)
                    exit(0)
            self.status = variables.up


class RequestHandler(socketserver.StreamRequestHandler):

    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)

    def handle(self):
        while True:
            try:
                msg = self.rfile.readline().strip().decode()
                if not msg:
                    break
                msg = msg.split()
                if msg[0] == variables.task:
                    # Handle task message from master server.
                    self.server.process_task(msg[1:])

                if msg[0] == variables.change:
                    # set status back to up
                    self.server.status = variables.up
            except ConnectionResetError:
                break
