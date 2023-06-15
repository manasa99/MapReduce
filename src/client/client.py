import socket
import time
from _socket import SHUT_WR


class Client:
    """
    Main class for creating a client for a map-reduce
    """
    def __del__(self):
        self.socket.shutdown(SHUT_WR)
        self.socket.close()

    def __init__(self, host: str, port: int, input_file: str, mapreduce_function: str, output_location: str):
        """
        :param host: host arg passed from the driver code
        :param port: port arg passed from the driver code
        """
        self.socket = None
        self.host = host
        self.port = port
        self.input_file = input_file
        self.mapreduce_function = mapreduce_function
        self.output_location = output_location

    def connect(self):
        try:
            time.sleep(5)
            self.socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            address = self.socket.getsockname()[0] + " " + str(self.socket.getsockname()[1])
            task = self.input_file + " " + self.mapreduce_function + " " + self.output_location
            msg = "task" + " " + address + " " + task + "\r\n"
            #print("from ", self.socket.getsockname(), "\nmsg:\t", msg)
            self.client_send(msg)
            time.sleep(10)
            msg = self.socket.recvfrom(1000)
            #print(msg)
            # break
        except Exception as e:
            print(e)

    def client_send(self, data: str):
        return self.socket.sendall(data.encode())
