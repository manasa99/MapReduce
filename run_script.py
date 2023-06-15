import argparse
import threading
import time
from src.test_cases.test_cases import test
from src.master.master import Master, RequestHandler
from src.worker.worker import Worker, RequestHandler as WorkerRequestHandler
from src.client.client import Client
import src.utils.variables as variables
import configparser
from multiprocessing import Process
import sys


def mas(config_path, host, port, common):
    print(locals())
    config = configparser.ConfigParser()
    config.read(config_path)
    master_server = Master((host, int(port)), RequestHandler, config_path)
    master_server.serve_forever()


def fun(s,):
    if s == "mapper":
        worker_server = Worker(("0.0.0.0", 0), WorkerRequestHandler, variables.config_path, "mapper")
        worker_server.serve_forever()
    else:
        worker_server = Worker(("0.0.0.0", 0), WorkerRequestHandler, variables.config_path, "reducer")
        worker_server.serve_forever()


def cli_fun(host, port, input_file, func, output_file):
    Client(host, port, input_file, func, output_file).connect()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--port", type=int, help="connect to port <port>")
    parser.add_argument("-i", "--host", type=str, help="connect to map-reduce-host")
    parser.add_argument("-c", "--config", type=str, help="when included, loads config")
    parser.add_argument("-r","--role", type=str, help="role of worker")
    parser.add_argument("-w", "--worker", help="when included, creates worker", action="store_true")
    parser.add_argument("-m", "--master", help="when included, creates master", action="store_true")
    parser.add_argument("-cli", "--client", help="when included, creates client", action="store_true")
    parser.add_argument("-inp", "--input", help="Input file")
    parser.add_argument("-fn", "--function", help="map reduce function")
    parser.add_argument("-t", "--tests", help="when runs tests", action="store_true")
    parser.add_argument("-s", "--start", help="when creates cluster", action="store_true")
    parser.add_argument("-nm", "--num_mappers", help="number of mappers for the cluster")
    parser.add_argument("-nr", "--num_reducers", help="number of reducers for the cluster")
    parser.add_argument("-op", "--output", help="Output file location")

    args = parser.parse_args()

    print(args.__dict__)

    if args.tests:
        test()
        exit(0)

    if args.start:
        config_path = variables.config_path
        config = configparser.ConfigParser()
        config.read(variables.config_path)
        host = config.get(variables.common, variables.master_host)
        port = config.getint(variables.common, variables.master_port)
        master_server = Master((host, port), RequestHandler, variables.config_path)
        # proc = Process(daemon=True,target=mas,args=["config.cfg",host,port,variables.common])
        # proc.start()
        mp = []
        rp = []
        for i in range(int(args.num_mappers)):
            # worker_server = Worker((host, 0), WorkerRequestHandler, config_path, "mapper")
            proc = Process(daemon=True,target=fun,args=["mapper"])
            proc.start()
            mp.append(proc)

        for i in range(int(args.num_reducers)):
            # worker_server = Work
            proc = Process(daemon=True, target=fun, args=["reducer"])
            proc.start()
            rp.append(proc)
        master_server.serve_forever()

    if args.client:
        # start client and connect to master
        c = Client(host=args.host, port=args.port, input_file=args.input, mapreduce_function=args.function, output_location=args.output)
        c.connect()

    else:
        if args.config:
            config = configparser.ConfigParser()
            try:
                config.read(args.config)
            except Exception:
                print("Config file unreadable")
                exit()
            config_path = args.config
        else:
            print("Master/Worker running with default config file")
            config_path = variables.config_path
            config = configparser.ConfigParser()
            try:
                config.read(config_path)
            except Exception:
                print("Config file not found!")
                exit()

        if args.master:
            # setup and start master server
            host = config.get(variables.common, variables.master_host)
            port = config.getint(variables.common, variables.master_port)
            master_server = Master((host, port), RequestHandler, config_path)
            master_server.serve_forever()

        if args.worker:
            # setup and start worker server
            host, port = 'localhost', 0
            if args.host:
                host = args.host
            if args.port:
                port = args.port
            worker_server = Worker((host, port), WorkerRequestHandler, config_path, args.role)
            worker_server.serve_forever()
