import argparse
import os
import threading
import time
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


def fun(s, ):
    if s == "mapper":
        worker_server = Worker(("0.0.0.0", 0), WorkerRequestHandler, variables.config_path, "mapper")
        worker_server.serve_forever()
    else:
        worker_server = Worker(("0.0.0.0", 0), WorkerRequestHandler, variables.config_path, "reducer")
        worker_server.serve_forever()


def cli_fun(host, port, input_file, func, output_file):
    Client(host, port, input_file, func, output_file).connect()


def test():
    # create cluster with defaults
    print("Initiating tests")
    print("Initiating cluster for tests with 4 mappers and 4 reducers")

    config = configparser.ConfigParser()
    config.read(variables.config_path)

    host = config.get(variables.common, variables.master_host)
    port = config.getint(variables.common, variables.master_port)
    base_path = config.get(variables.test, variables.base_path)
    inputs = config.get(variables.test, variables.inputs).split(",")
    outputs = config.get(variables.test, variables.outputs)
    app_path = config.get(variables.test, variables.app_path).split(",")

    master_server = Master((host, port), RequestHandler, variables.config_path)

    mp = []
    rp = []
    for i in range(4):
        # worker_server = Worker((host, 0), WorkerRequestHandler, config_path, "mapper")
        proc = Process(daemon=True, target=fun, args=["mapper"])
        proc.start()
        print("Started mapper with pid ", proc)
        mp.append(proc)

    for i in range(4):
        # worker_server = Work
        proc = Process(daemon=True, target=fun, args=["reducer"])
        proc.start()
        print("Started reducer with pid ", proc)
        rp.append(proc)

    print("Cluster Initiation completed")
    print("Sending mapreduce tasks using API implementation")

    test_cases = [
        [host, port, base_path + inputs[0],
         app_path[0],
         base_path + outputs + app_path[0].split(".")[-1] + "_" + inputs[0].split("/")[-1] + "_case1.txt"],
        [host, port, base_path + inputs[1],
         app_path[0],
         base_path + outputs + app_path[0].split(".")[-1] + "_" + inputs[1].split("/")[-1] + "_case1.txt"],
        [host, port, base_path + inputs[0],
         app_path[1],
         base_path + outputs + app_path[1].split(".")[-1] + "_" + inputs[0].split("/")[-1] + "_case1.txt"],
        [host, port, base_path + inputs[1],
         app_path[1],
         base_path + outputs + app_path[1].split(".")[-1] + "_" + inputs[1].split("/")[-1] + "_case1.txt"],
    ]

    for i in test_cases:
        proc = Process(target=cli_fun, args=i)
        proc.start()

    print("Awaiting execution")

    def test_tasks(master_server):
        n = 0
        while True:
            if master_server.active_task:
                print(".", end="")
            else:
                print(" - ", end="")
                if n > 10:
                    print()
                    break
            n += 1
            time.sleep(3)

    def test_tasks2(master_server, t,case, n1=10, it=False):
        while t.is_alive():
            time.sleep(5)

        print("Starting 2nd test/Negative test")
        n = 0
        print("killing 1  mapper and 1 reducer before we start before we start with pids", mp[0], rp[0])
        Process.kill(mp[0])
        Process.kill(rp[0])
        del mp[0]
        del rp[0]
        test_cases = [
            [host, port, base_path + inputs[0],
             app_path[0],
             base_path + outputs + app_path[0].split(".")[-1] + "_" + inputs[0].split("/")[-1] + "_case"+case+".txt"],
            [host, port, base_path + inputs[1],
             app_path[0],
             base_path + outputs + app_path[0].split(".")[-1] + "_" + inputs[1].split("/")[-1] + "_case"+case+".txt"],
            [host, port, base_path + inputs[0],
             app_path[1],
             base_path + outputs + app_path[1].split(".")[-1] + "_" + inputs[0].split("/")[-1] + "_case"+case+".txt"],
            [host, port, base_path + inputs[1],
             app_path[1],
             base_path + outputs + app_path[1].split(".")[-1] + "_" + inputs[1].split("/")[-1] + "_case"+case+".txt"],
        ]
        for i in test_cases:
            proc = Process(target=cli_fun, args=i)
            proc.start()
        while True:
            if master_server.active_task:
                n += 1
                if n > n1 and it:
                    Process.kill(mp[0])
                    Process.kill(rp[0])
                    del mp[0]
                    del rp[0]
                    it = False
            else:
                if n < 0: n = 1
                print(" - " + str(n) + " - ")
                n += 1
                it = True
                if n > n1:
                    print()
                    print("breaking thread")
                    break
                time.sleep(2)
                continue
            time.sleep(5)

    def shutdown(master_server, t):
        while t.is_alive():
            time.sleep(5)
        del master_server
        print("Tests complete")
        os.kill(os.getpid(), 9)
        exit(0)

    try:

        t1 = threading.Thread(target=test_tasks, args=[master_server])
        t1.start()

        t2 = threading.Thread(target=test_tasks2, args=[master_server, t1, "2", 10])
        t2.start()

        t3 = threading.Thread(target=test_tasks2, args=[master_server, t2, "3", 10, True])
        t3.start()

        t6 = threading.Thread(target=shutdown, args=[master_server, t3]).start()

        master_server.serve_forever()

    except:
        exit(0)
