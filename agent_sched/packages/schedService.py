# -*- coding: utf-8 -*-
"""
    The schedulerService runs on the scheduler head node, where the job scheuler master process is runnign
    then it collects the required data from the job scheduler and publish them on the RabbitMQ Queue

        - By now it only works with Univa Grid Engine (UGE)

 Misha ahmadian (misha.ahmadian@ttu.edu)
"""
from schedConfig import SchedConfig, SchedConfReadExcetion
from threading import Thread, Event
from queue import Queue

class SchedJobInfo(Thread):
    def __init__(self, sched_Q: Queue):
        Thread.__init__(self)
        self.exit_flag = Event()
        self.config = SchedConfig()
        self.sched_Q = sched_Q

    def run(self) -> None:
        while not self.exit_flag.is_set():
