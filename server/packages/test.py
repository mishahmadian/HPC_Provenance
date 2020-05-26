myStr = """
    job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }

job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }

job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }
"""
myStr2 = """
    job_stats:
- job_id:          genius_uge_70
  snapshot_time:   1565883714
  read_bytes:      { samples:           0, unit: bytes, min:       0, max:       0, sum:               0 }
  write_bytes:     { samples:         768, unit: bytes, min: 4194304, max: 4194304, sum:      3221225472 }
  getattr:         { samples:           0, unit:  reqs }
  setattr:         { samples:           0, unit:  reqs }
  punch:           { samples:           0, unit:  reqs }
  sync:            { samples:           0, unit:  reqs }
  destroy:         { samples:           0, unit:  reqs }
  create:          { samples:           0, unit:  reqs }
  statfs:          { samples:           0, unit:  reqs }
  get_info:        { samples:           0, unit:  reqs }
  set_info:        { samples:           0, unit:  reqs }
  quotactl:        { samples:           0, unit:  reqs }
"""
from threading import Thread, Event, Timer
import time
import datetime

def myfilter():
    result = myStr.split("job_stats:")
    del result[0]
    for data in result:
        print("This is split data-->")
        for line in data.splitlines():
            if not line.strip():
                continue
            print(line)

def mylist():
    for line in myStr.splitlines():
        attr = line.split(':')[0].strip()
        if "_bytes" in attr:
            attr_ext = {"" : 2, "_min" :4 , "_max" : 5, "_sum" : 6}
            for ext in attr_ext:
                inx = attr_ext[ext]
                objattr = attr + ext
                delim2 = '}' if  inx == 6 else ','
                value = line.split(':')[inx].split(delim2)[0].strip()
                print(objattr + " = " + value)

timestamp = 0;

class time_read(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.flag = Event()

    def run(self):
        global timestamp
        while not self.flag.is_set():
            timestamp = time.time()
            self.flag.wait(10)


def main():
    tr = time_read()
    tr.start()

    while True:
        print("time is: " + str(timestamp))
        time.sleep(5)

def date_conv():
    time_str = "16:04:31.505817780"
    date_str = "2019.08.28"
    datetime_str = time_str[:-3] + " " + date_str
    date_time_obj = datetime.datetime.strptime(datetime_str, '%H:%M:%S.%f %Y.%m.%d')
    mytimestamp = datetime.datetime.timestamp(date_time_obj)

    print('Date:', date_time_obj.date())
    print('Time:', date_time_obj.time())
    print('Date-time:', date_time_obj)
    print('timestamp:', mytimestamp)

#date_conv()
from multiprocessing import Process, Event, Manager
import time

class ProcTest(Process):
    def __init__(self):
        Process.__init__(self)
        self.event_flag = Event()
        self.timesUp = Event()

    def run(self):
        while not self.event_flag.is_set():
            procList = []
            myDict = Manager().dict()
            myDict[123] = 1
            self.timesUp.clear()

            procList.append(Process(target=self.__test1, args=(myDict,)))
            procList.append(Process(target=self.__test2, args=(myDict,)))
            procList.append(Process(target=self.__test3, args=(myDict,)))

            for proc in procList:
                proc.start()

            self.event_flag.wait(5)
            self.timesUp.set()

            for proc in procList:
                proc.join()
            print("======= " + str(myDict[123]))

    def __test1(self, myDict):
        while not self.timesUp.is_set():
            myDict[123] += 1
            print("I'm test 1 @ " + str(myDict[123]))
            self.timesUp.wait(1)

    def __test2(self, myDict):
        while not self.timesUp.is_set():
            myDict[123] += 1
            print("I'm test 2 @ " + str(myDict[123]))
            self.timesUp.wait(4)

    def __test3(self, myDict):
        while not self.timesUp.is_set():
            myDict[123] += 1
            print("I'm test 3 @ " + str(myDict[123]))
            self.timesUp.wait(1)


#mainProc = ProcTest()
#mainProc.start()
#mainProc.join()

from typing import Type
class Config:

    def getValue(self, sec: str, key: str, dataType: Type):
        attrName = sec + '_' + key
        if not hasattr(self, attrName):
            print("New value!")
            setattr(self, attrName, "Hello..")
            print(dataType("123"))
            print(dataType.__name__)
        return getattr(self, attrName)

#conf = Config()
#print(conf.getValue('sec1', 'key1', float))
#print(conf.getValue('sec1', 'key2'))
#print(float("123b4"))
from multiprocessing import Pool
from math import ceil

class ProcTest2(Process):
    def __init__(self):
        Process.__init__(self)
        self.event_flag = Event()
        self.timesUp = Event()
        self.myLst = [(0,1), (4,2), (7,3), (9,4)]

    def run(self):
        pool = Pool(2)
        chunkSize = ceil(len(self.myLst)/2)
        #print (chunkSize)
        results = pool.imap_unordered(self.doSomething, self.myLst, chunksize=chunkSize)
        pool.close()
        pool.join()

        for num, text in results:
            print("number {} * 2 = {}".format(num, text))

    @staticmethod
    def doSomething(t):
        x, y = t
        print(str(x) + " " + str(y))
        return y, str(x * 2)

#procTest2 = ProcTest2()
#procTest2.start()
#procTest2.join()

'''
for inx, rec in enumerate(recs):
    if (inx == len(recs) - 1) and ("=" not in rec):
        print("index={}  rec={}".format(inx, rec))
        chlogRec = "96 06UNLNK 21:06:00.508754493 2019.03.07 0x1 t=[0x200000403:0x60:0x0] j=rm.1000 
                    ef=0xf u=0:0 nid=0@<0:0> p=[0x200000405:0x2:0x0] test2.txt"
        recs = chlogRec.split(' ')
'''
import subprocess
from multiprocessing import Event as P_Event, Pool, Queue


def procTest(dummy):
    #time.sleep(2)
    print("its funny")
    return None
class ProcTest3(Process):
    def __init__(self):
        Process.__init__(self)
        self.mdtTarget = "test-MDT0000"
        self.startRec = 0
        self.event = P_Event()
        self.testList = [i for i in range(2)]

    def run(self):
        while not self.event.is_set():
            result = self.__test(self.mdtTarget, self.startRec)
            self.startRec = 1811

            print(self.testList)
            pool = Pool(10)
            pool.map(procTest, self.testList, chunksize=2)
            pool.close()
            pool.join()

            if not result:
                print("empty")
            else: print(result)

            self.event.wait(2)

    def __test(self, mdtTarget: str, startRec: int) -> str:
        #return subprocess.check_output("lfs changelog " + mdtTarget + " " + str(startRec + 1),
        #                                 shell=True).decode("utf-8")
        return subprocess.check_output("lfs changelog " + mdtTarget + " " + str(startRec + 1),
                                       shell=True, stderr=subprocess.STDOUT).decode("utf-8")


#procTest3 = ProcTest3()
#procTest3.start()
#procTest3.join()
from typing import List
class myObj1:
    def __init__(self):
        self.name = None
        self.lastname = None
        self.num = 0

    def __lt__(self, other):
        return self.num < other.num

class myObj2:
    def __init__(self):
        self.id = 0
        self.text = None
        self.obj1Lst: List[myObj1] = []

    def insert(self, myobj1):
        objlist = self.obj1Lst
        objlist.append(myobj1)
        print(self.obj1Lst)
from typing import Dict

import json, urllib.request

#r = urllib.request.urlopen("http://10.102.14.17:8182/jobs/79")
#data = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))
#print(json.dumps(data, sort_keys=True, indent=4))



from enum import Enum
class enumTest:
    class inner(Enum):
        MISHA = 1
        AHMADIAN = 2

    def testMe(self, val):
        print(enumTest.inner(val))

import types
from inspect import isfunction
class MyObject:
    def __init__(self):
        self.id = "123"
        self.name = "misha"
        self.lastname = "ahmadian"

    def test(self):
        pass

myobj = MyObject()

for attr in [atr for atr in dir(myobj) if (not atr.startswith('__'))
                                          and (not callable(getattr(myobj, atr)))]:
    #print(attr + " --> " + str(getattr(myobj, attr)))
    pass

#lfs_comm = subprocess.check_output("lfs changelog test-MDT0000 1", shell=True).decode("utf-8")
#print(lfs_comm)

import multiprocessing

def inProcFunc():
    myId = multiprocessing.current_process().pid
    print("Inner started: " + str(myId))
    time.sleep(5)
    print("Inner finished: " + str(myId))

def procFunc():
    innerProc = None
    myId = multiprocessing.current_process().pid
    print("I started: " + str(myId))
    try:
        innerProc = Process(target=inProcFunc)
        innerProc.start()
        innerProc.join()
        print("I finished: " + str(myId))
    finally:
        innerProc.terminate()
        innerProc.join()

class ProcTest(Process):

    def run(self) -> None:
        procLst = []
        while True:
            try:
                myProc = Process(target=procFunc)
                procLst.append(myProc)
                myProc.daemon = False
                myProc.start()
                time.sleep(3)
            except KeyboardInterrupt:
                print("I'm done")

            finally:
                for proc in procLst:
                    proc.terminate()

from communication import ServerConnection, CommunicationExp


def listener(ch, method, properties, body):
    if body.strip():
        print("New Body")

ch, conn = None, None
try:
    pass
    #comm = ServerConnection()
    #conn, ch = comm.Collect_io_stats(listener)

except CommunicationExp as commExp:
    print(commExp.getMessage())

except KeyboardInterrupt:
    ch.cancel()
    #conn.close()

except Exception as exp:
    print(str(exp))


#print(str(['1', '2']))

# jobid = '123'
# taskid = None
# print('.'.join(filter(None, [jobid, taskid])))

# from multiprocessing import Process, Manager
# from multiprocessing.managers import BaseManager, NamespaceProxy
# mydict = Manager().dict()
# mylist =  Manager().list()
# #mydict.__setitem__("test", mylist)
# mydict._callmethod('__setitem__', ("test", mylist,))
# mydict._callmethod('__getitem__', ("test",))._callmethod('append', ("12",))
# #mylist._callmethod('append', '12')
# print(mydict._callmethod('__getitem__', ("test",)))
# #mylstlen = mylist._callmethod('__len__')
# #print(mylist.__str__())
# print(mydict)



from multiprocessing import Process, Manager, Value, Lock, Queue
from multiprocessing.managers import BaseManager, NamespaceProxy, SyncManager

class JobInfo2(object):
    def __init__(self):
        self.myValue = []

    def insertValue(self, value):
        self.myValue.append(value)

    def getLst(self):
        return self.myValue

class MyJobManager(SyncManager):
    pass


class MyJobProxy(NamespaceProxy):
    _exposed_ = ('__getattribute__', '__setattr__', '__delattr__', 'insertValue', 'getLst')

    def insertValue(self, value):
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod(self.insertValue.__name__, (value,))

    def getLst(self):
        callmethod = object.__getattribute__(self, '_callmethod')
        return callmethod(self.getLst.__name__)

def procExec(mydict, t_id, manager, lock):
    #id_val.value = (1 if t_id % 2 == 0 else 2)
    _id = (1 if t_id % 2 == 0 else 2)
    with lock:
        if not mydict.get(_id):
        #if not mydict._callmethod('__getitem__', (_id,)):
            print("I'm here tid={}  _id={}".format(str(t_id), str(_id)))
            #mydict._callmethod('__setitem__', (_id, manager.JobInfo2(),))
            mydict[_id] = manager.JobInfo2()
            #mydict._callmethod('__setitem__', (_id, [],))
            #print("I'm here 2")
    #else:
    mydict._callmethod('__getitem__', (_id,)).insertValue(t_id)
        #mydict._callmethod('__getitem__', (_id,)).append(t_id)
    myjobinfo2 = mydict._callmethod('__getitem__', (_id,))
    print("The Thread_{} with dic_id={} has this list: {}".format(t_id, _id, str(myjobinfo2.getLst())))
    #print("The Thread_{} with dic_id={} has this list: {}".format(t_id, _id, str(myjobinfo2)))


# MyJobManager.register('JobInfo2', JobInfo2, MyJobProxy)
#
# jobInfoMngr = MyJobManager()
# jobInfoMngr.start()
#
# #jobInfo2 = jobInfoMngr.JobInfo2()
# myprocdict = Manager().dict()

# pool = multiprocessing.Pool(multiprocessing.cpu_count())
# for i in range(multiprocessing.cpu_count()):
#     pool.apply(func = procExec, args = (myprocdict, i))
# pool.close()
# pool.join()

# procList: List[Process] = []
# mylock = Lock()
# my_val = Value('i', 0)
# myrange = multiprocessing.cpu_count()
# #myrange = 1
# for i in range(myrange):
#     procList.append(Process(target=procExec, args=(myprocdict, i, jobInfoMngr, mylock,)))

# for proc in procList:
#     #proc.daemon = True
#     #time.sleep(0.5)
#     proc.start()
# for proc in procList:
#     proc.join()

# for key in myprocdict:
#     item = myprocdict.get(key)
#     print(str(item.getLst()))

class outtest:
    def __init__(self):
        intest = self._intest()
        print(str(intest.myid))

    class _intest:
        def __init__(self):
            self.myid = 1234

# myouttest = outtest()
#
# mydict = {}
# if  not mydict.get("test"):
#     print("Does not exist")
# else:
#     print("Does exist")
from multiprocessing import Process
from queue import LifoQueue

def produce_me(queue1, queue2):
    value = 0
    while True:
        print("Producer --> " + str(value))
        queue1.put(value)
        queue2.put(value * 2)
        value += 1
        time.sleep(2)

def consume_me(queue1, queue2):
    while True:
        print("Q1 Size: " + str(queue1.qsize()))
        print("Q2 Size: " + str(queue2.qsize()))
        result1 = []
        result2 = []
        # for i in iter(queue.get, 'STOP'):
        #     result.append(i)
        while queue1.qsize():
            result1.append(queue1.get())

        while queue2.qsize():
            result2.append(queue2.get())

        print("consumer --> R1=" + str(result1) + "  R2=" + str(result2))
        time.sleep(5)

class QManager(BaseManager):
    pass

import fcntl

class Write_to_File(object):
    def __init__(self):
        self.myfile = './testfile'

    def writeFile(self, myid, stime):
        f = open(self.myfile, 'a+')
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            print("[{}] started writing to file".format(str(myid)))
            msg = "The procees ID: {} is writing at this time: {}\n\n".format(str(myid), str(time.time()))
            f.write(msg)
            print("[{}] finished writing to file".format(str(myid)))
            time.sleep(stime)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)
            f.close()

def file_store(myid, stime):
    fwrie = Write_to_File()
    while True:
        fwrie.writeFile(myid, stime)



# proc1 = Process(target=file_store, args=(1, 1,))
# proc2 = Process(target=file_store, args=(2, 1,))
#
# proc1.start()
# #time.sleep(0.5)
# proc2.start()
#
# proc1.join()
# proc2.join()

import sys
accounting = 'all.q:compute-14-15.localdomain:users:misha:Test:475:sge:0:1581626412597:1581626415789:1581626462209:100:137:46.420:0.011:0.037:10096:0:0:0:0:1899:0:0:8:272:0:0:0:205:10:NONE:defaultdepartment:sm:1:0:0.047:0.001619:0.000052:-q all.q -l h_vmem=1.200G -pe sm 1 -binding no_job_binding 0 0 0 0 no_explicit_binding:1.170000:NONE:226451456:0:0:NONE:misha@genius.hpcc.ttu.edu:0:0:genius.hpcc.ttu.edu:/home/misha/uge:qsub sleep.sh:50.309000:514:NONE'
acctRec = accounting.split(':')
#print(str(acctRec[int(sys.argv[1])]))

# myq = "all.q@compute-1"
# queue = 'test'
# host = myq.split(',')
# print(queue + "  " + str(host))
from tabulate import tabulate
table = [['mydata', 'misha', '*', 'test', 'test2'], ['mydata', 'misha', '*', 'test', 'test2'], ['mydata', 'misha', '*', 'test', 'test2']]
#print(tabulate(table, headers=["Job Items:", "Value:", "*", "OSS Items:", "Value:"], tablefmt="fancy_grid"))

from db_manager import MongoDB

# mongodb = MongoDB()
# query = {"name" : "mytest", "lastname" : "testy1"}
# data = {"name" : "test", "lastname" : "testy"}
# mongodb.update(MongoDB.Collections.JOB_INFO_COLL, query, data)
# mongodb.close()

class TestObj(object):
    def __init__(self):
        self.id = 123
        self.name = 'misha'
        self.last = 'ahmadian'

    def to_dict(self) -> dict:
        attrDict = {}
        # collect all available attributes
        attrs = [atr for atr in dir(self) if (not atr.startswith('__')) and (not callable(getattr(self, atr)))]
        for atr in attrs:
            attrDict[atr] = getattr(self, atr)
        #
        return  attrDict

class TestObj2(TestObj):
    def __init__(self):
        TestObj.__init__(self)
        self.extera1 = '456'
        self.extera2 = 890
        self.my2 = None


from scheduler import JobInfo
from enum import Enum


# from config import ServerConfig
# chlRecs = {'test-MDT0000' : [12, 36, 90, 34, 45]}
# config = ServerConfig()
# mdtTargets = config.getMdtTargets()
# chLogUsers = config.getChLogsUsers()
#
# for mdtTarget in chlRecs:
#     # find the corresponding Changelog User of the given MDT
#     user = chLogUsers[mdtTargets.index(mdtTarget)]
#     # Get the last record that should be deleted
#     endRec = max(chlRecs.get(mdtTarget))
#
#     print(f"user: {user}  endRec={endRec}")

# from communication import ServerConnection
# import json
#
# request = json.dumps({'action' : 'uge_acct', 'data' : ['125.0', '675.1', '570.0', '133.0', '0.0']})
# print("I'm Client: " + str(request.split('[^@]')))
# serverCon = ServerConnection(is_rpc=True)
# response = serverCon.rpc_call("genius_rpc_queue", request)
# print("response -> " + response)

#import logger

#logger.log(logger.Mode.APP_ASTART, "************ App Started ***********")
#logger.log(logger.Mode.AGGREGATOR, "[Error]_ Something bad happened")

from db_manager import MongoDB
# mongodb = MongoDB()
# query = {"uid" : "ffb1208ee27fe68a27a7bf4d4494f7a8"}
# docs = mongodb.find_one(MongoDB.Collections.MDS_STATS_COLL, query)
# print(str(docs))

from db_manager import MongoDB
mydata = {
    "uid" : "68b8826c56367b766a01e3b4eddc99dc",
    "cluster" : "genius",
    "command" : None,
    "cpu" : None,
    "end_time" : None,
    "exec_host" : None,
    "failed_no" : None,
    "h_rt" : None,
    "h_vmem" : None,
    "io" : None,
    "ioops" : None,
    "iow" : None,
    "jobName" : None,
    "jobid" : "702",
    "maxvmem" : None,
    "mem" : None,
    "num_cpu" : None,
    "parallelEnv" : None,
    "project" : None,
    "pwd" : None,
    "q_del" : [

    ],
    "queue" : None,
    "s_rt" : None,
    "sched_type" : "uge",
    "start_time" : None,
    "status" : "UNDEF",
    "submit_time" : None,
    "taskid" : None,
    "username" : None,
    "wallclock" : None
}

myoss = {
    "uid" : "afdb80123f618e2367533a38381e0682",
    "cluster" : "genius",
    "create" : 0,
    "destroy" : 0,
    "getattr" : 0,
    "jobid" : "706",
    "oss_host" : "oss1",
    "ost_target" : "test-OST0001",
    "procid" : None,
    "punch" : 0,
    "read_bytes" : 0,
    "read_bytes_max" : 0,
    "read_bytes_min" : 0,
    "read_bytes_sum" : 0,
    "sched_type" : "uge",
    "setattr" : 0,
    "snapshot_time" : 1586910283,
    "sync" : 0,
    "taskid" : None,
    "timestamp" : 1586910310.754752,
    "write_bytes" : 256,
    "write_bytes_max" : 8888888,
    "write_bytes_min" : 3333333,
    "write_bytes_sum" : 1073741824
}

query = {"uid" : myoss["uid"], 'oss_host': myoss["oss_host"], 'ost_target': myoss["ost_target"]}

new_update = [
    {"$set" : {
        "write_bytes_sum" : {
            "$sum" : [
                "$write_bytes_sum",
                {"$cond" : [
                    {"$ne" : ["$snapshot_time", myoss.get("snapshot_time")]},
                    myoss.get("write_bytes_sum"),
                    0
                ]}
            ]
        }
    }}
]

update_q = {
    "update" : "oss_stats",
    "updates" : [{
        "q" : query,
        "u" : [
            {"$set" : myoss}
        ],
        "upsert" : True,
        "multi" : False
    }],
    "ordered": False
}

myoss['write_bytes_sum'] = {
    "$sum" : [
        "$write_bytes_sum",
        {"$cond" : [{"$ne" : ["$snapshot_time", myoss.get("snapshot_time")]}, myoss.get("write_bytes_sum"), 0 ]}
    ]
}

myoss['write_bytes_max'] = {
    "$max" : ["$write_bytes_max", myoss.get("write_bytes_max")]
}

myoss['write_bytes_min'] = {
    "$min" : ["$write_bytes_min", myoss.get("write_bytes_min")]
}

myoss["insert_time"] = {
    "$cond" : [{"$not" : ["$insert_time"]}, "$$NOW", "$insert_time" ]
}

myoss["update_time"] = "$$NOW"

update_data = [
    {"$set" : myoss}
]

#
# print(str(b_update_q))
# sys.exit(0)

# mongodb = MongoDB()
# # mongodb.prepare()
# #mongodb.insert(MongoDB.Collections.JOB_INFO_COLL, mydata)
# if mongodb.update(MongoDB.Collections.OSS_STATS_COLL, query, update_data, runcommand=True) > 0:
#     print("Successful")
#
# mongodb.close()

# {
#     "$cond" : {
#         "if" : { "$ne" : ["$timestamp", myoss.get("timestamp")] },
#         "then" : {
#             "$max" : {
#                 "read_bytes_max" : myoss.pop("read_bytes_max"),
#                 "write_bytes_max" : myoss.pop("write_bytes_max")
#             },
#         }, "else" : {
#             None
#         }
#     }
# },

# update_q = {
#         #"$set" : { "write_bytes_max" : {"$max" : {"write_bytes_max" : myoss["write_bytes_max"]}} }
#         #"$set" : {"write_bytes_max" : myoss["write_bytes_max"]}
#         "$max" : {"write_bytes_max" : myoss["write_bytes_max"]},
#         "$inc" : {"write_bytes_sum" : myoss["write_bytes_max"]}
#     }

# update_q = {
#
#     "$set" : myoss,
#     "$max" : {"read_bytes_max" : myoss.pop("read_bytes_max"), "write_bytes_max" : myoss.pop("write_bytes_max")},
#     "$min" : {"read_bytes_min" : myoss.pop("read_bytes_min"), "write_bytes_min" : myoss.pop("write_bytes_min")}
# }
# from pprint import PrettyPrinter
# pp = PrettyPrinter(indent=4)
# pp.pprint(update_q)

# from db_manager import InfluxDB
#
# influxdb = InfluxDB()
#
# try:
#     json_point = [{
#         "measurement": "oss",
#         "tags": {"test_tag": "Misha"},
#         "fields": {"test_field": "Ahmadian"},
#     }]
#
#     #influxdb.insert(json_point)
# except Exception as exp:
#     print(str(exp))
# finally:
#     influxdb.close()

from uge_service import UGE
from scheduler import UGEJobInfo

jobinfo = UGEJobInfo()
jobinfo.jobid = "869"
# jobinfo.exec_host = "compute-14-15.localdomain"
# jobinfo.pwd = "/home/misha/uge"
# jobinfo.command = "qsub sleep.sh"
spool_dir = "/export/uge/default/spool"

jobscript = UGE.getJobScript(jobinfo, spool_dir)

print(jobscript)



