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


mainProc = ProcTest()
mainProc.start()
mainProc.join()