#dave platten 6/20/2019
# queries the jobs table and displays the tasks in priority queues every few seconds

import xml.etree.ElementTree as ET
import os
import shutil
import sys
import datetime as dt
import time
import pyodbc
from datetime import datetime


class CommentedTreeBuilder(ET.TreeBuilder):
    #need this class to keep comments in the xml
    def __init__(self, *args, **kwargs):
        super(CommentedTreeBuilder, self).__init__(*args, **kwargs)
        
    def comment(self, data):
        self.start(ET.Comment, {})
        self.data(data)
        self.end(ET.Comment)

class Queue_item:
    #def __init__(self,ti,si,tpi,wbi,cid,spdt,tt,cd,qfp,pti,tpt,dfb):
    def __init__(self,ti,si,tpi,wbi,cid,spdt,tt,cd,pti,tpt,dfb):

        self.taskId = ti
        self.subjectId = si
        self.timePointId = tpi
        self.workBookId = wbi
        if (cid == None):
            cid = ''        
        self.computerId = cid
        self.startProcTime = spdt
        self.taskTitle = tt
        self.createDate = cd
        #if (qfp == None):
        #    qfp = ''
        #self.queued = qfp
        if (pti == None):
            pti = ''
        self.priority = pti
        self.elapsed = ''
        self.tptitle = tpt
        self.daysfrombaseline = dfb
        self.item_str =''

        self.format()
      

    def format(self):
        #a = '{0:<6}{1:<15}{2:>6}{3:1}{4:1}{5:1}{6:3}'.format((self.taskId + self.subjectId + self.timePointId + self.workBookId), self.taskTitle, self.elapsed, "", self.queued, "", self.computerId)  #
        a = '{0:<7}{1:<13}{2:>12}{3:>6}{4:>9}{5:>3}'.format(str(self.taskId + self.subjectId + self.timePointId + self.workBookId)[0:6], self.taskTitle[0:12], self.tptitle[0:11], str(self.daysfrombaseline)[0:5], str(self.elapsed)[0:8], str(self.computerId)[0:2])  #50
        self.item_str = '{0:51}'.format(a)  #50
        

class PrintAndLog:

    def __init__(self,lfname):  #constructor
        self.logFileName = lfname #includes full path
        self.logFile = open(self.logFileName, mode='w')

    def __del__(self): #destructor
        self.logFile.close()

    def print(self, s1):
        print(s1)

    def log(self, s1):
        self.logFile.write(s1 + "\n")

    def pnl(self, s1):
        self.print(s1)
        self.log(s1)



class query_jobs_class:

    blank = Queue_item('','','','','','','','','','','')
 
    def __init__(self):
        self.config_path = r'dp_RAIS_load_config.xml'
        self.config_tree, self.config_root = self.read_xml_file(self.config_path)
        self.num_tasks = int(self.find_tag(self.config_tree, 'Num_tasks')[2])
        self.task_burst = int(self.find_tag(self.config_tree, 'Task_burst')[2])
        self.cycle_time = int(self.find_tag(self.config_tree, 'Cycle_time')[2])
        self.task_names = self.find_tags(self.config_tree, 'Task_names')
        self.server_name = self.find_tag(self.config_tree, 'Server_name')[2]
        self.database_name = self.find_tag(self.config_tree, 'Database_name')[2]
        self.snapshot_time = self.find_tag(self.config_tree, 'snapshot_time')[2]
        self.drain_queue = int(self.find_tag(self.config_tree, 'Drain_Queue')[2])
        self.run_time = int(self.find_tag(self.config_tree, 'Run_Time')[2])        

        
        self.conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server=" + self.server_name + ";"
                                   "Database=" + self.database_name + ";"
                                   "Trusted_Connection=yes;")
        
        self.conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        self.conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        self.conn.setencoding(encoding='utf-8')
        
        self.cursor = self.conn.cursor()
        self.fastlane = []
        self.high = []
        self.medium = []
        self.low = []
        self.gridsize = 35

        self.pnl = PrintAndLog('C:\RAIS_loadtest_log_' + datetime.today().strftime('%Y_%m_%d') + ".txt")
        
 
    def read_xml_file(self, file):
        parser = ET.XMLParser(target=CommentedTreeBuilder()) #keeps the comments
        tree = ET.parse(file, parser)
        root = tree.getroot()
        return tree, root


               
  
  
    def find_tag(self, curr_tree, curr_tag):  #return single value
        result = ''
        for elem in curr_tree.iterfind('.//' + curr_tag):
            result = (elem, elem.tag, elem.get('value'))
            break
        return result


    def find_tags(self, curr_tree, curr_tag):  #return multiple values
        result = []
        for elem in curr_tree.iter(tag = curr_tag):
            for elem2 in elem:
                result.append(elem2)
        return result


          

    def find_files(self):
   
        today = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        today_str = dt.datetime.fromtimestamp(time.time()).strftime('%Y_%m_%d_%H_%M_%S')


    def process_row(self, r1):
        a,b,c,d,e,f,g,h,i,j,k = r1
        qitem = Queue_item(a,b,c,d,e,f,g,h,i,j,k)
        now = dt.datetime.now()
        #print("now : " + str(now))
        #print("h   : " + str(h))
        
        t = 0
        t = (now - h)
        if (t.seconds > 80000):
            qitem.elapsed = 0
        else:
            qitem.elapsed = str(t)[0:-7]

        
        qitem.format()
        if (qitem.priority == 4):
            self.fastlane.append(qitem)
        elif (qitem.priority==1):
            self.high.append(qitem)
        elif (qitem.priority==2):
            self.medium.append(qitem)
        elif (qitem.priority==3):
            self.low.append(qitem)
        else:
            pass
        
        


    def query_jobs(self):
        today = dt.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        self.fastlane = []
        self.high = []
        self.medium = []
        self.low = []
        self.gridsize = 35 #default, don't go lower

        self.pnl.pnl(today)
        num_jobs = 0
        self.cursor.execute('SELECT COUNT([TaskTitle]) FROM Jobs')
        for row in self.cursor:
            if row:
                num_jobs = int(str(row)[1:-3])
                
        self.pnl.pnl("")
        self.pnl.pnl("                     Tasks in Jobs table: " + str(row)[1:-3] + "                                                  Snapshot every : " + self.snapshot_time + " seconds")
        self.pnl.pnl("")

 
        
        if (num_jobs > 35):
            self.gridsize = num_jobs
        
       
        if (num_jobs > 0):
            sql_cmd = """SELECT j.[TaskId],
                        j.[SubjectId],
                        j.[TimePointId],
                        j.[WorkBookId],
                        j.[ComputerId],
                        j.[StartProcessingDateTime],
                        j.[TaskTitle],
                        j.[CreateDate],
                        j.[PriorityTypeId],
                        j.[TimePointTitle],
                        tp.[DaysFromBaseline]
                        FROM Jobs j INNER JOIN [TimePoint] tp on j.TimePointId = tp.TimePointId"""
            
            self.cursor.execute(sql_cmd)
            for row in self.cursor:
                self.process_row(row)


        self.sort_queues()                    

        self.fill_fastlane()                    
        self.fill_high()                    
        self.fill_medium()                    
        self.fill_low()

        self.show_queues()
            

    def get_fastlane(self):
        for x in self.fastlane:
            yield x
    
    def get_high(self):
        for x in self.high:
            yield x
            
    def get_medium(self):
        for x in self.medium:
            yield x
            
    def get_low(self):
        for x in self.low:
            yield x


    def fill_fastlane(self):
        while (len(self.fastlane) <self.gridsize):
            self.fastlane.append(query_jobs_class.blank)

    def fill_high(self):
        while (len(self.high) <self.gridsize):
            self.high.append(query_jobs_class.blank)

    def fill_medium(self):
        while (len(self.medium) <self.gridsize):
            self.medium.append(query_jobs_class.blank)

    def fill_low(self):
        while (len(self.low) <self.gridsize):
            self.low.append(query_jobs_class.blank)


    def sort_queue(self,q1):
        result = []
        tmp = sorted(q1, key=lambda x: (x.elapsed), reverse = True) #sort desc
        result = sorted(tmp, key=lambda x: (x.daysfrombaseline)) #sort asc
        
        return result

      

    def sort_queues(self):
        self.fastlane = self.sort_queue(self.fastlane)
        self.high = self.sort_queue(self.high)
        self.medium = self.sort_queue(self.medium)
        self.low = self.sort_queue(self.low)

    

    def show_queues(self):
        
        self.pnl.pnl('{0:<51}{1:<51}{2:<51}{3:<51}'.format('FASTLANE', 'HIGH', 'MEDIUM', 'LOW'))
        self.pnl.pnl('')
        fast,high,med,low = self.get_fastlane(), self.get_high(), self.get_medium(), self.get_low()
        for x in range(self.gridsize):
            self.pnl.pnl('{0:<50}{1:<50}{2:<50}{3:<50}'.format(fast.__next__().item_str, high.__next__().item_str, med.__next__().item_str, low.__next__().item_str))
            



    

obj1 = query_jobs_class()
start = time.time()
while ((time.time() - start) < (obj1.run_time + obj1.drain_queue)):
    obj1.query_jobs()
    time.sleep(int(obj1.snapshot_time))
    
obj1.pnl.pnl("Test complete.")



