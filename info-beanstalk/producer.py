#! /usr/bin/python
import beanstalkc
import piconfig
import ijson
from time import sleep
import sys

class Producer:
    #connection list to produce work
    connection = []
    #connection list to watch the feedback tube
    prod_connection = []
    availHosts = piconfig.availHosts
    try:
        jsonF = open(piconfig.ARTICLE_FILE)
        parser = ijson.parse(jsonF)
    except IOError:
        print "No such Article File"
        sys.exit()
    
    def write_log(self,fname,message):
        f = open(fname,'a+')
        f.write(message+'\n')
        f.close()
    
    def create_connections(self):
        """ creates connection objects and appends them to the availHosts list
            generates tubes names for each of these connections
         """
        for host in self.availHosts:
            try:
                beanstalk = beanstalkc.Connection(host=host['hostname'],port=host['port'])
                beanstalk.use(piconfig.TUBE_NAME)
                beanstalk.watch(piconfig.TUBE_NAME)
                #beanstalk.watch(piconfig.ERROR_TUBE_NAME)
                self.connection.append(beanstalk)
                #connection to the feedback tube
                b2 = beanstalkc.Connection(host=host['hostname'],port=host['port'])
                b2.watch(piconfig.PRODUCER_TUBE_NAME)
                self.prod_connection.append(b2)
            except beanstalkc.SocketError,e:
                self.write_log('error_log', e)
    
    def create_job(self,connection):
        """ Takes the json stream and extracts the string and puts it to the queueu """
        created = False
        while not created:
            q = self.parser.next()
            if q[1] == 'string':
                value = q[2]
                value = str(value)
                value = "(id="+str(piconfig.ARTICLE_ID)+")"+value
                piconfig.ARTICLE_ID += 1
                connection.put(value)
                created = True
            else:
                pass
    
    def logWork(self,connection):
        connection.watch(piconfig.PRODUCER_TUBE_NAME)
        jobs_ready = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-ready'])
        while jobs_ready:
            j = connection.reserve()
            print j.body
            if j.body == 'COMPLETE':
                print "yea"
                print piconfig.LOADING_COMPLETE
                #self.connection.remove(connection)
                #self.prod_connection.remove(connection)
                if piconfig.LOADING_COMPLETE:
                    j.delete()
                    self._finish_logging(connection)
                jobs_ready = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-ready'])
            else:
                self.write_log(piconfig.PRODUCER_LOG_FILE, j.body)
                j.delete()
                jobs_ready = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-ready'])
    
    
    def error_processing(self,connection):
        print "entered error processing"
        #this connection will be deleted after this; so no need to switch back use to ACKS
        connection.use(piconfig.ERROR_TUBE_NAME)
        for i in range(int(connection.stats_tube(piconfig.ERROR_TUBE_NAME)['total-jobs'])):
            j = connection.peek(i)
            msg = j.body
            self.write_log(piconfig.WORKERS_COMBINED_ERROR_FILE, msg)
            j.delete()
    
    def _finish_logging(self,connection):
        print 'entered finish logging'
        total_jobs = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['total-jobs'])
        jobs_remaining = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-ready'])
        reserved_jobs = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-reserved'])
        jobs_buried = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-buried'])
        flag = False
        while not flag:
            if jobs_remaining or reserved_jobs:
                j = connection.reserve()
                self.write_log(piconfig.PRODUCER_LOG_FILE, j.body)
                j.delete()
                jobs_remaining = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-ready'])
                reserved_jobs = int(connection.stats_tube(piconfig.PRODUCER_TUBE_NAME)['current-jobs-reserved'])
            elif jobs_buried:
                for i in range(total_jobs):
                    j = connection.peek(i)
                    if j.stats()['state'] == 'buried':
                        b = j.body
                        self.write_log(piconfig.WORKERS_COMBINED_ERROR_FILE, b)
                        connection.kick(i)
            elif (not jobs_remaining) and (not reserved_jobs) and (not jobs_buried):
                from pprint import pprint
                pprint(connection.stats_tube('ACKS'))
                self.error_processing(connection)
                self.prod_connection.remove(connection)
                flag = True
        
                
                
            
class Monitor:
    def get_queue_status(self,connection):
        """ returns true/false; true if queue is getting empty ; false otherwise """
        jobs_remaining = connection.stats_tube(piconfig.TUBE_NAME)['current-jobs-ready']
        if jobs_remaining < int(piconfig.numOfJobs):
            return True
        else:
            return False
    
    def monitor(self):
        """ Main method . monitors the producer """
        producer = Producer()
        #call this method, else connections will be empty list
        producer.create_connections()
        print producer.connection
        while True:
            for connection in producer.connection:
                if self.get_queue_status(connection):
                    try:
                        #create 20 jobs
                        for i in range(int(piconfig.numOfJobs)):
                            producer.create_job(connection)
                    except StopIteration:
                        #producer.finish_logging(connection)
                        piconfig.LOADING_COMPLETE = True
            for connection in producer.prod_connection:
                producer.logWork(connection)
            
            if not len(producer.prod_connection):
                print 'JOB COMPLETED'
                sys.exit()
            
            sleep(piconfig.monitoringInterval)
            

if __name__ == "__main__":
    try:
        m = Monitor()
        m.monitor()
    except KeyboardInterrupt:
        print '\nMaster wants me to quit.'
        sys.exit()
                