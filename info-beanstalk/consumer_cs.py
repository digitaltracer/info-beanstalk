""" This script should run on all the hosts in the cluster
  (hosts specified in piconf) This host should be specified in
  piconfig file of producer machine """

import beanstalkc
import nltk
import datetime
from time import sleep
import sys
#type in the host name (generally it'll be localhost as it is the worker)
hostname = 'localhost'
port = 11000 #int
#change it to the value that suits your machine
THREADS_TO_RUN = 100
PRODUCTION_TUBE_NAME = 'info'
FEEDBACK_TUBE = 'ACKS'
ERROR_TUBE = 'error'

    
class Connection:
    bean = beanstalkc.Connection(host=hostname,port=int(port))
    
    jobGiven = False
    
    worker = object
    
def check_completion():
    try:
        connection = beanstalkc.Connection(host=hostname,port=int(port))
        total_jobs = connection.stats_tube(PRODUCTION_TUBE_NAME)['total-jobs']
        jobs_remaining = int(connection.stats_tube(PRODUCTION_TUBE_NAME)['current-jobs-ready'])
        ongoing_jobs = int(connection.stats_tube(PRODUCTION_TUBE_NAME)['current-jobs-reserved'])
        jobs_buried = int(connection.stats_tube(PRODUCTION_TUBE_NAME)['current-jobs-buried'])
        if jobs_remaining or ongoing_jobs:
            return False
        elif jobs_buried:
            print 'there are jobs buried'
            for i in range(total_jobs):
                j = connection.peek(i)
                if j.stats()['state'] == 'buried':
                    b = j.body
                    connection.use(ERROR_TUBE)
                    connection.put(str(b))
                    connection.use(PRODUCTION_TUBE_NAME)
                    connection.kick(i)
            return True
        elif (not jobs_remaining) and (not ongoing_jobs) and (not jobs_buried):
            connection.use(FEEDBACK_TUBE)
            connection.put('COMPLETE')
            #print 'rem -> ',jobs_remaining
            #print 'ong -> ',ongoing_jobs
            #print 'buried -> ',jobs_buried
            return True
    except beanstalkc.UnexpectedResponse,beanstalkc.CommandFailed:
        return False

class Worker:
    #def __init__(self,connection):
    #    self.beanstalk = connection
    #    #tube ACKS is watched by the producer. Helps in writing the log of handled articles
    #    self.beanstalk.use(FEEDBACK_TUBE)
    #    self.beanstalk.watch(PRODUCTION_TUBE_NAME)
        
    def write_log(self,fname,message):
        f = open(fname,'a+')
        f.write(message+'\n')
        f.close()
    
    def _extract_entity_names(self,t):
        """ Takes chunked data and returns only NERs """
        entity_names = []        
        if hasattr(t, 'node') and t.node:
            if t.node == 'NE':
                #print "t=====>" ,t
                #print type(t)
                entity_names.append(' '.join([child[0] for child in t]))
            else:
                for child in t:
                    entity_names.extend(self._extract_entity_names(child))
                    
        return entity_names

    def gen_ners(self,sample):
        """ returns NERS in the sample given as a list """
        sentences = nltk.sent_tokenize(sample)
        tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
        tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
        chunked_sentences = nltk.batch_ne_chunk(tagged_sentences, binary=True)
        entity_names = []
        for tree in chunked_sentences:
                entity_names.extend(self._extract_entity_names(tree))
        unique_ners = list(set(entity_names))
        return unique_ners
    
    def work(self,con):
        #you can mention timeout as argument here; By default its 120s;
        # I figured its more than sufficient to handle one article
        connection = con.bean
        #print connection.using()
        #print connection.watching()
        job = connection.reserve()
        sample = job.body
        #print sample
        #print pprint(self.beanstalk.stats_tube('info'))
        #extracting Id string from the job body
        req_ind = sample.index(')')
        req_ind += 1
        req_ind = int(req_ind)
        id_string = sample[:req_ind]
        article = sample[req_ind:]
        eqlIndex = id_string.index('=') + 1
        idStr_len = len(id_string) - 1
        reqId = int(id_string[eqlIndex:idStr_len])
        logString = "Article Id:"+str(reqId) + "  "
        #print logString
        logString += "Start Time : " + datetime.datetime.now().strftime("%H:%M:%S.%f") + "  "
        #print logString
        #extract NERs from the article
        ners = self.gen_ners(article)
        logString += "End Time : "+datetime.datetime.now().strftime("%H:%M:%S.%f") + "  "
        #print logString
        #delete the job
        job.delete()
        print 'deleted the job'
        num_NERs = len(ners)
        logString += "Number of NERs = "+ str(num_NERs) + "  NER : "
        for NER in ners:
            logString += '"' + NER + '"' + " , "
        #print logString
        con.jobGiven = False
        connection.put(logString)
        #self.write_log('myfile', logString)
        

def main():
    print 'Waiting for work..'
    #w = Worker()
    try:
        while True:
            connectionList = []
            for i in range(THREADS_TO_RUN):
                c = Connection()
                c.bean.use(FEEDBACK_TUBE)
                c.bean.watch(PRODUCTION_TUBE_NAME)
                c.worker = Worker()
                #q = c.connect(hostname,port)
                connectionList.append(c)
            for connection in connectionList:
                if (not connection.jobGiven) and (not check_completion()):
                    connection.jobGiven = True
                    connection.worker.work(connection)
                if check_completion():
                    print 'Waiting for work..'
                    if not connection.jobGiven:
                        connection.jobGiven = True
                        connection.worker.work(connection)
            sleep(1)
    except KeyboardInterrupt:
        print 'Exiting......'
        sys.exit()
    except BaseException,e:
        connection = beanstalkc.Connection(host=hostname,port=port)
        connection.use('error')
        connection.put(str(e))
        connection.close()
        del connection
if __name__ == "__main__":
    main()
