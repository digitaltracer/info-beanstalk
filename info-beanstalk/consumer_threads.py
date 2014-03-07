""" This script should run on all the hosts in the cluster
  (hosts specified in piconf) This host should be specified in
  piconfig file of producer machine """

import threading
import beanstalkc
import nltk
import datetime
from time import sleep
import sys
#type in the host name (generally it'll be localhost as it is the worker)
hostname = 'localhost'
port = 11000 #int
#change it to the value that suits your machine
THREADS_TO_RUN = 10

def write_log(fname,message):
    f = open(fname,'w+')
    f.write(message+'\n')
    f.close()

class Worker:
    #def __init__(self):
    #    self.beanstalk = beanstalkc.Connection(host=hostname,port=port)
    #    #tube ACKS is watched by the producer. Helps in writing the log of handled articles
    #    self.beanstalk.use('ACKS')
    #    self.beanstalk.watch('info')
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
    
    def check_completion(self):
        try:
            connection = beanstalkc.Connection(host=hostname,port=port)
            total_jobs = connection.stats_tube('ACKS')['total-jobs']
            jobs_remaining = int(connection.stats_tube('ACKS')['current-jobs-ready'])
            ongoing_jobs = int(connection.stats_tube('ACKS')['current-jobs-reserved'])
            jobs_buried = int(connection.stats_tube('ACKS')['current-jobs-buried'])
            if jobs_remaining or ongoing_jobs:
                return False
            elif jobs_buried:
                for i in range(total_jobs):
                    j = connection.peek(i)
                    if j.stats()['state'] == 'buried':
                        b = j.body
                        connection.use('error')
                        connection.put(str(b))
                        connection.use('ACKS')
                        connection.kick(i)
                return True
        except beanstalkc.UnexpectedResponse,beanstalkc.CommandFailed:
            return False
    
    def work(self):
        beanstalk = beanstalkc.Connection(host=hostname,port=port)
        #tube ACKS is watched by the producer. Helps in writing the log of handled articles
        beanstalk.use('ACKS')
        beanstalk.watch('info')
        #print 'reserving the job'
        #you can mention timeout as argument here; By default its 120s;
        # I figured its more than sufficient to handle one article
        job = beanstalk.reserve()
        sample = job.body
        #print "got the body"
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
        print "started processing"
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
        write_log('myfile', logString)
        beanstalk.put(logString)
        beanstalk.close()
        
    def threadHandler(self,threadList):
        for thread in threadList:
            try:
                if not thread.isAlive():
                    threadList.remove(thread)
                    t = threading.Thread(target=self.work,args=())
                    t.start()
                    threadList.append(t)
            except AttributeError, e:
                #use the error tube
                connection = beanstalkc.Connection(host=hostname,port=port)
                connection.use('error')
                errorlog = str(e) + "at " + datetime.datetime.now().strftime("%H:%M:%S.%f")
                connection.put(errorlog)
                del connection

def main():
    try:
        w = Worker()
        threadList = []
        for i in range(THREADS_TO_RUN):
            t = threading.Thread(target=w.work,args=())
            threadList.append(t)
        for t in threadList:
            t.start()
        while True:
            w.threadHandler(threadList)
            if w.check_completion():
                connection = beanstalkc.Connection(host=hostname,port=port)
                connection.use('ACKS')
                connection.put('COMPLETE')
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
