# write all the available hosts in the cluster here //port value should be in integer
# recommended not to use the producer machine
availHosts = [{'hostname':'localhost','port':11000},{'hostname':'localhost','port':12000}]

#numOfThreads = 5

#how many jobs to queue ,once the queue has low number of jobs
#these many jobs are again queued
numOfJobs = 30

#monitor after this interval in seconds
monitoringInterval = 5

TUBE_NAME = 'info'
#should be same in client.py
ERROR_TUBE_NAME = 'error'
#should be same in client.py
PRODUCER_TUBE_NAME = 'ACKS'

PRODUCER_LOG_FILE = 'logs.txt'

WORKERS_COMBINED_ERROR_FILE = 'workers_errors.txt'

#do not change this
ARTICLE_ID = 1
ARTICLE_FILE = 'article_dump.json'
LOADING_COMPLETE = False