# Generating NERs from articles stored in large JSON files

## Synopsis

* used [Beanstalkd][1] for our message queue.
* used [Beanstalkc][2] for Python bindings.
* used [ijson][3] as JSON parser.
* used [nltk][4] to generate NERS
* used multiprocessing module in consumer.py
* used threading module in consumer_threads.py
* used multiple class instances as a substitute for threads in consumer_cs.py
* system is *fully worker-fault-tolerant*. Errors go to error tube.
* system is *partially server-fault-tolerant*.
	

 - For ijson to work you need pyYaml installed in your system

## How to use

* list all the working machines in which beanstalkd is running ready in piconfig.py.
* copy the appropriate consumer file to these machines and change the host and port and run the file.
* Now run producer.py on a separate machine or any one of these machines. I prefer on a separate machine.
* This script will pass the articles from the JSON file and allot work to the machines you
have listed in piconfig.py
* Later it will dump all the processed articles to a file and errors to another file

[1]: https://github.com/kr/beanstalkd 
[2]: https://github.com/earl/beanstalkc
[3]: https://github.com/isagalaev/ijson
[4]: http://www.nltk.org/
