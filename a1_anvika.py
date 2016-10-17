########################################
## Template Code for Big Data Analytics
## assignment 1, at Stony Brook Univeristy
## Fall 2016

## <Anvika SBUID: 110559308>

## version 1.04
## revision history
##  .01 comments added to top of methods for clarity
##  .02 reorganized mapTask and reduceTask into "system" area
##  .03 updated reduceTask comment  from_reducer
##  .04 updated matrix multiply test

from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
import random





##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:  # [TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=4, num_reduce_tasks=3):  # [DONE]
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks

    ###########################################################
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):  # [DONE]
        print "Need to override map"

    @abstractmethod
    def reduce(self, k, vs):  # [DONE]
        print "Need to override reduce"

    ###########################################################
    # System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, map_to_reducer):  # [DONE]
        # runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            # run mappers:
            mapped_kvs = self.map(k, v)
            # assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                map_to_reducer.append((self.partitionFunction(k), (k, v)))

    def partitionFunction(self, k):# [TODO]
        #it takes a list which is  map_to_reducer
        # given a key returns the reduce task to send it
        if isinstance(k[0] , str):
            node_number = hash(k[0]) % self.num_reduce_tasks
        else:
            node_number = k[0] % self.num_reduce_tasks
        return node_number



    def reduceTask(self, kvs, from_reducer):# [TODO]
    # sort all values for each key into a list# [TODO]
        # call reducers on each key paired with a *list* of values
        # and append the result for each key to from_reducer
        # [TODO]

        keyVsFreqList = {}
        for keyVal in kvs:
            key = keyVal[0]
            val = keyVal[1]


            if key not in keyVsFreqList:
                keyVsFreqList[key] = []

            keyVsFreqList[key].append(val)
        #print type(keyVsFreqList)

        for (k,vs) in keyVsFreqList.iteritems():
            mapped_vs = self.reduce(k, vs)
            from_reducer.append(mapped_vs)



        return from_reducer

    def runSystem(self):  # [TODO]
        # runs the full map-reduce system processes on mrObject

        # the following two lists are shared by all processes
        # in order to simulate the communication
        # [DONE]
        map_to_reducer = Manager().list() #mapper is storing values in this. # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        from_reducer = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]


        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,map_to_reducer))
        #      p.start()
        # [TODO]

        for i in xrange(0, len(self.data), self.num_map_tasks):
                jobs = []
                chunk = self.data[i:i + self.num_map_tasks]

                p = Process(target = self.mapTask, args=(chunk, map_to_reducer))
                jobs.append(p)
                p.start()


        # join map task processes back
        # [TODO]
                for i in jobs:
                     i.join()



        # print output from map tasks
        # [DONE]
        print "map_to_reducer after map tasks complete:"
        pprint(sorted(list(map_to_reducer)))


        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]

        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        for k,v in sorted(list(map_to_reducer)):
            to_reduce_task[k].append(v)


        for listKvs in to_reduce_task:
                if __name__ == '__main__':
                 reducers = []
                 pt = Process(target = self.reduceTask, args=[listKvs, from_reducer])
                 reducers.append(pt)
                 pt.start()


        for i in reducers:
            i.join()


        #
        # [TODO]


        # join the reduce tasks back
        # [TODO]


        # print output from reducer tasks
        # [DONE]
        print "map_to_reducer after map tasks complete:"
        pprint(sorted(list(from_reducer)))

        # return all key-value pairs:
        # [DONE]
        #return from_reducer


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items() #return key-value pairs

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))


class MatrixMultMR(MyMapReduce):  # [TODO]

    def __init__(self, data, num_map_tasks, num_reduce_tasks, matrix1):
        super(MatrixMultMR, self).__init__(data, num_map_tasks, num_reduce_tasks)
        self.matrix1 = matrix1

    def map(self, k, v):
        mapper = dict()
        for keys in (k,v):

             if k[0] == 'm':

                for i in range(self.matrix1['n'][1]):
                    mapper[(k[1], i)] = (k[0], k[2], v)
             else:
                 for i in range(self.matrix1['m'][0]):
                     mapper[(i,k[2])] = (k[0], k[1], v)
        return mapper.items()


    def reduce(self, k, vs):
         list_m = {}
         list_n = {}

         dimentions = self.matrix1['m'][1]


         for values in vs:

             if values[0] == 'm':

                 list_m[values[1]] = values[2]


             else:
                 list_n[values[1]] = values[2]


         sum = 0
         for i in range(dimentions):
             sum += list_m[i] * list_n[i]

         return sum












        # the mapper and reducer for matrix multiplication
    #write the map and reduce function for matrix multiplication that will be mapped
    # to the map task and reduce task
    pass




##########################################################################
##########################################################################
# PART II. Minhashing

def minhash(documents, k=5):  # [TODO]
    l = len(documents)
    # print(l)
    darray = [] * l
    for x in range(0, l):
        d = documents[x]

        # print(d);print("\n")
        s = d.replace(" ", "")
        # print(s);print("\n")
        darray.append(s)

    print(len(darray))

    shingles = []

    for x in range(0, l):
        d = darray[x]

        for i in range(0, len(d)):
            shingles.append(d[i:i + 5])

    print(len(shingles))
    shingles = set(shingles)
    seen = set()
    result = []
    for item in shingles:
        if item not in seen:
            seen.add(item)
            result.append(item)
    print(type(result))

    table = [[0 for i in range(len(darray))] for j in range(len(result))]
    # print table
    for d1 in range(len(result)):
        for d2 in range(len(darray)):

            if result[d1] in darray[d2]:
                table[d1][d2] = 1
    print table

    numHashes = 100
    signatures = [[float("inf") for i in range(len(darray))] for j in range(numHashes)]
    # print "Hi sigtable",sigtable

    largePrime = 4969

    for i in range(len(result)):
        hashcode = []
        for j in range(numHashes):  # no of hashes should be equal to the number of rows in the signature matrix
            a = random.randint(700, 1000)
            b = random.randint(500, 1000)
            hashNum = int((a * hash(result[i]) + b) % largePrime)
            hashcode.append(hashNum)

        for k in range(len(darray)):
            if table[i][k] == 1:

                for l in range(0, numHashes):
                    if hashcode[l] < signatures[l][k]:
                        signatures[l][k] = (hashcode[l])


    print signatures

    return signatures  # a minhash signature for each document


##########################################################################
##########################################################################

from scipy.sparse import coo_matrix
matrix1 = {}


def matrixToCoordTuples(label, m):  # given a dense matrix, returns ((row, col), value), ...
    cm = coo_matrix(np.array(m))
    global matrix1


    if (label == 'm'):
        matrix1[label] = cm.shape
    else:
        matrix1[label] = cm.shape



    return zip(zip([label] * len(cm.row), cm.row, cm.col), cm.data)




if __name__ == "__main__":  # [DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

    ####################
    ##run MatrixMultiply
    # (uncomment when ready to test)
    data1 = matrixToCoordTuples('m', [[1, 2], [3, 4]]) + matrixToCoordTuples('n', [[1, 2], [3, 4]])
    d1 = matrix1.copy()
    data2 = matrixToCoordTuples('m', [[1, 2, 3], [4, 5, 6]]) + matrixToCoordTuples('n', [[1, 2], [3, 4], [5, 6]])
    d2 = matrix1.copy()
    data3 = matrixToCoordTuples('m', np.random.rand(20,5)) + matrixToCoordTuples('n', np.random.rand(5, 40))
    d3 = matrix1.copy()

    mrObject = MatrixMultMR(data1, 2, 2, d1)
    mrObject.runSystem()
    mrObject = MatrixMultMR(data2, 2, 2 , d2)
    mrObject.runSystem()
    mrObject = MatrixMultMR(data3, 6, 6, d3)
    mrObject.runSystem()

######################
# run minhashing:

    documents = ["The horse raced past the barn fell. The complex houses married and single soldiers and their families",
                 "There is nothing either good or bad, but thinking makes it so. I burn, I pine, I perish. Come what come may, time and the hour runs through the roughest day",
                 "Be a yardstick of quality. A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful. I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."]
    sigs = minhash(documents, 5)


#References:
# http://michaelnielsen.org/blog/write-your-first-mapreduce-program-in-20-minutes/
# http://www.norstad.org/matrix-multiply/
# http://bioportal.weizmann.ac.il/course/python/PyMOTW/PyMOTW/docs/multiprocessing/mapreduce.html
# https://www.youtube.com/watch?v=30RaNpaupj0
# https://www.knowbigdata.com/blog/multiplying-matrix-using-mapreduce
# https://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
# https://pymotw.com/2/multiprocessing/mapreduce.html
# http://stackoverflow.com/questions/17192418/hash-function-in-python
# http://www.python-course.eu/deep_copy.php
# http://stackoverflow.com/questions/17246693/what-exactly-is-the-difference-between-shallow-copy-deepcopy-and-normal-assignm
# https://www.youtube.com/watch?v=96WOGPUgMfw&t=1189s
# https://adhoop.wordpress.com/2012/03/31/matrix-multiplication-using-mapreduce-1-step-solution/
# http://matthewcasperson.blogspot.com/2013/11/minhash-for-dummies.html
# http://infolab.stanford.edu/~ullman/mmds/ch3.pdf
# http://infolab.stanford.edu/~ullman/mmds/ch2.pdf
#
