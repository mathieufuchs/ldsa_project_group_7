import pysam, sys, pickledb, os, requests
from pyspark import SparkContext, SparkConf
from operator import add

# produces all k-mers from sequence given k-mer length k
# returns a tuple on the form (id, [kmers...])
def applyKMers(sequence, k):
    kMers = []
    seq = sequence[0]
    pos = sequence[1]
    for i in range(0, len(seq) - k):
        kMers.append((seq[i: i+k], pos + i))
    return kMers

# given a filename to a bam file, pysam is used to read and extract all sequences and their positions
def extractSequences(fileName):
    print("downloading " + fileName)
    url = swiftUrl + fileName
    
    while True:
        try:
            sequences = []
            with pysam.AlignmentFile(url, "rb") as samfile:
                for r in samfile.fetch(until_eof = True):
                    if not r.is_unmapped: continue
                    sequences.append((r.query_sequence, r.reference_start))
            break
        except:
            print("error: " + str(sys.exc_info()[0]))
            
    os.remove(fileName + ".bai")

    return sequences

def getMax(a, b):
    if a > b:
        return a
    else:
        return b

def findMaxPosition(sequences):
    return sequences.map(lambda x: x[1]).fold(0, getMax)

# list of tuples where first element is a sequence and second is the
# position of the sequence in the genome
sequences = []

k = 10
bins = 256.0
swiftUrl = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"
usage = "usage: python ./genome.py kmers|heatmap"

#conf = SparkConf().setMaster("spark://192.168.1.65:7077").setAppName("genom")
#sc = SparkContext(conf=conf)
sc = SparkContext("local", "genome")
files = sc.textFile("index.txt").cache()
sequences = files.map(extractSequences).flatMap(lambda x: x)

if len(sys.argv) < 2:

    print(usage)

elif sys.argv[1] == "kmers":

    allKMers = sequences.flatMap(lambda seq: applyKMers(seq, k)).groupByKey()
    manyKMers = allKMers.filter(lambda x : len(x[1]) > 1).map(lambda x: (len(x[1]), x[0])).sortByKey()
    manyKMers.saveAsTextFile("outputKmers")

elif sys.argv[1] == "heatmap":

    #maxPos = float(findMaxPosition(sequences))
    maxPos = 63000000.0
    sequences.map(lambda x: (int(round((x[1]/maxPos*float(bins)))), 1)).reduceByKey(add).sortByKey().saveAsTextFile("outputHeatmap")

else:

    print(usage)
