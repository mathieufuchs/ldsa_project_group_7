import pysam, os, sys
from pyspark import SparkContext

# produces all k-mers from sequence given k-mer length k
def applyKMers(sequence, k):
    kMers = []
    seq = sequence[0]
    pos = sequence[1]
    for i in range(0, len(seq) - k):
        kMers.append((seq[i: i+k], pos + i))
    return kMers

# given a filename to a bam file, pysam is used to read and extract all sequences and their positions
def extractSequences(fileName):
    sequences = []
    url = swiftUrl + fileName
    with pysam.AlignmentFile(url, "rb") as samfile:

        for r in samfile.fetch(until_eof = True):
            if not r.is_unmapped: continue
            if len(sequences) > 100: break
            sequences.append((r.query_sequence, r.reference_start))

    os.remove(fileName+".bai")
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
bins = 20.0
swiftUrl = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"
usage = "usage: python ./genome.py kmers|heatmap"

sc = SparkContext("local", "Genome")
files = sc.textFile("index.txt").cache()
sequences = files.map(extractSequences).flatMap(lambda x: x)

if len(sys.argv) < 2:

    print(usage)

elif sys.argv[1] == "kmers":

    allKMers = sequences.flatMap(lambda seq: applyKMers(seq, k)).groupByKey()
    manyKMers = allKMers.filter(lambda x : len(x[1]) > 1).map(lambda x: (len(x[1]), x[0])).sortByKey()

    manyKMers.saveAsTextFile("sparkOutput")

elif sys.argv[1] == "heatmap":

    maxPos = float(findMaxPosition(sequences))
    result = sequences.map(lambda x: (round(x[1]/maxPos*float(bins)), 1)).reduceByKey(lambda a, b: (a, sum(b))).collect()
    
    print(result)

else:
    print(usage)

#kMers = data.flatMap(lambda sequence: applyKMers(sequence, k))
#kMerCounts = kMers.groupByKey().filter(lambda x: len(x[1]) > 1).map(lambda x: (x[0], list(x[1])))

#print(sorted(kMers.collect(), key = lambda x: len(x[1])))

#with pysam.AlignmentFile(samPath, "rb") as samfile:
#    for r in samfile.fetch(until_eof=True):
#        if not r.is_unmapped: continue
#        if len(sequences) > 10: break
#        sequences.append((r.query_sequence, r.reference_start))
