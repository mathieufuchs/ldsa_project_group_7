import pysam, sys, pickledb
from pyspark import SparkContext
from operator import add

# produces all k-mers from sequence given k-mer length k
# returns a tuple on the form (id, [kmers...])
def applyKMers(sequence, k):
    kMers = []
    id = sequence[0]
    seq = sequence[1]
    pos = sequence[2]
    for i in range(0, len(seq) - k):
        kMers.append((seq[i: i+k], pos + i))
    return (id, kMers)

# given a filename to a bam file, pysam is used to read and extract all sequences and their positions
def extractSequences(fileName):
    id = fileName[0:7]
    sequences = []
    url = swiftUrl + fileName
    with pysam.AlignmentFile(url, "rb") as samfile:

        for r in samfile.fetch(until_eof = True):
            if not r.is_unmapped: continue
            if len(sequences) > 100: break
            sequences.append((id, r.query_sequence, r.reference_start))

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
bins = 200.0
swiftUrl = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/"
usage = "usage: python ./genome.py kmers|heatmap"

sc = SparkContext("local", "Genome")
files = sc.textFile("shortIndex.txt").cache()
sequences = files.map(extractSequences).flatMap(lambda x: x)
print(sequences.collect())

if len(sys.argv) < 2:

    print(usage)

elif sys.argv[1] == "kmers":

    allKMers = sequences.flatMap(lambda seq: applyKMers(seq, k)).groupByKey()
    manyKMers = allKMers.filter(lambda x : len(x[1]) > 1).map(lambda x: (len(x[1]), x[0])).sortByKey()

    manyKMers.saveAsTextFile("outputKmers")

elif sys.argv[1] == "heatmap":

    maxPos = float(findMaxPosition(sequences))
    sequences.map(lambda x: (int(round((x[1]/maxPos*float(bins)))), 1)).reduceByKey(add).sortByKey().saveAsTextFile("outputHeatmap")

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
