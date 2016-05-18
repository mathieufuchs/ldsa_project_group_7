import pysam
from pyspark import SparkContext

def applyKMers(sequence, k):
    kMers = []
    seq = sequence[0]
    pos = sequence[1]
    for i in range(0, len(seq) - k):
        kMers.append((seq[i: i+k], pos + i))
    return kMers

# list of tuples where first element is a sequence and second is the
# position of the sequence in the genome
sequences = []
samPath = "/home/ubuntu/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"

with pysam.AlignmentFile(samPath, "rb") as samfile:
    for r in samfile.fetch(until_eof=True):
        if not r.is_unmapped: continue
        if len(sequences) > 10: break
        sequences.append((r.query_sequence, r.reference_start))

k = 10
sc = SparkContext("local", "Genome")
data = sc.parallelize(sequences)
kMers = data.flatMap(lambda sequence: applyKMers(sequence, k)).groupByKey().filter(lambda x: len(x[1])).map(lambda x: (x[0], list(x[1])))

print(sorted(kMers.collect(), key = lambda x: len(x[1])))
