import pysam
num_of_unmapped = 0
num_of_kmers = 0
tabKmers=""
#posKmers=[]
#output = open("testlog.out", 'w')
bamUrl = "http://130.238.29.253:8080/swift/v1/1000-genomes-dataset/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam"
with pysam.AlignmentFile("HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam","rb") as samfile:
        for r in samfile.fetch(until_eof=True):
                if r.is_unmapped:
                        num_of_unmapped +=1
                        for i in range(r.query_alignment_start, r.query_alignment_end -9):
#                               posKmers.append(r.reference_start+i)
#                               output.write(r.query_sequence[i:i+10]+'\t')
                                tabKmers += r.query_sequence[i: i+10] +'\t'
                                num_of_kmers+=1
                        #break
#       print posKmers
        print tabKmers[0:1000]+"..........................."
        print "Number of unmapped sequences: ", num_of_unmapped
        print "Number of generated K-mers (k=10): ", num_of_kmers