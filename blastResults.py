from Bio import SeqIO
from Bio.Blast import NCBIWWW
from Bio.Blast import NCBIXML
handle = open("test.fasta", "rU")#.read()
for record in SeqIO.parse(handle, "fasta"):
    print(record)
#handle.close()
result = NCBIWWW.qblast("blastn", "nt", handle)
#result = open("my_blast.xml")  #USE THIS IF SAVED .xml file 
blastResult = NCBIXML.parse(result)
#blast_record = NCBIXML.read(result)
print blastResult ''' --> PRINTS THE CONTENT OF THE XML FILE '''
#blast_record = next(blastResult)

#save_file = open("my_blast.xml", "w")
#save_file.write(result.read())
#save_file.close()

''' ---> USE THIS TO PARSE THE RECORD 
blast_record = NCBIXML.read(result)

for alignment in blast_record.alignments:
	for hsp in alignment.hsps:
		print('****Alignment****')
		print('sequence:', alignment.title)
		print('length:', alignment.length)
		print('e value:', hsp.expect)
		print(hsp.query[0:75] + '...')
		print(hsp.match[0:75] + '...')
		print(hsp.sbjct[0:75] + '...')
'''