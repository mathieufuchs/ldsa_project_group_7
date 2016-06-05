input = open("result.txt","r")
out = open("test.fasta","w")

result = ""
id = 0
print 1
for line in input:
	id +=1
	splited = line[1:-2].split(",")
	splited[1] = splited[1][2:-1]
	result += ">" +str(id) + "\n" + splited[1]+"\n"

#print result
out.write(result)
input.close()
out.close()
