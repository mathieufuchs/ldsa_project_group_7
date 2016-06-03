import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

items = []

with open("outputHeatmap/part-00000") as f:
    content = f.readlines()
    for line in content:
        s = line.split(",")
        index = float(s[0][1:])
        num = float(s[1][:-2])
        items.append((index, num))

def getMax(a, b):
    if a > b:
        return a
    else:
        return b

nItems = []
prevItem = None

for item in items:
    if prevItem == None:
        prevItem = item
        continue

    nItems.append(prevItem)

    for i in xrange(int(prevItem[0]+1), int(item[0])):
        nItems.append((i, 0.0))

    prevItem = item

nItems.append(prevItem)
items = nItems  

#m = reduce(getMax, map(lambda x: x[1], items), 0)

y = map(lambda x: x[1], items)
x = map(lambda x: x[0], items)

plt.plot(x, y)
plt.ylabel("number of unmapped reads")
plt.xlabel("position in genome")
plt.savefig('heatmap.png')
