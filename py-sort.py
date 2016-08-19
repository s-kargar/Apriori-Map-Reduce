import re
import sys

input_dir= sys.argv[1]
output_dir= sys.argv[2]


from operator import itemgetter, attrgetter
# Iterate over the lines of the file
data=[]
with open(input_dir, 'rt') as f:
 for line in f:
    fields=re.split(r'[;,\s]\s*', line.strip())
    data.append(fields)

first=len(fields)-1
nextSet=range(0,first)

data=sorted(data, key=itemgetter(first,*nextSet))

f = open(output_dir, 'a')
for row in data:
    output=",".join(str(x) for x in row[:len(row)-1])
    f.write(output+"\t"+row[len(row)-1]+"\n")
f.close
