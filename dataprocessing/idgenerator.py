'''
Created on Jan 15, 2018

@author: dieutth
'''
global_ids = {}
id = 1
mypath = "/home/dhong/unzipped/processed"
out = "/home/dhong/out"


from os import listdir
from os.path import isfile, join
onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f.endswith("txt")]

# print(onlyfiles)
for file in onlyfiles:
    infilepath = join(mypath, file)
    outfilepath = join(out, file)  
    
    with open(outfilepath, "w") as g:
        ids = []
        with open(infilepath) as f:
            for line in f:
                coid = ','.join(line.split(',')[:2])
                if coid not in global_ids:
                    global_ids[coid] = id
                    id += 1
                    ids.append(str(id))   
                else:
                    
                    ids.append(str(global_ids[coid]))
        g.write(','.join(ids) + "\n")
		#print(file)

print(len(global_ids))
