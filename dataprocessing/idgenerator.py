'''
Created on Jan 15, 2018

@author: dieutth
'''

import sys
from os import listdir
from os.path import isfile, join


def generateID(mypath, out):
    global_id = 1
    global_ids = {}
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f.endswith("txt")]
    for file in onlyfiles:
        infilepath = join(mypath, file)
        outfilepath = join(out, file)  
        
        with open(outfilepath, "w") as g:
            ids = []
            with open(infilepath) as f:
                for line in f:
                    coid = ','.join(line.split(',')[:2])
                    if coid not in global_ids:
                        global_ids[coid] = global_id
                        global_id += 1
                        ids.append(str(global_id))   
                    else:
                        
                        ids.append(str(global_ids[coid]))
            g.write(','.join(ids) + "\n")

if __name__ == '__main__':
    input_filepath = sys.argv[1]
    output_filepath = sys.argv[2]
    generateID(input_filepath, output_filepath)
    