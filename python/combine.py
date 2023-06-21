# -*- coding: utf-8 -*-
"""

combine.py

Combine all the /data/*final* files into one file for the visualization, 
/data/combined/finish.dat

Created on Tue Jun 20 08:47:45 2023

@author: Elite
"""
import glob, os

datapath = "..\data"
os.chdir(datapath) 

outpath = "../data/combined/" 
name = "finish.dat" 

outfile = outpath + name 

with open(outfile, 'w') as out:
    for f in glob.glob("*final.dat"):
        text_file = open(f, "r")
 
        #read whole file to a string
        data = text_file.read()
        out.write(data) 
        #close file
        text_file.close()
    
    
print("All done.  Check file /data/combined/finish.dat")
    
    