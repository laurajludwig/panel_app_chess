
''' process_ratings.py 
    This program will be enhanced to take arguments in the future.
    For the time being it reads in a FIDE Dataset, pulls out the Rating and Nationality, for only players
    whose ratings are >= 2600.
    It loads those into an output flat file.
    Then, a dataframe is constructed called df.
    In the final step, a df2 dataframe counts the # of 2600-plus-players per nation via groupby and count.
    The df2 is written out to disk. 

    MG  June 2023

''' 
infile = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/oct2001.dat'
import re,sys 
import pandas as pd 

ctr = 0 
outfile = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/oct2001.out'
with open(outfile, 'w') as out:
    with open(infile, 'r') as dat:
        for r in dat:
        
            rtg = next(iter(re.findall(r" [0-9]{4} ",r)), None) 
            nat = next(iter(re.findall(r" [A-Z]{3} ",r)), None) 
        #print(rtg) # ,type(rtg)) # ,len(rtg))
            if int(rtg) >= 2600:
                outputline = rtg + "\t" + nat + "\n" 
                out.write(outputline) 
                ctr = ctr + 1
            if ctr > 100000000:
                sys.exit(0) 

data = pd.read_csv(outfile,encoding="ISO-8859-1",sep='\t',header=None,names=["rtg","nat"])

df2 = data.groupby(['nat'])['nat'].count() 

results = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/oct2001final.out' 
df2.to_csv(results,index=True) 
sys.exit(0) 