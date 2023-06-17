
''' process_ratings.py RATING_CUTOFF,BASEFILENAME 
    
    For the time being it reads in a FIDE Dataset, 
    BASEFILENAME.dat 
    pulls out the Rating and Nationality, for only players
    whose ratings are >= RATING
    It loads those into an output flat file.  BASEFILENAME.out 
    Then, a dataframe is constructed called df.a  We add a small dict (assign) to df in order to represent the MMYYYY
    timestamp for this dataset as a new column, in this case '102001'.
    In the final step, a df2 dataframe counts the # of 2600-plus-players per nation and timestamp via groupby and the size() function.
    The df2 is written out to disk.   BASEFILENAMEfinal.out 

    MG  June 2023

''' 




import re,sys 
import pandas as pd 
if len(sys.argv) != 3:
    print("Usage", sys.argv[0] , "ratingcutoff,basefilename")
    sys.exit(1) 
rating_cutoff = int(sys.argv[1]) 
basefilename =  sys.argv[2] 
infile = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/' + basefilename + '.dat'

ctr = 0 
outfile = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/' + basefilename + '.out'
with open(outfile, 'w') as out:
    with open(infile, 'r') as dat:
        for r in dat:
            lstrip = r.lstrip() 
            if lstrip[0].isdigit(): 
                
                rtg = next(iter(re.findall(r" [0-9]{4} ",r)), None) 
                nat = next(iter(re.findall(r" [A-Z]{3} ",r)), None) 
        #print(rtg) # ,type(rtg)) # ,len(rtg))
            #if int(rtg) >= 2600:
                outputline = rtg + "\t" + nat + "\n" 
                out.write(outputline) 
                ctr = ctr + 1
                if ctr > 100000000:
                    sys.exit(0)
            else:
                print("Do not process header.") 
ds = {'ds': '102001'}  # datestamp column label and value,  oct 2001 data source 
data = pd.read_csv(outfile,encoding="ISO-8859-1",sep='\t',header=None,names=["rtg","nat"])
data = data.assign(**ds) 
data = data[['ds','nat','rtg']]
data['rtg'] = data['rtg'].astype('int')
data =  data[data["rtg"] > rating_cutoff] #  'Spark']data[['rtg'] > 2500] 
print(data.head(5) )
df2 = data.groupby(['ds','nat']).size() 
 
results = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/' + basefilename + 'final.out' 
df2.to_csv(results,index=True,header=False) 
sys.exit(0) 