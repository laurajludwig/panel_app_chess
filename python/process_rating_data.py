''' process_ratings.py RATING_CUTOFF,BASEFILENAME 
    
    For the time being it reads in a FIDE Full Ratings List (FRL) Dataset, 
    which we fetched by wget and tar -xf and placed into /data/full.  It is named like this example:  JAN05FRL.TXT 

    Pull out the Rating and Nationality, for only players whose ratings are >= RATING_CUTOFF
    Load those into an output flat file.  BASEFILENAME.out
    The BASEFILENAME is a transformation of the input file, JAN05 --> 200501 (YYYYMM keeps sort order) 
    Then, a dataframe is constructed called df.a  We add a small dict (via 'assign') to df in order to represent the YYYYMM as a column.

    In the final step, a df2 dataframe counts the # of 2600-plus-players per nation and timestamp via groupby and the size() function.

    The df2 is written out to disk, example name '/data/200501final.dat' 
    Usage example:    python ./process_rating_data.py 2600 JAN05FRL ---->     /data/200501final.dat 

    MG  June 2023

''' 
import re,sys 
import pandas as pd 
if len(sys.argv) != 3:
    print("Usage", sys.argv[0] , "ratingcutoff basefilename")
    sys.exit(1) 
rating_cutoff = int(sys.argv[1]) 
basefilename =  sys.argv[2] 
#dictlabel    =  sys.argv[3] 

datedict = { 'jan' : '01', 'mar' : '03', 'apr' : '04', 'jul' : '07', 'oct': '10', 'nov' : '11'} # mar appears in 10, nov appears in 09 

rearrange = '20' + basefilename[3:5] + datedict[basefilename[0:3].lower()]
print(rearrange) 
dictlabel = rearrange  

infile = 'D:/Users/Elite/panel_app_chess/data/full/' + basefilename + '.TXT'


ctr = 0 
outfile = 'D:/Users/Elite/panel_app_chess/data/' + basefilename + '.out'
with open(outfile, 'w') as out:

    with open(infile, 'r',encoding='ISO-8859-1') as dat:
        for r in dat:
            #print(r) 
            lstrip = r.lstrip() 

            if lstrip[:1].isdigit():  # guard against lstrip being empty (unzipped file may have gap between header and data) 
                rtg = next(iter(re.findall(r" [0-9]{4} ",r)), None) 
                
                nat = next(iter(re.findall(r" [a-zA-Z]{3} ",r)), None) 
                nat = nat.upper()  # in JAN07FRL.TXT had an entry for 'Col' nationality 
                if rtg is None:  # starting in 2002, people started appearing who had rating = 0 (not 4 digits) 

                    pass 
                    #print("Bypass a None Rating for nationality", nat) 
                    # pass
                else:
                    #print(rtg,nat) 
                    outputline = rtg + "\t" + nat + "\n" 
                    out.write(outputline) 
                    ctr = ctr + 1
                    if ctr > 100000000:
                        sys.exit(0)
            else:
                pass 
                #print("Do not process header", r) 
                #print("Do not process header.") 
ds = {'ds': dictlabel }  # datestamp column label and value,  oct 2001 data source 
data = pd.read_csv(outfile,encoding="ISO-8859-1",sep='\t',header=None,names=["rtg","nat"])
data = data.assign(**ds) 
data = data[['ds','nat','rtg']]  # reorder columns for easier to read output 
data['rtg'] = data['rtg'].astype('int')
data =  data[data["rtg"] >= rating_cutoff]   
df2 = data.groupby(['ds','nat']).size() 
results = 'D:/Users/Elite/panel_app_chess/data/' + rearrange + 'final.dat' 
df2.to_csv(results,index=True,header=False) 
out.close() 
try:
    os.remove(outfile) 
except:
    print("Error trying to remove", outfile) 

sys.exit(0) 