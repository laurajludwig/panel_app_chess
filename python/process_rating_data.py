
''' process_ratings.py RATING_CUTOFF,BASEFILENAME ,DICTLABEL
    
    For the time being it reads in a FIDE Dataset, 
    BASEFILENAME.dat 
    pulls out the Rating and Nationality, for only players
    whose ratings are >= RATING_CUTOFF
    It loads those into an output flat file.  BASEFILENAME.out 
    Then, a dataframe is constructed called df.a  We add a small dict (assign) to df in order to represent the MMYYYY
    This is derived from the DICTLABEL argument, for example '102001' or '012002' 
    In the final step, a df2 dataframe counts the # of 2600-plus-players per nation and timestamp via groupby and the size() function.
    The df2 is written out to disk.   BASEFILENAMEfinal.out 
    
    Usage:  first get a FIDE file from https://ratings.fide.com/download/feb01frl.zip and extract it and rename it to 
    MMMYYYY.dat   then for example issue this:
    python ./process_ratings.py 2600 apr2002 042002 
    The program bypasses the string header row.
    The program now accepts FIDE's naming convention which is, for example, APR03FRL.TXT
    We first use wget and tar -xf to fetch the files and unzip them (all these operations can be in a single batch file) 
    and then we run e.g. 
        python ./process_rating_data.py 2600 JAN05FRL  
    this composes a final summary '200501final.dat'   The program prefixes '20' to the year portion, and converts
    JAN, APR, etc. via a small dictionary into 01, 04, etc. 
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
datedict = { 'jan' : '01', 'apr' : '04', 'jul' : '07', 'oct': '10'}
rearrange = '20' + basefilename[3:5] + datedict[basefilename[0:3].lower()]
print(rearrange) 
dictlabel = rearrange  

infile = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/' + basefilename + '.TXT'

ctr = 0 
outfile = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/' + basefilename + '.out'
with open(outfile, 'w') as out:

    with open(infile, 'r',encoding='ISO-8859-1') as dat:
        for r in dat:
            #print(r) 
            lstrip = r.lstrip() 
            if lstrip[0].isdigit(): 
                rtg = next(iter(re.findall(r" [0-9]{4} ",r)), None) 
                
                nat = next(iter(re.findall(r" [A-Z]{3} ",r)), None) 
                if rtg is None:  # starting in 2002, people started appearing who had rating = 0 (not 4 digits) 
                    pass 
                    #print("Bypass a None Rating for nationality", nat) 
                    # pass
                else:
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
results = 'C:/Users/Elite/chess/panel_app_chess/panel_app_chess/data/' + rearrange + 'final.dat' 
df2.to_csv(results,index=True,header=False) 
try:
    os.remove(outfile) 
except:
    print("Error trying to remove", outfile) 

sys.exit(0) 