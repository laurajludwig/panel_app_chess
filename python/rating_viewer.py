''' rating_viewer.py RATING_CUTOFF BASEFILE  NATIONALITY 
    
    To drill down to individual 2600-plus players for the Insights text 

    MG  June 2023

''' 
import re,sys 
import pathlib 
import pandas as pd 
from pathlib import Path
 
rating_cutoff = int(sys.argv[1]) 
basefilename =  sys.argv[2] 
nation = sys.argv[3] 
# mar appears in 10, nov appears in 09,  and sep appears in 12  (adjacent months missing) 

datedict = { 'jan' : '01', 'mar' : '03', 'apr' : '04', 'jul' : '07', 'sep': '09', 'oct': '10', 'nov' : '11'}  

rearrange = '20' + basefilename[3:5] + datedict[basefilename[0:3].lower()]
print(rearrange) 
dictlabel = rearrange  
# starting october 2012, we prefix filename with standard_
#nfile = 'D:/Users/Elite/panel_app_chess/data/full/' + basefilename + '.TXT' 
infile = 'D:/Users/Elite/panel_app_chess/data/full/' + 'standard_' + basefilename + '.TXT'

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
                if rtg is None:
                    pass
                else:
                    if nation in nat and int(rtg) >= 2600:
                        print(lstrip) 
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
path = pathlib.Path(outfile)
try:
    path.unlink(outfile)  
except:
    print("Error trying to unlink remove", outfile) 


sys.exit(0) 