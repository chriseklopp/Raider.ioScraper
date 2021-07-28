
"""

"""


from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
import pandas as pd
import json
import requests
import keyboard


def run_formatter(rank_trim): #takes a rank_trim html slice, returns a df with run data
    rank_split_trim = rank_trim.copy()
    for run_enum,run in enumerate(rank_split_trim):
    
        
        rank_row_json = json.loads(rank_split_trim[run_enum])
        
        #clean row dict
        removekey = ['keystone_platoon_id','platoon']
        for key in removekey:
            rank_row_json.pop(key)
           
            
        #clean dungeon dict
        removekey = ['expansion_id','id','keystone_timer_ms','name','slug']
        for key in removekey:
            rank_row_json['dungeon'].pop(key)
        # add dungeon keys to top layer
        for key,value in rank_row_json['dungeon'].items():
            rank_row_json[key] = value
        # remove dungeon key
        rank_row_json.pop('dungeon')
        
        
        #clean weekly_modifiers
        removekey = ['id','icon','description']
        for affix,j in enumerate(rank_row_json['weekly_modifiers']):
            rank_row_json['weekly_modifiers'][affix] = rank_row_json['weekly_modifiers'][affix].pop('name')
        # add modifiers to top layer
        bstring = 'Affix'
        for i in range(0,4):
            affix_string = bstring+str(i+1)
            try:
                rank_row_json[affix_string] = rank_row_json['weekly_modifiers'][i]
            except:
                rank_row_json[affix_string] = None
        # remove modifier key
        rank_row_json.pop('weekly_modifiers')
         
        
        #clean roster
        # removekey = ['class','faction','level','persona_id','path']
        bstring = 'Char'
        for charac,j in enumerate(rank_row_json['roster']):
            rank_row_json['roster'][charac] = rank_row_json['roster'][charac].pop('character')
            #clean next layer down
            rank_row_json['roster'][charac]['race'] = rank_row_json['roster'][charac]['race'].pop('name')
            rank_row_json['roster'][charac]['realm'] = rank_row_json['roster'][charac]['realm'].pop('slug')
            rank_row_json['roster'][charac]['spec'] = rank_row_json['roster'][charac]['spec'].pop('name')
            rank_row_json['roster'][charac]['region'] = rank_row_json['roster'][charac]['region'].pop('short_name')
            rank_row_json['roster'][charac]['class'] = rank_row_json['roster'][charac]['class'].pop('name')
            
        # account for specs with same name but diff class
            if rank_row_json['roster'][charac]['spec'] == 'Holy':
                if rank_row_json['roster'][charac]['class'] == 'Priest':
                            rank_row_json['roster'][charac]['spec'] = 'Holy_Pr'
                else:
                            rank_row_json['roster'][charac]['spec'] = 'Holy_Pa'
                    
            if rank_row_json['roster'][charac]['spec'] == 'Restoration':
                if rank_row_json['roster'][charac]['class'] == 'Shaman':
                            rank_row_json['roster'][charac]['spec'] = 'Resto_S'
                else:
                            rank_row_json['roster'][charac]['spec'] = 'Resto_D'
                               
            if rank_row_json['roster'][charac]['spec'] == 'Protection':
                if rank_row_json['roster'][charac]['class'] == 'Paladin':
                            rank_row_json['roster'][charac]['spec'] = 'Prot_P'
                else:
                            rank_row_json['roster'][charac]['spec'] = 'Prot_W' 
        
        
        # remove unneccesary keys
            rank_row_json['roster'][charac].pop('class')
            rank_row_json['roster'][charac].pop('path')
            char_string = bstring + str(charac+1)
        # add char keys to top layer
            rank_row_json[char_string] = rank_row_json['roster'][charac]
        
        #remove roster key
        rank_row_json.pop('roster')
        
        row_series = pd.Series(rank_row_json)
        if run_enum == 0:
            df= pd.DataFrame(row_series)
            df = df.T #transposes dataframe
        else:
            df= df.append(row_series,ignore_index=True)
            

    return(df)


def datagabber(url): #takes a url, outputs data in a dataframe
    t0 = time.time()

    data_source= requests.get(url).text
    soup = BeautifulSoup(data_source, 'lxml')
    tb = time.time()
    # print(tb-t0,"request")
    # extracting the relevent text mess
    info_block = soup.find_all('script',type="text/javascript")

    #dealing with cases where data is unfound
    rankedGroups = None
    indicstring = 'window.__RIO_INITIAL_DATA'
    for i,block in enumerate(info_block):
        if indicstring in str(info_block[i])[:100]:
            rankedGroups = info_block[i]
    if rankedGroups == None:
          # add part to save current dataframe here
          raise Exception('Info not found in html')

    # more extraction
    group_split = str(rankedGroups).split('rankedGroups')
    group_split1 = group_split[1]
    rank_split = group_split[1].split('"rank"')
    rank_split.pop(0)
    rank_split[-1] = rank_split[-1].split('"ui"')[0]
    rank_split_trim = [n.split(':', maxsplit=2)[2] for n in rank_split]

    # create vector to store score, readd later as column to final df
    score_vector = []
    score_vector = [item.split(':',maxsplit=2)[0] for item in rank_split_trim]
    score_vector = [float(item.split(',',maxsplit=1)[0]) for item in score_vector]

    # cuts out score shit; preps for json conversion
    rank_split_trim = [n.split(':', maxsplit=1)[1] for n in rank_split_trim]
    rank_split_trim = [n[:-3] for n in rank_split_trim]

    #use run_formatter function to spit out the dataframe
    df = run_formatter(rank_split_trim)
    #finally re-add score to the dataframe       
    df['score'] = score_vector
    t1 = time.time()
    total = t1-t0
    # print(total)
    return(df)



# browser = webdriver.Chrome(executable_path=r'C:\Users\chris\Desktop\chromedriver')

# run until command is given, if command given save current data and page number to file and end.
# on start up resume from page left off on.

# while True

urlbase = "https://raider.io/mythic-plus-rankings/season-sl-1/all/world/leaderboards/"
urlend = '#content'


workingdf = pd.DataFrame()
pagenum = 0
ecounter = 0

try:
    start_time = time.time()
    f = open("pagenum.txt",'r')
    pagenum = int(f.read())
    f.close()
    print(f"Continuing from page: {pagenum}")
except:
    print("No pagefile. Starting from 0")
    pagenum = 0


while True:
    try:
       # assemble url string
       url =  ''.join([urlbase,str(pagenum),urlend])
       pagedf = datagabber(url)
       pagenum += 1
       ecounter = 0
       workingdf = workingdf.append(pagedf)
       if (pagenum % 1000 == 0):
            ctime = time.time()
            print(pagenum," Time elapsed: ",(ctime-start_time))
            

       
    except KeyboardInterrupt: #save and close when keyboard inturrupt
        print("Saving Df to File")
        print(f"Left off on page {pagenum}")
        d_filename =  ''.join([r'D:/mythic_run_data/','mythicdata',str(pagenum),'.csv'])
        workingdf.to_csv(d_filename,index=False)
        with open("pagenum.txt",'w')as f:
            f.write(str(pagenum))
        break
             
    
    except IndexError:
        ecounter += 1
        print("Index Error")
        e_filename =  ''.join(['errorloc',str(pagenum),'.txt'])
        d_filename =  ''.join([r'D:/mythic_run_data/','mythicdata',str(pagenum),'.csv'])
        with open("pagenum.txt",'w')as f:
            f.write(str(pagenum))
        if len(workingdf.index) > 0:
            workingdf.to_csv(d_filename,index=False)
            print("File Saved as", d_filename)
            
        if ecounter < 4:
            print("Retrying Page: ", pagenum)
            time.sleep(1)
            continue
        else: #after 4 retrys skips page
            print('Skipping Page: ', pagenum)
            pagenum += 1
            
            with open("pagefail.txt",'a')as f:
                f.write(" " + str(pagenum))
            continue
        
    except Exception as e:
        ecounter += 1
        print("Something Went Wrong")
        print(e)
        d_filename =  ''.join([r'D:/mythic_run_data/','mythicdataEXCEPTION',str(pagenum),'.csv'])
        with open("pagenum.txt",'w')as f:
            f.write(str(pagenum))
        if len(workingdf.index) > 0:
            workingdf.to_csv(d_filename,index=False)
            print("File Saved as", d_filename)
            
        if ecounter < 4:
            print("Retrying Page: ", pagenum)
            time.sleep(1)
            continue
        else:
            print('Skipping Page: ', pagenum)
            pagenum += 1
            
            with open("pagefail.txt",'a')as f:
                f.write(" " + str(pagenum))
            continue




# try:
#     try:
#         start_time = time.time()
#         f = open("pagenum.txt",'r')
#         pagenum = int(f.read())
#         f.close()
#         print(f"Continuing from page: {pagenum}")
#     except:
#         print("No pagefile. Starting from 0")
#         pagenum = 0
        
#     while True:
#        # pagenumstr = str(pagenum)
#        # assemble url string
#         url =  ''.join([urlbase,str(pagenum),urlend])
#         pagedf = datagabber(url)
#         pagenum += 1
#         workingdf = workingdf.append(pagedf)
#         if (pagenum % 1000 == 0):
#             ctime = time.time()
#             print(pagenum," Time elapsed: ",(ctime-start_time))
        
# except KeyboardInterrupt:
#        print("Saving Df to File")
#        print(f"Left off on page {pagenum}")
#        d_filename =  ''.join([r'./rundata/','mythicdata',str(pagenum),'.csv'])
#        workingdf.to_csv(d_filename,index=False)
#        with open("pagenum.txt",'w')as f:
#            f.write(str(pagenum))
           
# except IndexError:
#         print("Index Error")
#         e_filename =  ''.join(['errorloc',str(pagenum),'.txt'])
#         d_filename =  ''.join([r'./rundata/','mythicdata',str(pagenum),'.csv'])
#         with open("pagenum.txt",'w')as f:
#             f.write(str(pagenum))
#         if len(workingdf.index) > 0:
#             workingdf.to_csv(d_filename,index=False)
#             print("File Saved as", d_filename)
        

# except Exception as e:
#     print("Something Went Wrong")
#     print(e)
#     d_filename =  ''.join([r'./rundata/','mythicdataEXCEPTION',str(pagenum),'.csv'])
#     with open("pagenum.txt",'w')as f:
#         f.write(str(pagenum))
#     if len(workingdf.index) > 0:
#         workingdf.to_csv(d_filename,index=False)
#         print("File Saved as", d_filename)

   
    


#create function for combining run datasets into a single dataframe.
# set index to runid,it should remove duplicated runs.





# later analyses stuff
spec_list = [
        'Blood','Frost','Unholy',
        'Havoc','Vengeance',
        'Balance','Feral','Guardian','Resto_D',
        'Beast_Mastery','Marksmanship','Survival',
        'Arcane','Fire','Frost',
        'Brewmaster','Mistweaver','Windwalker',
        'Holy_Pa','Prot_P','Retribution',
        'Discipline','Holy_Pr','Shadow',
        'Assassination','Outlaw','Subtlety',
        'Elemental','Enhancement','Resto_S',
        'Affliction','Demonology','Destruction',
        'Arms','Fury','Prot_W']
    
role_list = ['tank','healer','dps']


