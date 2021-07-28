# -*- coding: utf-8 -*-
"""
scraper v 3.2



"""
# from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
import pandas as pd
import json
# import numpy as np
import aiohttp
import asyncio






def affix_extract(x):
    try:
        x = x['name']
    except:
        x = 'None'
    return(x)
        
        
def roster_clean(x):

    try:
        del x['oldCharacter']
        del x['isTransfer']
        del x['role']
        y = x['character']
        for i in ['id', 'persona_id', 'level']:
            del y[i]
        y['class'] = y['class']['name']
        y['race'] = y['race']['name']
        y['spec'] = y['spec']['name']
        y['realm'] = y['realm']['name']
        y['region'] = y['region']['name']

        if ' ' in y['race']:
            y['race'] = y['race'].replace(" ", "_")

        if y['spec'] == 'Holy':
            if y['class'] == 'Priest':
                y['spec'] = 'Holy_Pr'
            else:
                y['spec'] = 'Holy_Pa'
        
        
        if y['spec'] == 'Frost':
            if y['class'] == 'Mage':
                y['spec'] = 'Frost_M'
            else:
                y['spec'] = 'Frost_D' 
        
        
        if y['spec'] == 'Restoration':
            if y['class'] == 'Shaman':
                y['spec'] = 'Resto_S'
            else:
                y['spec'] = 'Resto_D'
                
                
        if y['spec'] == 'Protection':
            if y['class'] == 'Paladin':
                y['spec'] = 'Prot_P'
            else:
                y['spec'] = 'Prot_W'   
        
        if y['spec'] == 'Beast Mastery':
            y['spec'] == 'Beast_Mastery'

    
    
        y['name'] = '-'.join([y['name'],y['realm'],y['region']])
        y['spec'] = '-'.join([y['race'],y['spec']])
        
        del y['faction']
        del y['race']
        del y['realm']
        del y['region']
        del y['path']
        del y['class']
    
    except:
        pass
    
    try:
        del y['stream']
    except:
        pass

    
    
    return(x)


# def faction_finder(x):
#     indicator = None
#     y = roster.loc[x,'Char1']
#     z = y['character']
#     if z['faction'] == 'alliance':
#         indicator = 'A'
#     if z['faction'] == 'horde':
#         indicator = 'H'
#     return(indicator)







def runjson_ext(data): #takes a raider io mythic plus json and returns it as dataframe
   try: 
    soup = BeautifulSoup(data, 'lxml')
    
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
    
    # group_split1 = group_split[1]

    # group_split_n = 
    rank_split = group_split[1].split('"ui"')
    
    jank = '{"run_ranks' +  rank_split[0] +'}'
    jank = jank[:-2] + "}"
    data_json = json.loads(jank)
    
    df = pd.DataFrame(data_json['run_ranks']) # top layer.
   except:
       df = None
   return df
    
def run_format(df):
    run_info = pd.DataFrame(df['run'].tolist()) #splits run info into own dataset
    
    dungeon_info = pd.DataFrame(run_info['dungeon'].tolist())
    dungeon_info1 = dungeon_info[["short_name","patch","keystone_timer_ms"]].copy()
    
    weekly_modifiers = pd.DataFrame(run_info['weekly_modifiers'].tolist())
    weekly_modifiers1 = weekly_modifiers.applymap(affix_extract)
    
    
    weekly_modifiers1.rename(columns={0:'Affix1',
                           1:'Affix2',
                           2:'Affix3',
                           3:'Affix4'}, inplace = True)
    
    roster = pd.DataFrame(run_info['roster'].tolist())
    
    rosterlist = []
    # filldick = {'character':{'name':'None','spec':'None'}}
    # WHY NO WORK WITH A DICTIONARY
    # roster.fillna('NONESTR',inplace = True)
    
    roster = roster.where(roster.notna(), lambda x: [{'character':{'name':'None','spec':'None'}}])

    # print(len(roster.columns))
    for i in range(0,5):
        roster[i].map(roster_clean)
        roster11 = pd.DataFrame(roster[i].tolist())
        roster12 = pd.DataFrame(roster11['character'].tolist())
        try: 
           del roster12['anonymized']
        except:
            pass
        roster12.rename(columns={'name':f'Name{i+1}',
                                     'spec':f'Char{i+1}'},inplace=True)
        rosterlist.append(roster12)
        # print(i)
    
  
    roster = pd.concat(rosterlist,axis=1)
    
    keystone_id = run_info['keystone_run_id'].copy()
    roster['keystone_run_id'] = keystone_id
  
    
    # roster.rename(columns={0:'Char1',
    #                        1:'Char2',
    #                        2:'Char3',
    #                        3:'Char4',
    #                        4:'Char5'}, inplace = True)
    
    
    
    

    # run info will be main frame to merge onto. index by keystone_run_id
    
    run_info.drop(columns=['dungeon','keystone_platoon_id',
                           'weekly_modifiers',
                           'roster','platoon'],inplace= True)
    
    
    df.drop(columns=['rank','run'],inplace=True)
    
    # df.drop(columns=['run'],inplace=True)
    # merging
    # df into run_info
    run_info_m = run_info.merge(df,left_index=True,right_index=True)
    
    # roster into run_info_m
    # run_info_m2 = run_info_m.merge(roster,left_index=True,right_index=True)
    
    # dungeon_info1 into run_info_m2
    run_info_m3 = run_info_m.merge(dungeon_info1,left_index=True,right_index=True)
    
    # weekly_modifiers1 into run_info_m3
    mythic_run_info = run_info_m3.merge(weekly_modifiers1,left_index=True,right_index=True)

    #set keystone_run_id to index
    mythic_run_info.set_index('keystone_run_id',inplace=True)
    return(mythic_run_info,roster)






async def get_page(session, url):
    async with session.get(url) as resp:
        page_text = await resp.text()
        return page_text

async def main(lower,upper):
    working_df_list = []
    gt = time.time()
    async with aiohttp.ClientSession() as session:
        workingdf = pd.DataFrame()
        tasks = []
        for number in range(lower,upper):
            url = f'https://raider.io/mythic-plus-rankings/season-sl-1/{dungio}/world/leaderboards/{number}#content'
            tasks.append(asyncio.ensure_future(get_page(session, url)))

        page_set = await asyncio.gather(*tasks)
        it = time.time()
        print(it - gt,"h")
        
        print("starting comp")
        # page_list = [runjson_ext(n) for n in page_set]
        # workingdf = workingdf.append(page_list)
        
        

        
        for i,page in enumerate(page_set):
            # print(i)
            page_df = runjson_ext(page)
            # print(page_df)
            #new method
            working_df_list.append(page_df)
            #old method
            # workingdf = workingdf.append(page_df,ignore_index=True)

    workingdf = pd.concat(working_df_list, ignore_index=True)
    t1 = time.time()
    print("PT:", t1 - it)
    # workingdf.to_csv("UNFIXED.csv",index=False)
    
    workingdfs = run_format(workingdf)
    tf = time.time()
    print("FT:", tf - t1)
    return(workingdfs)
        # print(workingdf)
  
   
  
    
  
    
  
    
  
    
  
# start_time = time.time() 
try:

    f = open('D:/mythic_run_data/pagenum.txt','r')
    pagenum = int(f.read())
    f.close()
    print(f"Continuing from page: {pagenum}")
except:
    print("No pagefile. Starting from 0")
    pagenum = 0  
  
   
  
    
lower = pagenum
upper = lower+1000
ecount = 0


# f'https://raider.io/mythic-plus-rankings/season-sl-1/de-other-side/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/halls-of-atonement/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/mists-of-tirna-scithe/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/plaguefall/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/sanguine-depths/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/spires-of-ascension/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/the-necrotic-wake/world/leaderboards/{number}#content'
# f'https://raider.io/mythic-plus-rankings/season-sl-1/theater-of-pain/world/leaderboards/{number}#content'



# dungeons = ['mists-of-tirna-scithe']
dungeons = ['sanguine-depths','de-other-side','halls-of-atonement',
            'mists-of-tirna-scithe','plaguefall','spires-of-ascension','the-necrotic-wake','theater-of-pain']


# dungeons.reverse()

# print(dungeons)

for dungio in dungeons:
    print('--------------------------------------------------------')
    print(dungio)
    print('--------------------------------------------------------')
    while True:
        try:
           start_time = time.time()
           t = asyncio.run(main(lower,upper))
           d_filename =  f'D:/mythic_run_data/mythicdata_{dungio}{upper}.csv'  #location to store files
           n_filename = f'D:/mythic_run_data/namedata_{dungio}{upper}.csv'     #location to store files
           lower +=1000
           upper +=1000
           t[0].to_csv(d_filename)
           t[1].to_csv(n_filename)
           print("--- %s seconds ---" % (time.time() - start_time))
           
           ecount = 0
           

        except Exception as e:
            ecount += 1
            print(e)
            # with open("scraper3.2_error.txt",'a')as f:
            #         f.write(" " + str(lower) + "-" + str(upper))
            if ecount > 3:
                print("Three Errors in a row, end of cycle?")
                print('Breaking')
                lower = 0
                upper = lower+1000
                break

time.sleep(120)
    
