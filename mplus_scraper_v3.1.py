# -*- coding: utf-8 -*-
"""
Created on Sat Apr 24 16:20:04 2021

@author: chris
"""
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import time
import pandas as pd
import json

import numpy as np
import aiohttp
import asyncio
import time
import pandas as pd
import json




def affix_extract(x):
    x = x['name']
    return(x)
        
        
def roster_clean(x):
    
    # del x['oldCharacter']
    # del x['isTransfer']
    y = x['character']
    for i in ['id', 'persona_id', 'level']:
        del y[i]
    y['class'] = y['class']['name']
    y['race'] = y['race']['name']
    y['spec'] = y['spec']['name']
    y['realm'] = y['realm']['name']
    y['region'] = y['region']['name']
    try:
        del y['stream']
    except:
        pass
    
    if y['spec'] == 'Holy':
        if y['class'] == 'Priest':
            y['spec'] = 'Holy_Pr'
        else:
            y['spec'] = 'Holy_Pa'
    
    if y['spec'] == 'Restoration':
        if y['class'] == 'Shaman':
            y['spec'] = 'Resto_S'
        else:
            y['spec'] = 'Resto_D'
            
            
    if y['spec'] == 'Protection':
        if y['class'] == 'Paladin':
            y['spec'] = 'Prot_P)'
        else:
            y['spec'] = 'Prot_W'       

    return(x)


def faction_finder(x):
    indicator = None
    y = roster.loc[x,'Char1']
    z = y['character']
    if z['faction'] == 'alliance':
        indicator = 'A'
    if z['faction'] == 'horde':
        indicator = 'H'
    return(indicator)







def run_format(data): #takes a raider io mythic plus json and returns it as dataframe
    
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
    
    group_split1 = group_split[1]

    # group_split_n = 
    rank_split = group_split[1].split('"ui"')
    
    jank = '{"run_ranks' +  rank_split[0] +'}'
    jank = jank[:-2] + "}"
    data_json = json.loads(jank)



    t0 = time.time()
    df = pd.DataFrame(data_json['run_ranks']) # top layer.
    
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
    
    for i in roster.columns:
        roster[i].apply(roster_clean)
    
  
    
    roster.rename(columns={0:'Char1',
                           1:'Char2',
                           2:'Char3',
                           3:'Char4',
                           4:'Char5'}, inplace = True)
    
    
    
    

    # run info will be main frame to merge onto. index by keystone_run_id
    
    run_info.drop(columns=['dungeon','keystone_platoon_id',
                           'weekly_modifiers',
                           'roster','platoon'],inplace= True)
    
    df.drop(columns=['rank','run'],inplace=True)
    # merging
    # df into run_info
    run_info_m = run_info.merge(df,left_index=True,right_index=True)
    
    # roster into run_info_m
    run_info_m2 = run_info_m.merge(roster,left_index=True,right_index=True)
    
    # dungeon_info1 into run_info_m2
    run_info_m3 = run_info_m2.merge(dungeon_info1,left_index=True,right_index=True)
    
    # weekly_modifiers1 into run_info_m3
    mythic_run_info = run_info_m3.merge(weekly_modifiers1,left_index=True,right_index=True)
    t1 = time.time()
    print(t1-t0)
    #set keystone_run_id to index
    mythic_run_info.set_index('keystone_run_id',inplace=True)
    return(mythic_run_info)




start_time = time.time()

# workingdf = pd.DataFrame()



async def get_page(session, url):
    async with session.get(url) as resp:
        page_text = await resp.text()
        return page_text

async def main():
    gt = time.time()
    async with aiohttp.ClientSession() as session:
        workingdf = pd.DataFrame()
        tasks = []
        for number in range(25,100):
            url = f'https://raider.io/mythic-plus-rankings/season-sl-1/all/world/leaderboards/{number}#content'
            tasks.append(asyncio.ensure_future(get_page(session, url)))

        page_set = await asyncio.gather(*tasks)
        it = time.time()
        print(it - gt,"h")
        for i,page in enumerate(page_set):

            print(i)
            try:
                page_df = run_format(page)
                # print(page_df)
                workingdf = workingdf.append(page_df,ignore_index=True)
            except TypeError:
                print(page)
                break
            
        tk = time.time()
        # workingdf['keystone_run_id'] = pd.to_numeric(workingdf['keystone_run_id'])
        # workingdf['keystone_team_id'] = pd.to_numeric(workingdf['keystone_team_id'])
        # workingdf['mythic_level'] = pd.to_numeric(workingdf['mythic_level'])
        # workingdf['clear_time_ms'] = pd.to_numeric(workingdf['clear_time_ms'])
        # workingdf['keystone_time_ms'] = pd.to_numeric(workingdf['keystone_time_ms'])
        # workingdf['num_chests'] = pd.to_numeric(workingdf['num_chests'])
        # workingdf['time_remaining_ms'] = pd.to_numeric(workingdf['time_remaining_ms'])
        # workingdf['num_modifiers_active'] = pd.to_numeric(workingdf['num_modifiers_active'])
        print("tk:", time.time()-tk)
        return(workingdf)
        # print(workingdf)
            

t = asyncio.run(main())
print(t)
print("--- %s seconds ---" % (time.time() - start_time))

time.sleep(60)