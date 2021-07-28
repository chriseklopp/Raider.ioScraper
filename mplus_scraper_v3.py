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




def run_formatter(html): #takes a html text, outputs a df
    t0 = time.time()
    soup = BeautifulSoup(html, 'lxml')

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
    # score_vector = [item.split(':',maxsplit=2)[0] for item in rank_split_trim]
    # score_vector = [float(item.split(',',maxsplit=1)[0]) for item in score_vector]
    
    score_vector = [float((item.split(':',maxsplit=2)[0]).split(',',maxsplit=1)[0]) for item in rank_split_trim]

    # cuts out score shit; preps for json conversion
    rank_split_trim = [n.split(':', maxsplit=1)[1] for n in rank_split_trim]
    rank_split_trim = [n[:-3] for n in rank_split_trim]
    
    # print(rank_split_trim)
    # time.sleep(1)






    # start of formatter from v1
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
        # add char keys to top layer. MAKE THIS SECOND DATASET.
            rank_row_json[char_string] = rank_row_json['roster'][charac] #TAKES .015 SECONDS TO RUN
        
        #remove roster key
        rank_row_json.pop('roster')
        
        row_series = pd.Series(rank_row_json)
        
        if run_enum == 0:
            df= pd.DataFrame(row_series)
            df = df.T #transposes dataframe
        else:
            df= df.append(row_series,ignore_index=True)
        

    t1 = time.time()
    print("P1:", t1-t0)
    return(df)



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
        for number in range(25, 100):
            url = f'https://raider.io/mythic-plus-rankings/season-sl-1/all/world/leaderboards/{number}#content'
            tasks.append(asyncio.ensure_future(get_page(session, url)))

        page_set = await asyncio.gather(*tasks)
        it = time.time()
        print(it - gt,"h")
        for i,page in enumerate(page_set):

            print(i)
            page_df = run_formatter(page)
            # print(page_df)
            workingdf = workingdf.append(page_df,ignore_index=True)
            
        tk = time.time()
        workingdf['keystone_run_id'] = pd.to_numeric(workingdf['keystone_run_id'])
        workingdf['keystone_team_id'] = pd.to_numeric(workingdf['keystone_team_id'])
        workingdf['mythic_level'] = pd.to_numeric(workingdf['mythic_level'])
        workingdf['clear_time_ms'] = pd.to_numeric(workingdf['clear_time_ms'])
        workingdf['keystone_time_ms'] = pd.to_numeric(workingdf['keystone_time_ms'])
        workingdf['num_chests'] = pd.to_numeric(workingdf['num_chests'])
        workingdf['time_remaining_ms'] = pd.to_numeric(workingdf['time_remaining_ms'])
        workingdf['num_modifiers_active'] = pd.to_numeric(workingdf['num_modifiers_active'])
        print("tk:", time.time()-tk)
        return(workingdf)
        # print(workingdf)
            

t = asyncio.run(main())
print(t)
print("--- %s seconds ---" % (time.time() - start_time))

time.sleep(60)