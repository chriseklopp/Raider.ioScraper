
import json
import pandas as pd
import time

pd.set_option('display.max_columns', None)
file = r"E:\Spring 2021\math 268\mythicplus\responsmplus.json"

data = json.load(open(file,encoding='UTF-8'))




def run_format(data): #takes a raider io mythic plus json and returns it as dataframe
    def affix_extract(x):
            x = x['name']
            return(x)
    
    
    def roster_clean(x):
        
        del x['oldCharacter']
        del x['isTransfer']
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
    t0 = time.time()
    df = pd.DataFrame(data['rankings']) # top layer.
    
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
    
        
    
    # extract information from roster characters to top level.
    
    # for i in roster.index:
    #     print(df.iloc[i])
    
    
    # change beast mastery to beast_mastery,
    # Prot pal to Prot_P; Prot Warr to Prot_W
    # Resto sham to Resto_S; Resto druid to Resto_D
    # Holy priest to Holy_Pr; Holy pally to H_Pa
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
    
    role_list = ['tank','healer','dps'] # probably not necessary
    
    factions = ['horde','alliance']
    
    for i in spec_list:
        roster[i] = 0
        
    for i in role_list:
        roster[i] = 0
    
    # roster['faction'] = 0
    
    roster.rename(columns={0:'Char1',
                           1:'Char2',
                           2:'Char3',
                           3:'Char4',
                           4:'Char5'}, inplace = True)
    
    def faction_finder(x):
        indicator = None
        y = roster.loc[x,'Char1']
        z = y['character']
        if z['faction'] == 'alliance':
            indicator = 'A'
        if z['faction'] == 'horde':
            indicator = 'H'
        return(indicator)
    
    
    # for i in roster.index:
    #     roster.loc[i,'faction'] = faction_finder(i)
    
    def spec_indicators(i,char):
            y = roster.loc[i,char]
            z = y['character']
            # print(y)
            # print(type(y))
            spec = z['spec']
            roster.loc[i,spec] += 1
    
    def role_indicators(i,char):
          y = roster.loc[i,char]
          role = y['role']
          roster.loc[i,role] += 1
          
        
    for i in roster.index:
        for char in ['Char1','Char2','Char3','Char4','Char5']:
            spec_indicators(i,char)
      
    for i in roster.index:
        for char in ['Char1','Char2','Char3','Char4','Char5']:
            role_indicators(i,char)
            
            
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


mythic_run_info = run_format(data) 


# create function for adding updates to dataset
def update_data(old_data, new_data): #combines two formatted datasets
    data = old_data.append(new_data)
    return(data)

