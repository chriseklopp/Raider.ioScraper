# -*- coding: utf-8 -*-
"""
Created on Thu May  6 11:59:57 2021

@author: chris
"""

# import datetime
import pandas as pd
import os


# get file names
mdirectory = 'D:\mythic_run_data'
files = os.listdir(mdirectory)
files = [x for x in files if ".csv" in x]
files = [x for x in files if "clean" in x]
names = [x for x in files if "names" in x]
runs = [x for x in files if "runs" in x]

def run_fmt(runs_df):

    
    runs_df.loc[runs_df.mythic_level < 10,'Affix4'] = 'None'
    runs_df.loc[runs_df.mythic_level < 7,'Affix3'] = 'None'
    runs_df.loc[runs_df.mythic_level < 4, 'Affix2'] = 'None'
    

    
    runs_df = runs_df[~runs_df.index.duplicated(keep='first')]








spec_list = [
        'Blood','Frost_D','Unholy',
        'Havoc','Vengeance',
        'Balance','Feral','Guardian','Resto_D',
        'Beast_Mastery','Marksmanship','Survival',
        'Arcane','Fire','Frost_M',
        'Brewmaster','Mistweaver','Windwalker',
        'Holy_Pa','Prot_P','Retribution',
        'Discipline','Holy_Pr','Shadow',
        'Assassination','Outlaw','Subtlety',
        'Elemental','Enhancement','Resto_S',
        'Affliction','Demonology','Destruction',
        'Arms','Fury','Prot_W']


    
    

# def final_combine(joined_df):
    
#     for i in joined_df.columns:
#             if 'Char' in i:
#                 joined_df[i] =  joined_df[i].apply(fix_bm)

#     for i in spec_list:
#         joined_df[i] = 0
        
#     for row in ['Char1','Char2','Char3','Char4','Char5']:
#         print(row)
#         for spec in spec_list:
#             joined_df.loc[joined_df[row].str.split("-").str[1] == spec, spec] +=1
    
#     print(r_path[:-13]+'combined'+r_path[-4:])
    
#     joined_df.to_csv(r_path[:-13]+'combined'+r_path[-4:])
    




for i,run in enumerate(runs):
    n_path = os.path.join(mdirectory,names[i])
    r_path = os.path.join(mdirectory,runs[i])

    names_df = pd.read_csv(n_path,index_col="keystone_run_id")
    runs_df = pd.read_csv(r_path,index_col="keystone_run_id")
    joined_df = runs_df.join(names_df, how = "outer")
    final_combine(joined_df)



    
# #testing purposes
# runs_df_head = runs_df.iloc[:200]

# names_df_head = names_df.iloc[:200]


# # jointest = runs_df_head.join(names_df_head, how = "outer")


# # tells rows with missing vals
# # null_data = joined_df[joined_df.isnull().any(axis=1)]

# spec_list = [
#         'Blood','Frost','Unholy',
#         'Havoc','Vengeance',
#         'Balance','Feral','Guardian','Resto_D',
#         'Beast_Mastery','Marksmanship','Survival',
#         'Arcane','Fire','Frost',
#         'Brewmaster','Mistweaver','Windwalker',
#         'Holy_Pa','Prot_P','Retribution',
#         'Discipline','Holy_Pr','Shadow',
#         'Assassination','Outlaw','Subtlety',
#         'Elemental','Enhancement','Resto_S',
#         'Affliction','Demonology','Destruction',
#         'Arms','Fury','Prot_W']


# for i in spec_list:
#     joined_df[i] = 0

# jointest = joined_df.iloc[:200]

# def fix_bm(x): #remove the space form beast mastery
#     if 'Beast Mastery' in x:
#         x = x.replace(' ','_')
#         return(x)
#     else:
#         return(x)
    

# for i in joined_df.columns:
#         if 'Char' in i:
#             joined_df[i] =  joined_df[i].apply(fix_bm)
            
            
# # def spec_indicators(i,char):
# #         try:
# #               # print(i,char)
# #               x = joined_df.loc[i,char].split("-")
# #               # print(x)
# #               y = x[1]
# #               joined_df.loc[i,y] += 1
# #         except IndexError as e:
# #             print(e)
# #             print(joined_df.loc[i,char])
# #         except KeyError as k:
# #             print(k)
# #             print(joined_df.loc[i,char])
# #             if joined_df.loc[i,char] in spec_list:
# #                 x = joined_df.loc[i,char]
# #                 joined_df.loc[i,x] += 1



# # for i in joined_df.index:
# #         for char in ['Char1','Char2','Char3','Char4','Char5']:
# #             spec_indicators(i,char)



# # set fixed value to 'c2' where the condition is met

# # df[df.name.str.contains('|'.join(search_values ))]


# # apply specs to their correct rows
# for row in ['Char1','Char2','Char3','Char4','Char5']:
#     print(row)
#     for spec in spec_list:
#         joined_df.loc[joined_df[row].str.split("-").str[1] == spec, spec] +=1



# joined_df.to_csv(r_path[:-13]+'combined'+r_path[-4:])


# jointest.loc["Vengeance" in jointest['Char1'].split(1)]

# runs_df.loc[runs_df[char] spec,'Affix4'] = 'None'

# print(r_path[:-13]+'combined'+r_path[-4:])

# names_df.to_csv(r_path[:-13]+'combined'+r_path[-4:])

