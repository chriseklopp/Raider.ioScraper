# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 18:20:19 2021



"""


# import datetime
import pandas as pd
import os
import gc




spec_list = [
        'Blood','Frost_D','Unholy',
        'Havoc','Vengeance',
        'Balance','Feral','Guardian','Resto_D',
        'Beast Mastery','Marksmanship','Survival',
        'Arcane','Fire','Frost_M',
        'Brewmaster','Mistweaver','Windwalker',
        'Holy_Pa','Prot_P','Retribution',
        'Discipline','Holy_Pr','Shadow',
        'Assassination','Outlaw','Subtlety',
        'Elemental','Enhancement','Resto_S',
        'Affliction','Demonology','Destruction',
        'Arms','Fury','Prot_W']

spec_list_wdashes = ['-' + spec for spec in spec_list]



def run_fmt(df):

    print('remove affixes')
    df.loc[df.mythic_level < 10,'Affix4'] = 'None'
    df.loc[df.mythic_level < 7,'Affix3'] = 'None'
    df.loc[df.mythic_level < 4, 'Affix2'] = 'None'
    
    
    for i in spec_list:
        df[i] = 0



    print('indicators')
    for i,spec in enumerate(spec_list_wdashes):
        print(spec)
        df.loc[df['Char1'].str.contains(spec) | df['Char2'].str.contains(spec)  | df['Char3'].str.contains(spec) | df['Char4'].str.contains(spec) | df['Char5'].str.contains(spec),spec_list[i]] +=1
    # for i in df.index:
    #     for char in ['Char1','Char2','Char3','Char4','Char5']:
    #         # print(char)
    #         spec_indicators(i,char)
    print('remove dupes')
    df.set_index('keystone_run_id',inplace = True)
    df = df[~df.index.duplicated(keep='first')]
    print(df.index.is_unique)
    print('Saving')
    df.to_csv(item[:-4]+'clean'+item[-4:])
    print('Saved')
    
    
        
# get file names
mdirectory = 'D:\mythic_run_data'
files = os.listdir(mdirectory)
files = [x for x in files if ".csv" in x]
files = [x for x in files if  "combined" in x]
files = [x for x in files if not "clean" in x]


paths = [os.path.join(mdirectory,x) for x in files]

# names = [x for x in files if "names" in x]
# runs = [x for x in files if "runs" in x]

for item in paths:
    df = pd.read_csv(item)#,index_col="keystone_run_id")
    df_head = df[:1609]
    run_fmt(df)
    del df
    gc.collect()
    
    
    
    
    
    
    # def combine():   
#     dung = 'theater-of-pain'
    
#     name_path = os.path.join('D:\mythic_run_data',dung +'_names.csv')
#     run_path = os.path.join('D:\mythic_run_data',dung + '_runs.csv')
    
    
    
    
#     root = r'D:\mythic_run_data'
#     root = os.path.join(root,dung)
#     print(root)
    
#     listy = os.listdir(root)
#     name_list = []
#     run_list = []
    
#     for file in listy:
#         if 'name' in file:
#             name_list.append(os.path.join(root,file))
#         else:
#             run_list.append(os.path.join(root,file))
    
#     rdf_list = [pd.read_csv(file,index_col="keystone_run_id") for file in run_list]
#     ndf_list = [pd.read_csv(file,index_col="keystone_run_id") for file in name_list]
    
#     run_df = pd.concat(rdf_list)
#     name_df = pd.concat(ndf_list)
    
#     run_df.to_csv(run_path)
#     name_df.to_csv(name_path)
# def spec_indicators(i,char):
#       try:
#             # testdf = df[df['Char1'].str.contains('Arcane') | df['Char2'].str.contains('Arcane')  | df['Char3'].str.contains('Arcane')]
#             df.loc[df['Char1'].str.contains('Vengeance') | df['Char2'].str.contains('Vengeance')  | df['Char3'].str.contains('Vengeance'), 'Vengeance'] +=1
#             print(df.loc['Vengenace'])
#             # print(i,char)
#             # print(df.loc[i,char])
#             x = df.loc[i,char].split("-")
#             # print(x)
#             y = x[1]
#             df.loc[i,y] += 1
#       except IndexError as e:
#           print(e)
#           print(df.loc[i,char])
#       except KeyError as k:
#           print(k)
#           print(df.loc[i,char])
#           if df.loc[i,char] in spec_list:
#               x = df.loc[i,char]
#               df.loc[i,x] += 1   




# df.loc[df.mythic_level < 10,'Affix4'] = 'None'
# n_path = os.path.join(mdirectory,names[2])



# for i,thing in enumerate(names):
#     n_path = os.path.join(mdirectory,names[i])
#     names_df = pd.read_csv(n_path,index_col="keystone_run_id")
#     names_fmt(names_df)


# # Formatting for names df

# def names_fmt(names_df):

#     names_df.drop(columns=['Unnamed: 0'],inplace=True)
    
    
#     def name_formatter(series): #reformats name columns so they dont have spaces, use with apply
#         if series == 'None':
#             return(series)
#         if "Azjol-Nerub" in series:
#              x = series.split("Nerub")
#              x[0] = x[0][:-1] + '_Nerub'
#              x = ''.join(x)
#              return(x)
#         if "Arak-arahm" in series:
#             x = series.split("arahm")
#             x[0] = x[0][:-1] + 'arahm'
#             x = ''.join(x)
    
#             return(x)
             
#         x = series.split("-")
    
#         if len(x) > 3:
#             x[1] = int(x[1])
#             del x[1]
    
#         x[1] = x[1].replace(' ','_')
#         x[2] = x[2].replace(' ','_')
#         if x[2] == 'United_States_&_Oceania':
#             x[2] = 'US_OCE'
#         x = '-'.join(x)
#         return(x)

#     #         names_df[i] =  names_df[i].apply(name_formatter)
    
        
        
#     # names_df = names_df[~names_df.index.duplicated(keep='first')]
    
#     # print(names_df.index.is_unique)
#     #save to csv
#     # names_df.to_csv(n_path[:-4]+'clean'+n_path[-4:])

# # names_fmt(names_df)



# r_path = os.path.join(mdirectory,runs[2])


# runs_df = pd.read_csv(r_path,index_col="keystone_run_id")



# # for i,thing in enumerate(runs):
# #     r_path = os.path.join(mdirectory,runs[i])
# #     runs_df = pd.read_csv(r_path,index_col="keystone_run_id")
# #     run_fmt(runs_df)
# # name = "Gerrith-Dalaran"


# for i,thing in enumerate(names):
#     n_path = os.path.join(mdirectory,names[i])
#     names_df = pd.read_csv(n_path,index_col="keystone_run_id")
#     names_fmt(names_df)




# n1 = name_df[name_df['Name1'].str.contains(name)]

# n2= name_df[name_df['Name2'].str.contains(name)]

# n3 = name_df[name_df['Name3'].str.contains(name)]

# n4 = name_df[name_df['Name4'].str.contains(name)]

# n5 = name_df[name_df['Name5'].str.contains(name)]

# print(len(n1.index) + len(n2.index) + len(n3.index) + len(n4.index) + len(n5.index))
# # merged_df.loc[]['Name2'].str.find("Gerrith")






