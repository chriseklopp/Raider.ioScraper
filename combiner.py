# -*- coding: utf-8 -*-
"""
Created on Tue Apr 27 18:20:19 2021

Combine dungeon data into a combined data set for each dungeon. Also combine names and run data

"""

import pandas as pd
import os
import gc

pd.set_option("max_columns",11)

dungeons = ['sanguine-depths','de-other-side','halls-of-atonement',
            'mists-of-tirna-scithe','plaguefall','spires-of-ascension','the-necrotic-wake','theater-of-pain']



        
# get file names
mdirectory = 'D:\mythic_run_data'

files = os.listdir(mdirectory)
# files = [x for x in files if ".csv" in x]
# files = [x for x in files if dung in x]
def combine():   
    # dung = dung
    
    files = os.listdir(mdirectory)
    name_path = os.path.join('D:\mythic_run_data',dung +'_namescomb.csv')
    run_path = os.path.join('D:\mythic_run_data',dung + '_runscomb.csv')
    join_path = os.path.join('D:\mythic_run_data',dung + '_combined.csv')
    
    
    # names = [x for x in dfiles if "names" in x]
    
    files = [x for x in files if ".csv" in x]
    files = [x for x in files if dung in x]
    files = [x for x in files if not 'combined' in x]
    names = [x for x in files if "name" in x]
    runs = [x for x in files if "mythic" in x]

        
    names_paths = [os.path.join(mdirectory,x) for x in names]

    run_paths = [os.path.join(mdirectory,x) for x in runs]

    print('Concating')
    rdf_list = [pd.read_csv(file,index_col="keystone_run_id") for file in run_paths]
    ndf_list = [pd.read_csv(file,index_col="keystone_run_id") for file in names_paths]
    
    
    run_df = pd.concat(rdf_list)
    
    name_df = pd.concat(ndf_list)
    
    joined_df = run_df.join(name_df, how = "outer")
    joined_df.drop(columns = ['Unnamed: 0'],inplace= True)
    
    print('saving')
    run_df.to_csv(run_path)
    name_df.to_csv(name_path)
    joined_df.to_csv(join_path)
    


    
    del run_df
    del name_df
    del joined_df
    gc.collect()
    
for dung in dungeons:
    print(dung)
    combine()
    
    
    
    
    
    
    
    
    
    
# files = [x for x in files if ".csv" in x]
# dfiles = [x for x in files if dungeon in x]

# for item in names:
#     name_path = os.path.join('D:\mythic_run_data',dung +'_names.csv')
    
# names = [x for x in dfiles if "names" in x]

# runs = [x for x in dfiles if "runs" in x]

# name_path = os.path.join('D:\mythic_run_data',dung +'_names.csv')
# run_path = os.path.join('D:\mythic_run_data',dung + '_runs.csv')

# n_path = os.path.join(mdirectory,names[0])

# names_df = pd.read_csv(n_path,index_col="keystone_run_id")


# names_df_head = names_df.head



# # for dung in   
# #     dung = 'theater-of-pain'
    
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

# # print(names_df.head)

# # print(walk)
# # for a,b,c in walk:
#     # print(a,b,c)
#     # print(c)    


# # del my_array
# # del my_object
# # gc.collect()

# name = "Gerrith-Dalaran"


# n1 = name_df[name_df['Name1'].str.contains(name)]

# n2= name_df[name_df['Name2'].str.contains(name)]

# n3 = name_df[name_df['Name3'].str.contains(name)]

# n4 = name_df[name_df['Name4'].str.contains(name)]

# n5 = name_df[name_df['Name5'].str.contains(name)]

# print(len(n1.index) + len(n2.index) + len(n3.index) + len(n4.index) + len(n5.index))
# # merged_df.loc[]['Name2'].str.find("Gerrith")






