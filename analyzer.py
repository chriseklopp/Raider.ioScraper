# -*- coding: utf-8 -*-
"""
Graph and analyze data.

@author: chris
"""
import gc
import pandas as pd
import os
import plotnine as p9
import numpy as np
import matplotlib.pyplot as plt
from prettytable import PrettyTable 
  


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

class_list = ['Death_Knight','Demon_Hunter','Druid','Hunter',
              'Mage','Monk','Paladin','Priest',
              'Rogue','Shaman','Warlock','Warrior']

#color position corresponds to their class position
class_colors = ['#C41E3A','#A330C9','#FF7C0A','#AAD372',
                '#3FC7EB','#00FF98','#F48CBA','#FFFFFF',
                '#FFF468','#0070DD','#8788EE','#C69B6D']

# get file names
mdirectory = 'D:\mythic_run_data'  #location of files.
files = os.listdir(mdirectory)
files = [x for x in files if ".csv" in x]
files = [x for x in files if "combined" in x]
files = [x for x in files if "clean" in x]

path_list = [os.path.join(mdirectory,i) for i in files]


df = pd.read_csv(path_list[0])
df_head = df[:2000]

#### Compare proportion of timed runs by healer in key range 15-20 



healers = ['Resto_D', 'Mistweaver', 'Holy_Pa',
           'Discipline', 'Holy_Pr', 'Resto_S']

tanks = ['Blood','Vengeance', 'Guardian',
         'Brewmaster','Prot_P','Prot_W']

healer_runs_array_15 = np.zeros((2,6))
tank_runs_array_15 = np.zeros((2,6))

for dung in path_list:
    print(dung)
    df = pd.read_csv(dung)
    df_15 = df.loc[df.mythic_level >= 15]
    df_15 = df_15.loc[df_15.mythic_level <= 20]
    
      
    for i, spec in enumerate(healers): #run for each healer in healer list
        print(i)
        print(spec)
        # print(df[spec])
        
        
        df_spec = df_15.loc[df_15[spec] > 0]


        in_time = df_spec.loc[df_spec.time_remaining_ms > 0]
        out_time = df_spec.loc[df_spec.time_remaining_ms < 0]
        
        
        
        healer_runs_array_15[0,i] += len(in_time)
        healer_runs_array_15[1,i] += len(out_time)
    
    for i, spec in enumerate(tanks): #run for each healer in healer list
        print(i)
        print(spec)
        # print(df[spec])
        
        
        df_spec = df_15.loc[df_15[spec] > 0]


        in_time = df_spec.loc[df_spec.time_remaining_ms > 0]
        out_time = df_spec.loc[df_spec.time_remaining_ms < 0]
        
        
        
        tank_runs_array_15[0,i] += len(in_time)
        tank_runs_array_15[1,i] += len(out_time)
    
    del df

healer_proportions = healer_runs_array_15[0] / (healer_runs_array_15[0] + healer_runs_array_15[1])
tank_proportions = tank_runs_array_15[0] / (tank_runs_array_15[0] + tank_runs_array_15[1])
# healer_runs_array_15.append(healer_runs_array_15[0] / (healer_runs_array_15[0] + healer_runs_array_15[1]))






propheal_round = np.around(healer_proportions, decimals=3)
proptank_round = np.around(tank_proportions, decimals=3)


healer_runs_df = pd.DataFrame([propheal_round], columns = healers)
healer_runs_df = healer_runs_df.sort_values(healer_runs_df.last_valid_index(), axis=1,ascending = False)



tank_runs_df = pd.DataFrame([proptank_round], columns = tanks)
tank_runs_df = tank_runs_df.sort_values(tank_runs_df.last_valid_index(), axis=1,ascending = False)





mytable = PrettyTable(["Tank", "Proportion Timed (15-20)"]) 
  
# Add rows 
for i,spec in enumerate(tank_runs_df.columns):
    # print(tank_runs_df.iloc[0,i])
    mytable.add_row([spec,tank_runs_df.iloc[0,i]]) 

print(mytable)





#### SPECS PER DUNG LEVEL

# create numpy array for each dungeon, rows are across levels, columns are across specs, (x = spec, y = level)
dung_array = np.zeros((36,30))

for dung in path_list:
    print(dung)
    df = pd.read_csv(dung)
    specs_df = df[spec_list]
    specs_df['mythic_level'] = df['mythic_level']
    
    for dlevel in range(0,30): #0-29 dungeon levels, corresponding to columns in dataframe
        clevel = specs_df.loc[specs_df.mythic_level == dlevel] 
        
        for i,spec in enumerate(spec_list): #36 specs, corresponding to rows in df
            dung_array[i,dlevel] += clevel[spec].sum()
            
    del df
    gc.collect()



# dung_array_sum = dung_array.sum(axis=0)
# xx = dung_array/dung_array.sum(axis=0,keepdims=1)
# xx_df = pd.DataFrame(xx)

# xx_df['specs'] = spec_list

# ac = pd.melt(xx_df, id_vars=['specs'], value_vars=[x for x in range(0,30)])

dung_level_df = pd.DataFrame(dung_array, index = spec_list) #probably have to add columns to this


# FACTION COUNTS

faction_dict = {'alliance': int(0), 'horde': int(0)} 

for dung in path_list:
    print(dung)
    df = pd.read_csv(dung)
    faction_counts = df['faction'].value_counts().to_dict()
    faction_dict['alliance'] +=faction_counts['alliance']
    faction_dict['horde'] +=faction_counts['horde']
    print(faction_dict)
    # faction_dict + faction_counts
    del df
    gc.collect()
    

# FACTION COUNTS 15+

faction_dict_15 = {'alliance': int(0), 'horde': int(0)}
 
for dung in path_list:
    print(dung)
    df = pd.read_csv(dung)
    over_15 = df.loc[df.mythic_level >= 15]
    faction_counts_15 = over_15['faction'].value_counts().to_dict()
    faction_dict_15['alliance'] +=faction_counts_15['alliance']
    faction_dict_15['horde'] +=faction_counts_15['horde']
    print(faction_dict_15)
    del df
    gc.collect()



#Faction Counts 20? +


faction_dict_20 = {'alliance': int(0), 'horde': int(0)}
 
for dung in path_list:
    print(dung)
    df = pd.read_csv(dung)
    over_20 = df.loc[df.mythic_level >= 20]
    faction_counts_20 = over_20['faction'].value_counts().to_dict()
    faction_dict_20['alliance'] +=faction_counts_20['alliance']
    faction_dict_20['horde'] +=faction_counts_20['horde']
    print(faction_dict_20)
    del df
    gc.collect()




# Dungeon Counts


dung_dict =  {dg: int(0) for dg in path_list}


for dung in path_list:
    df = pd.read_csv(dung)
    dung_dict[dung] += len(df)

    del df
    gc.collect()



# Count In-time by dungeon

countin_dung_dict =  {dg: int(0) for dg in path_list}

# df_h = df[:2000]
for dung in path_list:
    df = pd.read_csv(dung)
    in_time = df.loc[df.time_remaining_ms > 0 ]
    countin_dung_dict[dung] += len(in_time)

    del df
    gc.collect()


prop_dung_dict = countin_dung_dict/(countin_dung_dict + dung_dict)


# In time by dung level

in_time_dict = {i: int(0) for i in range(0,30)}


for dung in path_list:
    df = pd.read_csv(dung)
    in_time = df.loc[df.time_remaining_ms > 0 ]
    for dlevel in range(0,30):
        clevel = in_time.loc[in_time.mythic_level == dlevel]
        in_time_dict[dlevel] += len(clevel)


    del df
    gc.collect()


# print(in_time_dict)

# Out time by dung level

out_time_dict = {i: int(0) for i in range(0,30)}


for dung in path_list:
    df = pd.read_csv(dung)
    out_time = df.loc[df.time_remaining_ms < 0 ]
    for dlevel in range(0,30):
        clevel = out_time.loc[out_time.mythic_level == dlevel]
        out_time_dict[dlevel] += len(clevel)


    del df
    gc.collect()
    
    
    
    
out_list = list(out_time_dict.values())[2:28]
in_list = list(in_time_dict.values())[2:28]

prop_list = []


for i,val in enumerate(in_list):
    try:
        prop_list.append(in_list[i]/(in_list[i]  +out_list[i]))
    except ZeroDivisionError:
        prop_list.append(0)

# prop_time_dict = in_time_dict/(in_time_dict + out_time_dict)



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


####@@@@@@@@ Stacked line plot?
# collapse specs into classes for visual clarity.

df = pd.DataFrame(dung_array, index = spec_list)

new_row = df.loc['Blood'] + df.loc['Frost_D'] + df.loc['Unholy']
new_row.name = 'Death_Knight'
df= df.append([new_row])

new_row = df.loc['Havoc'] + df.loc['Vengeance']
new_row.name = 'Demon_hunter'
df= df.append([new_row])

new_row = df.loc['Balance'] + df.loc['Feral'] + df.loc['Guardian'] + df.loc['Resto_D']
new_row.name = 'Druid'
df= df.append([new_row])

new_row = df.loc['Beast Mastery'] + df.loc['Marksmanship'] + df.loc['Survival']
new_row.name = 'Hunter'
df= df.append([new_row])

new_row = df.loc['Arcane'] + df.loc['Fire'] + df.loc['Frost_M']
new_row.name = 'Mage'
df= df.append([new_row])

new_row = df.loc['Brewmaster'] + df.loc['Mistweaver'] + df.loc['Windwalker']
new_row.name = 'Monk'
df= df.append([new_row])

new_row = df.loc['Holy_Pa'] + df.loc['Prot_P'] + df.loc['Retribution']
new_row.name = 'Paladin'
df= df.append([new_row])

new_row = df.loc['Discipline'] + df.loc['Holy_Pr'] + df.loc['Shadow']
new_row.name = 'Priest'
df= df.append([new_row])

new_row = df.loc['Assassination'] + df.loc['Outlaw'] + df.loc['Subtlety']
new_row.name = 'Rogue'
df= df.append([new_row])

new_row = df.loc['Elemental'] + df.loc['Enhancement'] + df.loc['Resto_S']
new_row.name = 'Shaman'
df= df.append([new_row])

new_row = df.loc['Affliction'] + df.loc['Demonology'] + df.loc['Destruction']
new_row.name = 'Warlock'
df= df.append([new_row])

new_row = df.loc['Arms'] + df.loc['Fury'] + df.loc['Prot_W']
new_row.name = 'Warrior'

df= df.append([new_row])
df_class = df.drop(spec_list)
df_class = df_class.drop(columns = [0,1,28,29])

df_class.update(df_class.div(df_class.sum(axis=0),axis=1))

# df = pd.Dataframe.from_dict()
df_class.reset_index(inplace = True)
df_class.rename(columns={"index": "Class"}, inplace= True)

# for i in df_class.columns[1:]:

df_class.iat[0,0] = 'Death Knight' #remove underscores manually
df_class.iat[1,0] = 'Demon Hunter'


df_class_l = pd.melt(df_class, id_vars=['Class'], value_vars=[x+2 for x in range(0,26)])



# p9.ggplot(xx_df, mapping = p9.aes(x = xx_df.columns, y = 'prop_of_total_class')) + p9.geom_area(position = 'stack')
# ax = xx_df.plot.bar(x=xx_df.columns, rot=0)
# acg = ac.plot.bar(x = 'variable', y = 'value')

# ac.dropna(inplace = True)
#works!! gives stacked BAR
p9.ggplot(df_class_l, p9.aes(x= 'variable', y= 'value'))+\
     p9.labs(title= "Class Representation by Key Level", x="Key Level", y = "Proportion") +\
        p9.theme(panel_background = p9.element_rect(color = "black", fill = "black"),plot_background=p9.element_rect(fill='#fffbde', alpha=.3),
                 panel_grid_minor = p9.element_blank(),
                 legend_background=p9.element_rect(color='white', size=2, fill='#63625d'),
                 axis_text_y = p9.element_text(margin={'t': 5, 'r':3},face="bold", color="black",),
                 axis_text_x = p9.element_text(margin={'t': 5, 'r':5},face="bold", color="black",)) +\
            p9.geom_bar(p9.aes(fill="Class"),stat="identity", position="fill",width = 1) +\
            p9.scale_fill_manual(values= class_colors) + p9.scale_y_continuous(expand = (0,0)) 
  
    
  
    
####@@@@@@@@ Stacked line plot? intime by level
# df = pd.Dataframe.from_dict()
# p9.ggplot(df, mapping = p9.aes(x = 'mythic_dungeon_level', y = 'prop_of_total_time')) + p9.geom_area(position = 'stack')

df_time = pd.DataFrame(prop_list)

df_time.reset_index(inplace=True)
df_time.rename(columns={'index':'level',0: "prop"}, inplace= True)
df_time['level'] +=2

# p9.ggplot(df_time, p9.aes(x='level', y= 'prop' ))+\
#     p9.theme(panel_background = p9.element_rect(color = "black", fill = "red"),plot_background=p9.element_rect(fill='#fffbde', alpha=.3),
#             panel_grid_minor = p9.element_blank(),
#             axis_text_y = p9.element_text(margin={'t': 5, 'r':3},face="bold", color="black",),
#             axis_text_x = p9.element_text(margin={'t': 5, 'r':5},face="bold", color="black",)) +\
#     p9.geom_area(fill = 'green') +\
#     p9.scale_fill_manual(values= class_colors) + p9.scale_y_continuous(limit = (0,1.25, breaks = [0,.25,.50,.75,1,1.25]) +\
#         p9.scale_x_continuous(breaks = range(2,2,2))
#     # p9.scale_y_discrete(expand = (0, 0))
#         # p9.scale_y_continuous(breaks = range(0, 1,.25))

# p9.ggplot(df_time) + p9.geom_bar(p9.aes(x=df_time.index))





# p9.ggplot(df_class_l, p9.aes(x= 'variable', y= 'value'))+\
#      p9.labs(title= "Class Representation by Key Level", x="Key Level", y = "Proportion") +\
#         p9.theme(panel_background = p9.element_rect(color = "black", fill = "black"),plot_background=p9.element_rect(fill='#fffbde', alpha=.3),
#                  panel_grid_minor = p9.element_blank(),
#                  legend_background=p9.element_rect(color='white', size=2, fill='#63625d'),
#                  axis_text_y = p9.element_text(margin={'t': 5, 'r':3},face="bold", color="black",),
#                  axis_text_x = p9.element_text(margin={'t': 5, 'r':5},face="bold", color="black",)) +\
#             p9.geom_bar(p9.aes(fill="Class"),stat="identity", position="fill",width = 1) +\
#             p9.scale_fill_manual(values= class_colors) + p9.scale_y_continuous(expand = (0,0)) 



fig = plt.figure()
# plt.figure(dpi=1200)
plt.fill_between(df_time['level'], df_time['prop'],color="#E0E0E0")
plt.xticks([x for x in range(2,30,2)])
plt.yticks([0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1])

plt.xlabel("Key Level")
plt.ylabel("Proportion")
# plt.title("Space for Title")
plt.axvline(x=15, ymin=0, ymax=.735,color = 'black')
plt.axvline(x=10, ymin=0, ymax=.7,color = 'black')
plt.axvline(x=2, ymin=0, ymax=.895,color = 'black')
plt.axvline(x=27, ymin=0, ymax=.685,color = 'black')
ax = plt.axes()
ax.set_facecolor("black")
# plt.margins(y=0)
fig.savefig('E:\Spring 2021\math 268\Elookthisup.png',dpi = 300, transparent = True)


####@@@@@@@@@ faction donut charts

faction_colors = ['#1f5099','#d10f0f'] # [ally,horde]
names = ['Alliance','Horde']


# faction_dict
# faction_dict_15
# faction_dict_20
# pd.Data

factionyo = list(faction_dict.values())
fig = plt.figure()
# faction_df  = pd.DataFrame(faction_dict,index = ['alliance','horde'] )
plt.pie(factionyo, colors = faction_colors)
# add a circle at the center to transform it in a donut chart
my_circle=plt.Circle( (0,0), 0.7,  color='black')
p=plt.gcf()
p.gca().add_artist(my_circle)
# plt.xaxis.label.set_color('red')

fig.savefig('faction.png', transparent=True)
plt.show()


#####'hardest'/'easiest' dung

total_dungy = list(dung_dict.values())
intime_dungy = list(countin_dung_dict.values())
sum(intime_dungy)
sum(total_dungy)
sum(intime_dungy)/sum(total_dungy)

prop_dungy = []
for i,val in enumerate(intime_dungy):
    prop_dungy.append(intime_dungy[i]/total_dungy[i])


# #'easiest dung'
# total_dungy = list(dung_dict.values())
# intime_dungy = list(countin_dung_dict.values())

# prop_dungy = []
# for i,val in enumerate(intime_dungy):
#     prop_dungy.append(total_dungy[i]-intime_dungy[i]/total_dungy[i])

#######most popular spec

#row sums on array

 
spec_sums=np.sum(dung_array,axis=1)  
max(spec_sums)
 


