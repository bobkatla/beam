import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import geopandas as gpd
#%%

incomebins = [-1,20000,35000,50000,75000,100000,150000,1e10]

urbansim = "/Users/zaneedell/Desktop/git/smart-analysis/polaris-compare/data/austin/"
hh = pd.read_csv(urbansim+'households.csv.gz')
per = pd.read_csv(urbansim+'persons.csv.gz')

pwd = '/Users/zaneedell/Desktop/git/beam/src/main/python/events_analysis'

outfolder = "/polaris-out/austin-updated/"

hh['incomeBin'] = np.digitize(hh['income'],incomebins)

income_counts = hh.incomeBin.value_counts()
income_counts = income_counts/income_counts.sum()
#%%
Household = pd.Series()
Household['<$20k'] = income_counts[1]
Household['$20k - $35k'] = income_counts[2]
Household['$35k - $50k'] = income_counts[3]
Household['$50k - $75k'] = income_counts[4]
Household['$75k - $100k'] = income_counts[5]
Household['$100k - $150k'] = income_counts[6]
Household['>$150k'] = income_counts[7]


#%%
tenure_counts = hh.tenure.value_counts()
tenure_counts = tenure_counts/tenure_counts.sum()

Household['Own'] = tenure_counts[1]
Household['Rent'] = tenure_counts[2]

#%%
cars_counts = hh.auto_ownership.value_counts()
cars_counts = cars_counts / cars_counts.sum()
Household['0-Vehicle Household'] = cars_counts[0]
Household['1-Vehicle Household'] = cars_counts[1]
Household['2-Vehicle Household'] = cars_counts[2]
Household['3+ Vehicle Household'] = cars_counts[3] + cars_counts[4]
#%%
size_counts = per.groupby('household_id').agg({"person_id":"count"}).value_counts()
size_counts = size_counts / size_counts.sum()
Household['1-Person Household'] = size_counts[1]
Household['2-Person Household'] = size_counts[2]
Household['3-Person Household'] = size_counts[3]
Household['4-Person Household'] = size_counts[4]
Household['5-Person Household'] = size_counts[5]
Household['6-Person Household'] = size_counts[6]
Household['7+ Person Household'] = sum([size_counts[key] for key in size_counts.keys() if key[0] > 7])

Household.to_csv(pwd+outfolder +  "Household.csv")

#%%
Person = pd.Series()
agebins = [-1,15,25,35,45,55,65,200]
per['agebin'] = np.digitize(per['age'],agebins)
age_counts = per.agebin.value_counts()
age_counts = age_counts / age_counts.sum()

Person['<15 years'] = age_counts[1]
Person['15-24 years'] = age_counts[2]
Person['25-34 years'] = age_counts[3]
Person['35-44 years'] = age_counts[4]
Person['45-54 years'] = age_counts[5]
Person['55-64 years'] = age_counts[6]
Person['65+ years'] = age_counts[7]

#%%
race_counts = per.race_id.value_counts()
race_counts = race_counts / race_counts.sum()

Person['White'] = race_counts[1]
Person['Black'] = race_counts[2]
Person['NativeAmerican'] = race_counts[3] + race_counts[4] + race_counts[5]
Person['Asian'] = race_counts[6]
Person['Other'] = race_counts[7] + race_counts[8] + race_counts[9]

#%%
edu = dict()
for i in range(16):
    edu[i] = "<HS"
edu[16] = "HS"
edu[17] = "HS"
edu[18] = "<College"
edu[19] = "<College"
edu[20] = "<College"
edu[21] = "College"
edu[22] = "College"
edu[23] = "College"
edu[24] = "College"

per['edu2'] = per['edu'].apply(lambda x: edu[x])

edu_counts = per['edu2'].value_counts()
edu_counts = edu_counts / edu_counts.sum()


#%%
emp = {0:'NILF',1:'Employed'}
inc_emp = per[['edu2','worker','person_id']].groupby(['edu2','worker']).agg('nunique')
for row in inc_emp.iterrows():
    Person[row[0][0] + ' ' + emp[row[0][1]]] = row[1].person_id / inc_emp['person_id'].sum()
    
Person.to_csv(pwd+outfolder +  "Person.csv")
#%%
activities = pd.read_csv(urbansim+'new-austin-plans.csv.gz')
activities = activities.loc[activities.ActivityElement == 'activity']
activities['arrival_time']=0
#%%
activities.iloc[1:,10]= activities.iloc[:-1,9].values #TODO: index by column names
activities['departure_time'].fillna(24,inplace=True)
activities['arrival_time'].fillna(0,inplace=True)
activities['duration_min'] = (activities['departure_time'] - activities['arrival_time']) * 60
#%%
acts = {'Home':'home', 'work':'Primary Work', 'othmaint':'Other', 'social':'Social', 'univ':'School', 'othdiscr':'Other', 'escort':'Pickup-Dropoff','eatout':'Eat Out', 'atwork':'Primary Work', 'Work':'Primary Work', 'shopping':'Shopping', 'school':'School'}

activities['act'] = activities['ActivityType'].apply(lambda x: acts[x])
act_dur = activities[['act','duration_min']].groupby('act').agg('mean')

#%%
plans = pd.read_csv(urbansim+'15.plans.csv.gz')
#%%
plans = plans.loc[plans.planSelected,:]
legindex = np.where(plans.planElementType.str.contains("Leg"))[0]
#%%
plans.iloc
legs = plans.loc[plans.planElementType.str.contains("Leg"),['personId','planElementIndex','legMode','legDepartureTime','legTravelTime','legRouteType','legRouteTravelTime','legRouteDistance']]
legs['startLocation'] = gpd.points_from_xy(plans.iloc[legindex-1,7],plans.iloc[legindex-1,8])
legs['endLocation'] = gpd.points_from_xy(plans.iloc[legindex+1,7],plans.iloc[legindex+1,8])
legs['prevActivityType'] = plans.iloc[legindex-1,6].values
legs['prevActivityType'] = legs['prevActivityType'].apply(lambda x: acts[x])


legs['nextActivityType'] = plans.iloc[legindex+1,6].values
legs['nextActivityType'] = legs['nextActivityType'].apply(lambda x: acts[x])

DistanceMean = legs.groupby('nextActivityType').agg('mean')['legRouteDistance']/1609.34
DistanceMean.to_csv(pwd+outfolder +  "DistanceMean.csv")
#%%
activities = plans.loc[plans.planElementType.str.contains("Activity"),['personId','planElementIndex','activityType','activityLocationX','activityLocationY','activityEndTime']]
activities['activityStartTime'] = 0.0
activities.activityEndTime.replace(-np.inf, 24*3600, inplace=True)
activities.activityEndTime.replace(np.inf, 24*3600, inplace=True)
activities.activityStartTime.replace(-np.inf, 24*3600, inplace=True)
activities.loc[activities.planElementIndex > 0, 'activityStartTime'] = legs['legDepartureTime'].fillna(0).values + legs['legTravelTime'].fillna(0).values
activities['duration'] = activities.activityEndTime - activities.activityStartTime
activities.loc[activities['duration'] < 0, 'duration'] = 0
activities['activityType'] = activities['activityType'].apply(lambda x: acts[x])
activities.duration.replace(np.inf, np.nan, inplace=True)

for act in activities['activityType'].unique():
    sub = activities.loc[activities.activityType == act,'activityStartTime']
    n = sub.size
    plt.hist(sub/3600, bins = np.arange(24), density=True)
    plt.title(act + " start time")
    
    plt.ylabel('Portion of Activities')
    plt.xlabel('Hour of Day')
    plt.savefig(pwd + outfolder + act + '_startTime.png')
    plt.clf()

ActivityDuration = activities.groupby('activityType').agg('mean')['duration']/60.
ActivityDuration.to_csv(pwd+outfolder +  "ActivityDuration.csv")


#%%
legs["TripType"] = "NHB"
legs.loc[(legs.prevActivityType == "home"), "TripType" ] = "HBO"
legs.loc[(legs.nextActivityType == "home"), "TripType"] = "HBO"
legs.loc[(legs.prevActivityType == "home") & (legs.nextActivityType == "Primary Work"), "TripType"] = "HBW"
legs.loc[(legs.prevActivityType == "PrimaryWork") & (legs.nextActivityType == "home"), "TripType"] = "HBW"
legs.groupby(['TripType','legMode']).agg({'personId':'count'}).unstack().transpose().to_csv(pwd+outfolder +  "ModeSplitByPurpose.csv")


#%%
border = gpd.read_file('/Users/zaneedell/Desktop/git/smart-analysis/polaris-compare/data/austin/districts.zip').dissolve()
legsGDF = gpd.GeoDataFrame(legs, geometry='startLocation')
joined = gpd.sjoin(legsGDF, border.to_crs('EPSG:26910'), op='within', how='left')
joined.fillna(-1).groupby(['single_mem','legMode']).agg({'personId':'count'}).unstack().transpose().to_csv(pwd+outfolder +  "ModeSplitByCity.csv")
