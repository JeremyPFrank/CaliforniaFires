import pandas as pd
import numpy as np
import sqlite3
import datetime
import matplotlib.pyplot as plt

# read csv file and open connection to sqlite database
file = 'California_Fire_Perimeters_(all).csv'
data = pd.read_csv(file)
conn = sqlite3.connect("fire_data.db")
cursor = conn.cursor()

# select only relavant columns
data = data[['YEAR_', 'ALARM_DATE', 'CONT_DATE', 'CAUSE', 'GIS_ACRES']]

# calculate Length (difference between fire containment date and alarm date)
data['LENGTH'] = (pd.to_datetime(data['CONT_DATE']) - pd.to_datetime(data['ALARM_DATE'])) / datetime.timedelta(days=1) 

# store dataframe in sqlite
table_name = "fires"
data.to_sql(table_name, conn, if_exists='replace', index=False)

# clean data
delete_nulls = """
    DELETE FROM fires
    WHERE Year_ is NULL
        OR ALARM_DATE is NULL
        OR CONT_DATE is NULL
        OR CAUSE is NULL
        OR GIS_ACRES is NULL
        OR LENGTH is NULL
        OR LENGTH > 3000
        OR LENGTH < 0; """
cursor.execute(delete_nulls)

# run queries, store in variables, and print results in terminal
# .iloc[0, 0].item() transforms the sqlite output to a workable datatype 

total_rows = pd.read_sql_query("SELECT COUNT(*) FROM fires;", conn)
print('The dataset contains ' + str(total_rows.iloc[0, 0].item()) + ' fires without missing data') 

# fires per year from 1912 to 2023
fires_per_year = []
for year in range(1912,2024):
    fires_this_year = pd.read_sql_query(f"SELECT COUNT(*) AS condition_count FROM fires WHERE YEAR_ = {year};", conn)
    fires_per_year.append(fires_this_year.iloc[0, 0].item())
# plot fires year to year
fires_per_year = pd.DataFrame(fires_per_year)
fires_per_year.columns = ['Fires']
fires_per_year.index = fires_per_year.index + 1912
fires_per_year.plot(title='Fires over time', xlabel='Year', ylabel='Fires')
plt.show()

# Average Fire Size Yearly
avg_size_per_year = []
for year in range(1912,2024):
    avg_this_year = pd.read_sql_query(f"SELECT YEAR_, AVG(GIS_ACRES) AS average_value FROM fires WHERE YEAR_ = {year};", conn)
    avg_size_per_year.append(avg_this_year)
# plot sizes year to year
avg_size_per_year = pd.DataFrame(np.reshape(avg_size_per_year,(112,2)))
avg_size_per_year.columns = ['Year', 'Average Size (ACRES)']
avg_size_per_year.plot(title='Fire Sizes over time', xlabel='Year', ylabel='Avg Fire Size')
plt.show()

avg_len_per_year = []
for year in range(1912,2024):
    avg_this_year = pd.read_sql_query(f"SELECT YEAR_, AVG(LENGTH) AS average_value FROM fires WHERE YEAR_ = {year};", conn)
    avg_len_per_year.append(avg_this_year)
# plot lengths year to year
avg_len_per_year = pd.DataFrame(np.reshape(avg_len_per_year,(112,2)))
avg_len_per_year.columns = ['Year', 'Average Duration (Days)']
plt.plot(avg_len_per_year['Year'], avg_len_per_year['Average Duration (Days)'])
plt.xlabel('Year')
plt.ylabel('Average Duration (Days)')
plt.title('Average Fire Duration Over Time')
plt.show()

max_size_per_year = []
for year in range(1912,2024):
    max_this_year = pd.read_sql_query(f"SELECT YEAR_, MAX(GIS_ACRES) AS maximum_value FROM fires WHERE YEAR_ = {year};", conn)
    max_size_per_year.append(max_this_year)
# plot sizes year to year

max_len_per_year = []
for year in range(1912,2024):
    max_this_year = pd.read_sql_query(f"SELECT YEAR_, MAX(LENGTH) AS maximum_value FROM fires WHERE YEAR_ = {year};", conn)
    max_len_per_year.append(max_this_year)
# plot lengths year to year

num_fires_by_cause = []
for cause in range(1,20):
    fires_this_cause = pd.read_sql_query(f"SELECT COUNT(*) AS condition_count FROM fires WHERE CAUSE = {cause};", conn)
    num_fires_by_cause.append(fires_this_cause.iloc[0, 0].item())
# plot fires by cause

length_vs_acres = pd.read_sql_query("SELECT LENGTH, GIS_ACRES FROM fires ORDER BY LENGTH ASC", conn)
# plot length vs acres

# Monthly
monthly_dist = []
for month in range(1,13):
    if month < 10:
        monthly_fires = pd.read_sql_query(f"SELECT COUNT(*) FROM fires WHERE SUBSTR(ALARM_DATE, 6, 2) = '0{month}';", conn)
    else:
        monthly_fires = pd.read_sql_query(f"SELECT COUNT(*) FROM fires WHERE SUBSTR(ALARM_DATE, 6, 2) = '{month}';", conn)
    monthly_dist.append(monthly_fires)

monthly_acres_dist = []
for month in range(1,13):
    if month < 10:
        monthly_acres = pd.read_sql_query(f"SELECT AVG(GIS_ACRES) AS average_value FROM fires WHERE SUBSTR(ALARM_DATE, 6, 2) = '0{month}';", conn)
    else:
        monthly_acres = pd.read_sql_query(f"SELECT AVG(GIS_ACRES) AS average_value FROM fires WHERE SUBSTR(ALARM_DATE, 6, 2) = '{month}';", conn)
    monthly_acres_dist.append(monthly_acres)

monthly_len_dist = []
for month in range(1,13):
    if month < 10:
        monthly_len = pd.read_sql_query(f"SELECT AVG(LENGTH) AS average_value FROM fires WHERE SUBSTR(ALARM_DATE, 6, 2) = '0{month}';", conn)
    else:
        monthly_len = pd.read_sql_query(f"SELECT AVG(LENGTH) AS average_value FROM fires WHERE SUBSTR(ALARM_DATE, 6, 2) = '{month}';", conn)
    monthly_len_dist.append(monthly_len)
    
total_rows = pd.read_sql_query("SELECT COUNT(*) FROM fires;", conn)
print('The dataset contains ' + str(total_rows.iloc[0, 0].item()) + ' fires without missing data') 





#Make simple front end to display data
#analyze data: year to year trends, common causes, acres vs time lengths, what changes year to year, etc

conn.commit()
conn.close()

#OK so trying to predict fires in CA: 
# We first need historical fire data - make it binary 0/1
# Then indicators lined up by date
# Temperature, Humidity, Wind Speed, Vegitation/Terrain?, Drought Conditions?
# More creative ideas: Population denisty, median income, 
print("Hi")