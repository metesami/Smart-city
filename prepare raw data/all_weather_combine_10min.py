import numpy as np
import pandas as pd


# Load 10-minute air temperature and humidity dataset
df_temperature = pd.read_csv('/content/drive/MyDrive/Test ontology_A142/Weather/tempreture-2022 to 2024.txt',sep=';', decimal=',', encoding='utf-8')

# Convert timestamp from string to datetime
df_temperature['datetime'] = pd.to_datetime(df_temperature['MESS_DATUM'], format='%Y%m%d%H%M')

# Set datetime index
df_temperature = df_temperature.set_index('datetime')

# Keep only relevant column
df_temperature = df_temperature[['TT_10','RF_10']]
df_temperature.rename(columns={'TT_10': 'temperature','RF_10': 'humidity'}, inplace=True)


# Load 10-minute precipitation dataset('RWS_10(mm) Sum of the precipitation height of the previous 10 minutes')
df_precipitation = pd.read_csv('/content/drive/MyDrive/Test ontology_A142/Weather/precipitation-2022 to 2024.txt',sep=';', decimal=',', encoding='utf-8')

# Convert timestamp
df_precipitation['datetime'] = pd.to_datetime(df_precipitation['MESS_DATUM'], format='%Y%m%d%H%M')

# Set datetime index
df_precipitation = df_precipitation.set_index('datetime')


# Keep only relevant column
df_precipitation = df_precipitation[['RWS_10']]
df_precipitation.rename(columns={'RWS_10': 'precipitation'}, inplace=True)


# Load 10-minute pressure dataset
df_pressure = pd.read_csv('/content/drive/MyDrive/Test ontology_A142/Weather/pressure-2022 to 2024.txt',sep=';', decimal=',', encoding='utf-8')

# Convert timestamp
df_pressure['datetime'] = pd.to_datetime(df_pressure['MESS_DATUM'], format='%Y%m%d%H')

# Set datetime index
df_pressure = df_pressure.set_index('datetime')

# Keep only relevant column
df_pressure = df_pressure[['  P0']]
df_pressure.rename(columns={'  P0': 'pressure'}, inplace=True)

#add 1 hour to time index
last_time = df_pressure.index.max()
df_pressure.loc[last_time + pd.Timedelta(hours=1)] = df_pressure.iloc[-1].values

#change literal to 10 minutes
df_pressure_10min = df_pressure.resample('10T').asfreq()
df_pressure = df_pressure_10min.ffill(limit=5)

# Load 10-minute wind dataset
df_wind = pd.read_csv('/content/drive/MyDrive/Test ontology_A142/Weather/wind-2022 to 2024.txt',sep=';', decimal=',', encoding='utf-8')
# Convert timestamp
df_wind['datetime'] = pd.to_datetime(df_wind['MESS_DATUM'], format='%Y%m%d%H%M')

# Set datetime index
df_wind = df_wind.set_index('datetime')


# Keep only relevant column
df_wind = df_wind[['FF_10','DD_10']]
# Rename for clarity (optional)
df_wind.rename(columns={'FF_10': 'wind_speed', 'DD_10': 'wind_direction'}, inplace=True)

#convert to float
df_wind['wind_speed'] = pd.to_numeric(df_wind['wind_speed'], errors='coerce')

#handle missing values
df_wind[['wind_speed', 'wind_direction']] = df_wind[['wind_speed', 'wind_direction']].replace(-999, np.nan)
df_wind['wind_speed'] = df_wind['wind_speed'].ffill()
df_wind['wind_direction'] = df_wind['wind_direction'].ffill()


# Convert degree to Radian
wind_dir_rad = np.radians(df_wind['wind_direction'])
# Calculate u and v components
df_wind['wind_u'] = -df_wind['wind_speed'] * np.sin(wind_dir_rad).round(2)
df_wind['wind_v'] = -df_wind['wind_speed'] * np.cos(wind_dir_rad).round(2)

#final columns
df_wind = df_wind[['wind_speed', 'wind_u', 'wind_v','wind_direction']]

df_wind.head()

#List of dataFrames
dfs = [df_temperature, df_precipitation, df_pressure, df_wind]
df_merged = pd.concat(dfs, axis=1, join='outer')
df_merged = df_merged.sort_index()
df_merged.columns


#convert all columns to float
for i in df_merged.columns:
  df_merged[i] = pd.to_numeric(df_merged[i], errors='coerce')

#remove last row with NaN
df_merged = df_merged.iloc[:-1]

# Prepare final result and format datetime in ISO UTC (+00:00)
result = df_merged.reset_index()
result['datetime'] = result['datetime'].dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')


df_merged.to_csv('weather_10min.csv', index=True, sep=',', decimal='.', encoding='utf-8',date_format='%Y-%m-%dT%H:%M:%S+00:00')
