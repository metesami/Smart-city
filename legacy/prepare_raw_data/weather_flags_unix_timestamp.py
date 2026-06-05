import pandas as pd
import numpy as np

weather_flagged = pd.read_csv('/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/weather_10min.csv',parse_dates=['datetime'],low_memory=False)


bins_temperature = [-12.7, 0.0, 10.0, 20.0, 30.0, float("inf")]
labels_temperature = [
    "TempBin_minus12p7_0p0_DEGC",
    "TempBin_0p0_10p0_DEGC",
    "TempBin_10p0_20p0_DEGC",
    "TempBin_20p0_30p0_DEGC",
    "TempBin_30p0_plus_DEGC",
]

bins_precipitation = [0.0, 0.1, 1.0, 5.0, 10.0, float("inf")]
labels_precipitation = [
    "RainBin_0p0_0p1_MM",
    "RainBin_0p1_1p0_MM",
    "RainBin_1p0_5p0_MM",
    "RainBin_5p0_10p0_MM",
    "RainBin_10p0_plus_MM"
]

bins_humidity = [0, 50.0, 70.0, 85.0, 95.0, 100.0]
labels_humidity = [
    "HumidityBin_0p0_50p0_PCT",
    "HumidityBin_50p0_70p0_PCT",
    "HumidityBin_70p0_85p0_PCT",
    "HumidityBin_85p0_95p0_PCT",
    "HumidityBin_95p0_100p0_PCT"
]

bins_pressure = [900, 980.0, 995.0, 1005.0, 1015.0, float("inf")]
labels_pressure = [
    "PressureBin_900p0_980p0_HPA",
    "PressureBin_980p0_995p0_HPA",
    "PressureBin_995p0_1005p0_HPA",
    "PressureBin_1005p0_1015p0_HPA",
    "PressureBin_1015p0_plus_HPA"
]

bins_wind_speed = [0, 1.0, 3.0, 5.0, 10.0, float("inf")]
labels_wind_speed = [
    "WindBin_0p0_1p0_MS",
    "WindBin_1p0_3p0_MS",
    "WindBin_3p0_5p0_MS",
    "WindBin_5p0_10p0_MS",
    "WindBin_10p0_plus_MS"
]

bins_wind_direction = [0, 45, 90, 135, 180, 225, 270, 315, 360]
labels_wind_direction = [
    "WindDir_N",
    "WindDir_NE",
    "WindDir_E",
    "WindDir_SE",
    "WindDir_S",
    "WindDir_SW",
    "WindDir_W",
    "WindDir_NW"
]



# pre process on wind direction to avoid wind direction (0 and 360 both are North)

#Rotate degree for binning
deg = weather_flagged['wind_direction'] % 360
deg_rot = (deg + 22.5) % 360


weather_flagged['temperature_category'] = pd.cut(weather_flagged['temperature'], bins=bins_temperature, labels=labels_temperature,include_lowest = True)

weather_flagged['precipitation_category'] = pd.cut(weather_flagged['precipitation'], bins=bins_precipitation, labels=labels_precipitation,include_lowest = True)

weather_flagged['humidity_category'] = pd.cut(weather_flagged['humidity'], bins=bins_humidity, labels=labels_humidity,include_lowest = True)

weather_flagged['pressure_category'] = pd.cut(weather_flagged['pressure'], bins=bins_pressure, labels=labels_pressure,include_lowest = True)

weather_flagged['wind_speed_category'] = pd.cut(weather_flagged['wind_speed'], bins=bins_wind_speed, labels=labels_wind_speed,include_lowest = True)

weather_flagged['wind_direction_category'] = pd.cut(deg_rot, bins=bins_wind_direction, labels=labels_wind_direction,include_lowest = True)


#Unix timestamp
weather_flagged['timestamp_seconds'] = (
    weather_flagged['datetime']
    .astype('int64')
    // 10**9                 # convert to second
)


# Save final CSV
out_file = 'weather_10min.csv'
weather_flagged.to_csv(out_file, index=False)
