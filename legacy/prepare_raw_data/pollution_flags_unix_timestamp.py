import pandas as pd
import numpy as np


pollution_flagged = pd.read_csv('/content/drive/MyDrive/Test ontology_A142/10 min Interval Datasets/pollution_10min.csv',parse_dates=['datetime'])


pollution_flagged.info()

bins_pm10 = [0,6.60,9.10,11.85,15,16.95,19.25,22.65,30,50,75,100,float('inf')]
labels_pm10 = [
    "PM10Bin_0p0_6p6_UGM3",
    "PM10Bin_6p6_9p1_UGM3",
    "PM10Bin_9p1_11p85_UGM3",
    "PM10Bin_11p85_15p0_UGM3",
    "PM10Bin_15p0_16p95_UGM3",
    "PM10Bin_16p95_19p25_UGM3",
    "PM10Bin_19p25_22p65_UGM3",
    "PM10Bin_22p65_30p0_UGM3",
    "PM10Bin_30p0_50p0_UGM3",
    "PM10Bin_50p0_75p0_UGM3",
    "PM10Bin_75p0_100p0_UGM3",
    "PM10Bin_100p0_plus_UGM3",
]

bins_pm2_5 = [0,2.7,3.55,4.3,5,5.9,6.95,8.25,10,15,25,40,float("inf")]
labels_pm2_5 = [
    "PM25Bin_0p0_2p7_UGM3",
    "PM25Bin_2p7_3p55_UGM3",
    "PM25Bin_3p55_4p3_UGM3",
    "PM25Bin_4p3_5p0_UGM3",
    "PM25Bin_5p0_5p9_UGM3",
    "PM25Bin_5p9_6p95_UGM3",
    "PM25Bin_6p95_8p25_UGM3",
    "PM25Bin_8p25_10p0_UGM3",
    "PM25Bin_10p0_15p0_UGM3",
    "PM25Bin_15p0_25p0_UGM3",
    "PM25Bin_25p0_40p0_UGM3",
    "PM25Bin_40p0_plus_UGM3",
]

bins_no2 = [0,7.6,10,12.2,14.5,16.95,20,23.95,30,40,60,float("inf")]
labels_no2 =[
    "NO2Bin_0p0_7p6_UGM3",
    "NO2Bin_7p6_10p0_UGM3",
    "NO2Bin_10p0_12p2_UGM3",
    "NO2Bin_12p2_14p5_UGM3",
    "NO2Bin_14p5_16p95_UGM3",
    "NO2Bin_16p95_20p0_UGM3",
    "NO2Bin_20p0_23p95_UGM3",
    "NO2Bin_23p95_30p0_UGM3",
    "NO2Bin_30p0_40p0_UGM3",
    "NO2Bin_40p0_60p0_UGM3",
    "NO2Bin_60p0_plus_UGM3",
]


pollution_flagged['NO2_category'] = pd.cut(pollution_flagged['NO2'], bins=bins_no2, labels=labels_no2,include_lowest = True)

pollution_flagged['PM10_category'] = pd.cut(pollution_flagged['PM10'], bins=bins_pm10, labels=labels_pm10,include_lowest = True)

pollution_flagged['PM2.5_category'] = pd.cut(pollution_flagged['PM2.5'], bins=bins_pm2_5, labels=labels_pm2_5,include_lowest = True)


#Unix timestamp
pollution_flagged['timestamp_seconds'] = (
    pollution_flagged['datetime']
    .astype('int64')
    // 10**9                 # convert to second
)
pollution_flagged.head()



# Save final CSV
out_file = 'pollution_10min.csv'
pollution_flagged.to_csv(out_file, index=False)
