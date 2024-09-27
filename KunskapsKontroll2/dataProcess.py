import logging
import pandas as pd
import sqlite3

        
def fetch_data():
    logger = logging.getLogger(__name__)
    logger.info('Collecting data from archive...')
    
    # Fetching files
    df = pd.DataFrame()
    df1 = pd.read_csv(r"./archive/drivers.csv")
    df2 = pd.read_csv(r"./archive/races.csv")
    df3 = pd.read_csv(r"./archive/lap_times.csv")
        
    # Merging the data into one dataframe
    logger.info('Merging csv files to be processed...')
    try:
        df_m = df3.merge(df2, on='raceId', how='inner')
        df_m = df3.merge(df2, on='raceId', how='inner')
        df = df_m.merge(df1, on='driverId', how='inner')
        df.set_index('raceId', inplace=True)
        logger.info('Merging complete')
    except:
        logger.error('an error occured')
        raise Exception ('An error occured while merging the dataframes.')
    
    # Cleaning the data
    logger.info('Cleaning the unwanted columns...')
    try:
        df = df.drop(['position', 'round','sprint_date',
                        'sprint_time', 'dob', 'driverRef',
                        'url_y', 'url_x', 'time_y',
                        "fp1_date", 'fp1_time', "fp2_date",
                        'fp2_time', "fp3_date", 'fp3_time',
                        "quali_date", 'quali_time'],
                        axis=1)
        logger.info('Cleaning complete')
    except:
        logger.error('an error occured')
                
    # Saving and exporting the data to CSV and sqlite
    logger.info('Saving and exporting dataframe to csv and splite...')
    try:
        df.to_csv("trackPerformanceYears.csv")
        con = sqlite3.connect("TrackPerformanceYearsF1.db")
        print('Dataframe saved as "trackPerformanceYears.csv", and exported as sqlite database "TrackPerformanceYearsF1.db"')
        df.to_sql("TrackPerformanceYearsF1", con, if_exists="replace")
        logger.info('Saving successful')
    except:
        logger.error('an error occured, saving failed.')
        
        
    
    # Printing out a sample for the user
    logger.info('Process complete')
    print(df)

if __name__ == ('__main__'):
    fetch_data
