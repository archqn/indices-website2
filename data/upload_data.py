import os
import pickle
import pandas as pd
from sqlalchemy import create_engine



def omni_load_raw_pickled(varname, res, level):
    """Function: read pickled raw omni data"""

    file_data = os.path.join(r'C:\Users\uclam\projects\indices', 'data', 'omni2024', 'omni_'+res+'_'+varname+'_'+level+'.pkl')

    if os.path.isfile(file_data):
        data = pickle.load(open(file_data, 'rb'))
        return data
    else:
        raise FileNotFoundError(f"File {file_data} not found")
    

def convert_to_dataframe(data):
    """
    Function to convert the 'tomni' and 'domni' data into a pandas DataFrame
    """
    # Convert the 'tomni' (Unix timestamps) to datetime format
    timestamps = pd.to_datetime(data['tomni'], unit='s')
    
    # Create a DataFrame with 'timestamp' and 'BZ_GSM' (or similar values)
    df = pd.DataFrame({
        'timestamp': timestamps,
        'BZ_GSM': data['domni']  # or whichever parameter you're dealing with
    })

    df = df.dropna()
    return df

    
def upload_data_to_mysql(df, chunksize=1000):
    engine = create_engine('mysql+mysqlconnector://indices_website_user:Indices-MySQL@localhost:3306/indices_dash_db')
    df.to_sql('solar_wind', con=engine, if_exists='append', index=False, chunksize=chunksize)

if __name__ == "__main__":
    data = omni_load_raw_pickled('BZ_GSM', '5min', 'hro')
    df = convert_to_dataframe(data)
    upload_data_to_mysql(df)