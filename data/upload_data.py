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
    

def convert_to_dataframe(data, varname):
    # Convert the 'tomni' (Unix timestamps) to datetime format
    timestamps = pd.to_datetime(data['tomni'], unit='s')
    
    # Create a DataFrame with 'timestamp' and 'BZ_GSM' (or similar values)
    df = pd.DataFrame({
        'timestamp': timestamps,
        varname : data['domni']
    })

    df = df.dropna()
    return df

    
def upload_data_to_mysql(df, chunksize=1000):
    engine = create_engine('mysql+mysqlconnector://indices_website_user:Indices-MySQL@localhost:3306/indices_dash_db')
    df.to_sql('solar_wind_new', con=engine, if_exists='replace', index=False, chunksize=chunksize)

if __name__ == "__main__":
    data_BZ_GSM = omni_load_raw_pickled('BZ_GSM', '5min', 'hro')
    df = convert_to_dataframe(data_BZ_GSM, 'BZ_GSM')

    data_flow_speed = omni_load_raw_pickled('flow_speed', '5min', 'hro')
    data_T = omni_load_raw_pickled('T', '5min', 'hro')
    data_proton_density = omni_load_raw_pickled('proton_density', '5min', 'hro')

    df_flow_speed = convert_to_dataframe(data_flow_speed, 'flow_speed')
    df_T = convert_to_dataframe(data_T, 'T')
    df_proton_density = convert_to_dataframe(data_proton_density, 'proton_density')

    # Merge the additional data with the original DataFrame on 'timestamp'
    df = df.merge(df_flow_speed, on='timestamp', how='left')
    df = df.merge(df_T, on='timestamp', how='left')
    df = df.merge(df_proton_density, on='timestamp', how='left')

    print(df.head())  # To check the final merged DataFrame

    upload_data_to_mysql(df)