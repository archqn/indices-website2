import os
import pickle
import pandas as pd
import numpy as np
from sqlalchemy import create_engine



def omni_load_raw_pickled(varname, res, level):
    """Function: read pickled raw omni data"""

    file_data = os.path.join(r'C:\Users\uclam\projects\indices', 'data', 'omni2024', 'omni_'+res+'_'+varname+'_'+level+'.pkl')

    if os.path.isfile(file_data):
        data = pickle.load(open(file_data, 'rb'))
        return data
    else:
        raise FileNotFoundError(f"File {file_data} not found")
    

def omni_load_coupling_function(varname, res, level):
    """
    Load the OMNI data from pre-generated pickle file, and then calculate the coupling function.
    """
    cpl_function = ['etl']
    varname = varname.lower()

    if varname not in cpl_function:
        raise ValueError(f"The variable name '{varname}' is not valid. Valid options are: {', '.join(cpl_function)}")

    elif varname in {'eps', 'eps2', 'eps3', 'ekl', 'ekl0.5', 'eklv', 'ewav', 'ewav2', 'ewav0.5', 'esr', 'etl', 'ewv', 'dphidt', 'opn'}:
        bx = omni_load_raw_pickled('BX_GSE', res, level)
        by = omni_load_raw_pickled('BY_GSM', res, level)
        bz = omni_load_raw_pickled('BZ_GSM', res, level)
        btot = np.sqrt(bx['domni']**2.0+by['domni']**2.0+bz['domni']**2.0)
        vv = omni_load_raw_pickled('flow_speed', res, level)
        # press = omni_load_raw_pickled('Pressure', res, level)
        # den = omni_load_raw_pickled('proton_density', res, level)
        btr = np.sqrt(by['domni']**2.0+bz['domni']**2.0)
        theta = np.arctan2(by['domni'], bz['domni'])  # Clock angle between BY and BZ
        sin_theta_2 = np.abs(np.sin(theta/2.0))

        # Ensure the time arrays are the same
        assert np.array_equal(bx['tomni'], vv['tomni'])
        assert np.array_equal(bx['tomni'], by['tomni'])
        assert np.array_equal(bx['tomni'], bz['tomni'])
        # assert np.array_equal(bx['tomni'], press['tomni'])
        # assert np.array_equal(bx['tomni'], den['tomni'])


        if varname == 'etl':  # n^0.5*v^2*Btr*sin^6(theta/2)
            domni = np.sqrt(btot)*(vv['domni']**2.0)*btr*(sin_theta_2**6.0)
            return {'tomni': bx['tomni'], 'domni': domni}

        else:
            raise ValueError(f"The variable name '{varname}' is not valid. Valid options are: {', '.join(cpl_function)}")
    

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
    df.to_sql('solar_wind_3', con=engine, if_exists='replace', index=False, chunksize=chunksize)

if __name__ == "__main__":
    res = '5min'
    level = 'hro'
    data_BZ_GSM = omni_load_raw_pickled('BZ_GSM', res, level)
    df = convert_to_dataframe(data_BZ_GSM, 'BZ_GSM')

    data_BX_GSE = omni_load_raw_pickled('BX_GSE', res, level)
    data_BY_GSM = omni_load_raw_pickled('BY_GSM', res, level)

    data_btot = {}
    data_btot['domni'] = np.sqrt(data_BX_GSE['domni']**2.0 + data_BY_GSM['domni']**2.0 + data_BZ_GSM['domni']**2.0)
    data_btot['tomni'] = data_BX_GSE['tomni']

    data_flow_speed = omni_load_raw_pickled('flow_speed', res, level)
    data_T = omni_load_raw_pickled('T', res, level)
    data_proton_density = omni_load_raw_pickled('proton_density', res, level)
    data_etl = omni_load_coupling_function('etl', res, level)

    df_BX_GSE = convert_to_dataframe(data_BX_GSE, 'BX_GSE')
    df_BY_GSM = convert_to_dataframe(data_BY_GSM, 'BY_GSM')
    df_btot = convert_to_dataframe(data_btot, 'btot')
    df_flow_speed = convert_to_dataframe(data_flow_speed, 'flow_speed')
    df_T = convert_to_dataframe(data_T, 'T')
    df_proton_density = convert_to_dataframe(data_proton_density, 'proton_density')
    df_etl = convert_to_dataframe(data_etl, 'etl')

    # Merge dfs on 'timestamp'
    df = df.merge(df_BX_GSE, on='timestamp', how='left')
    df = df.merge(df_BY_GSM, on='timestamp', how='left')
    df = df.merge(df_btot, on='timestamp', how='left')
    df = df.merge(df_flow_speed, on='timestamp', how='left')
    df = df.merge(df_T, on='timestamp', how='left')
    df = df.merge(df_proton_density, on='timestamp', how='left')
    df = df.merge(df_etl, on='timestamp', how='left')

    print(df.head())  

    upload_data_to_mysql(df)