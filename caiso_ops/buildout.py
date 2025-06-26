import requests
import zipfile
import io
import pandas as pd
import os
from bs4 import BeautifulSoup


def _retrieve_excel_links_from_url(url: str) -> list:
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    excel_links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith('.xlsx'):
            excel_links.append(url + href)
    return excel_links


def _download_excel_from_url(url: str, output_dir: str, excel_file_name: str) -> None:
    response = requests.get(url)
    with open(output_dir + excel_file_name, "wb") as f:
        f.write(response.content)


def _retrieve_oasis_response(url:str) -> pd.DataFrame:
    response = requests.get(url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    csv_filename = [name for name in zip_file.namelist() if name.endswith('.csv')][0]
    df = pd.read_csv(zip_file.open(csv_filename))
    return df


def _get_master_list_data(date: str, raw_data_dir: str) -> pd.DataFrame:
    start_date = pd.to_datetime(date).strftime('%Y%m%d')
    end_date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime('%Y%m%d')
    master_list_url = f'https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_GEN_CAP_LST&version=4&startdatetime={start_date}T07:00-0000&enddatetime={end_date}T07:00-0000&resource_id=ALL&agge_type=ALL&resource_type=ALL'
    df = _retrieve_oasis_response(master_list_url)
    df = df.query('RESOURCE_AGG_TYPE == "N" and ENERGY_SOURCE == "LESR"')[['RESOURCE_ID', 'GEN_UNIT_NAME', 'NET_DEPENDABLE_CAPACITY', 'NAMEPLATE_CAPACITY', 'OWNER_OR_QF', 'ENERGY_SOURCE', 'ZONE', 'PTO_AREA', 'COD' , 'BAA_ID','UDC']].reset_index(drop=True)
    df.to_csv(os.path.join(raw_data_dir, 'master_list.csv'), index = False)
    return df


def _get_resource_node_data(date:str, raw_data_dir: str) -> pd.DataFrame:
    start_date = pd.to_datetime(date).strftime('%Y%m%d')
    end_date = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime('%Y%m%d')
    resource_node_url = f'https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=ATL_RESOURCE&version=1&startdatetime={start_date}T07:00-0000&enddatetime={end_date}T07:00-0000&resource_id=ALL&agge_type=ALL&resource_type=ALL'
    df = _retrieve_oasis_response(resource_node_url)
    df.to_csv(os.path.join(raw_data_dir, 'resource_node.csv'), index = False)
    return df


def _get_price_map_node_coordinates(raw_data_dir: str) -> pd.DataFrame:
    price_map_url = 'https://wwwmobile.caiso.com/Web.Service.Chart/api/v3/ChartService/PriceContourMap1'
    response = requests.get(price_map_url)
    data = response.json()['l'][2]['m']

    node_df = pd.DataFrame()
    for node in data:
        new_row = pd.DataFrame({
            'name': [node['n']],
            'lat': [node['c'][0]],
            'lon': [node['c'][1]], 
            'gen_or_load': [node['p']],
            'balancing_area': [node['a']]
        })
        node_df = pd.concat([node_df, new_row], ignore_index=True)
    node_df.to_csv(os.path.join(raw_data_dir, 'price_map_node_coordinates.csv'), index = False)
    return node_df


def _get_new_resource_interconnection_data(raw_data_dir: str) -> pd.DataFrame:
    excel_url = "https://www.caiso.com/documents/generator-interconnection-resource-id-report.xlsx"
    _download_excel_from_url(excel_url, raw_data_dir, "latest_interconnection_resource_id_report.xlsx")

    df = pd.read_excel(os.path.join(raw_data_dir, "latest_interconnection_resource_id_report.xlsx"), sheet_name=1, skiprows=1)
    df = df.query('BATT_MWS > 0 and STATUS != "COD"')[['PROJECT_NAME', 'QUEUE_NUMBER', 'RES_ID', 'HYBRID_YN', 'CO_LOCATED_YN', 'STATUS', 'NAMEPLATE_MW', 'SOLAR_MWS', 'WIND_MWS', 'BATT_MWS', 'ACTIVE_DATE', 'SYNC_DATE', 'COMX_DATE', 'MF_COD', 'IC_COD']].reset_index(drop=True)
    df.to_csv(os.path.join(raw_data_dir, 'new_batteries_being_implemented.csv'), index = False)
    return df


def _get_public_interconnection_queue_report_data(raw_data_dir: str) -> pd.DataFrame:
    excel_url = "https://www.caiso.com/library/public-queue-report"
    _download_excel_from_url(_retrieve_excel_links_from_url(excel_url)[0], raw_data_dir, "public_queue_report.xlsx")
    df = pd.read_excel(os.path.join(raw_data_dir, "public_queue_report.xlsx"), sheet_name=1, skiprows=1)
    return df
    

def create_buildout_charts(raw_data_dir: str, charts_dir: str, date: str):
    master_list_lesr = _get_master_list_data(date=date, raw_data_dir=raw_data_dir)
    resource_node_data = _get_resource_node_data(date=date, raw_data_dir=raw_data_dir)


    master_list_lesr['Node'] = master_list_lesr[['RESOURCE_ID']].merge(resource_node_data, on='RESOURCE_ID', how='left')['NODE_ID']
    if master_list_lesr['Node'].isna().sum() > 0:
        print("Could not find a node for these batteries in the master list:")
        print(master_list_lesr[master_list_lesr['Node'].isna()][['RESOURCE_ID', 'GEN_UNIT_NAME', 'NAMEPLATE_CAPACITY', 'Node']])

    print('Total Nameplate Capacity: ',master_list_lesr['NAMEPLATE_CAPACITY'].sum().round(1), ' MW')


    master_list_lesr['COD'] = pd.to_datetime(master_list_lesr['COD'])
    print('New sites in 2025: ', master_list_lesr.query('COD >= "2025-01-01"').reset_index(drop=True)['GEN_UNIT_NAME'].nunique(), ' sites below')
    new_sites = master_list_lesr.query('COD >= "2025-01-01"')[['RESOURCE_ID', 'GEN_UNIT_NAME', 'NAMEPLATE_CAPACITY', 'OWNER_OR_QF', 'ZONE', 'COD' , 'UDC', 'Node']].sort_values(by='COD', ascending=True).reset_index(drop=True)
    new_sites.to_csv(os.path.join(charts_dir, 'new_buildout_sites.csv'), index = False)
    print(new_sites)
    

    master_list_lesr.query('COD >= "2025-01-01"'
                           )[['GEN_UNIT_NAME', 'NAMEPLATE_CAPACITY', 'OWNER_OR_QF', 'ZONE', 'COD' , 'UDC']
                             ].sort_values(by='COD', ascending=True
                                           ).rename(columns={'GEN_UNIT_NAME': 'Site', 'NAMEPLATE_CAPACITY':'Rated Power (MW)', 'OWNER_OR_QF':'Owner', 'ZONE':'Price Zone', 'COD':'Commencement Date', 'UDC':'Utility Area'}
                                                    ).reset_index(drop=True).to_csv('battery_sites.csv', index = False)

    master_list_lesr['New sites'] = master_list_lesr['COD'] >= pd.to_datetime('2025-01-01')

    master_list_lesr = master_list_lesr.merge(_get_price_map_node_coordinates(raw_data_dir), left_on='Node', right_on='name', how='left')
    print(f"Coord. coverage from price map: {(1-master_list_lesr['lat'].isna().sum()/len(master_list_lesr))*100:.1f}%")

    import pathlib
    raw_path = pathlib.Path(raw_data_dir)
    eia_dir = raw_path.parent / "processed"

    # eia_coords = pd.read_csv('./projects/caiso/masterList_eia_mapping.csv').dropna(subset= 'RESOURCE_ID')
    eia_coords = pd.read_csv(eia_dir / 'masterList_eia_mapping.csv').dropna(subset= 'RESOURCE_ID')
    master_list_lesr = master_list_lesr.merge(eia_coords, how = 'left', left_on='RESOURCE_ID', right_on='RESOURCE_ID')
    print(f"Coord. coverage from EIA mapping: {(1-master_list_lesr['EIA Latitude'].isna().sum()/len(master_list_lesr))*100:.1f}%")

    master_list_lesr.drop(columns = ['name','lat', 'lon', 'gen_or_load', 'balancing_area', 'ENERGY_SOURCE', 'COD', 'BAA_ID']).to_csv(os.path.join(charts_dir, 'battery_sites.csv'), index = False)

