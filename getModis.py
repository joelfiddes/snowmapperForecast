import requests
import os
from datetime import datetime

USERNAME = 'jfiddes'
PASSWORD = 'sT0kkang!'
token = "eyJ0eXAiOiJKV1QiLCJvcmlnaW4iOiJFYXJ0aGRhdGEgTG9naW4iLCJzaWciOiJlZGxqd3RwdWJrZXlfb3BzIiwiYWxnIjoiUlMyNTYifQ.eyJ0eXBlIjoiVXNlciIsInVpZCI6ImpmaWRkZXMiLCJleHAiOjE3MzQzMjgzMzgsImlhdCI6MTcyOTE0NDMzOCwiaXNzIjoiaHR0cHM6Ly91cnMuZWFydGhkYXRhLm5hc2EuZ292In0.hFztpj6g2TvmkT_t-e8OXyYzorezmEiLnVmcEGYYipMPO2UXadhGbtlybQnERmpbKEWr1ZkIgf_6YN7YH2i255nQwGn4GBr1ZTbgvKRndP4j_AAloKaUj76XOwJXWkh1WJQ35V1lfNF0u1xHTctci5-xzYBgf3cW0ACW-7iOO7RF5JJgeMHvuoFQAd1LYnoMjGasWZi9-_97FWuk1ao6KlO7axR74j5XN__eY_vlCkdfrXIYAsqReSZDzS5xK6bz04NO37m6ayGKcC-Uj9uAzyr5ErgCMSVFy-0AJTcqS8S9tz23v4angjk6fI44LBpK2PFsL5Ijg5W4QOadJppE-w"

def download_modis_data(product, start_date, end_date, bounding_box, save_dir):
    base_url = "https://n5eil02u.ecs.nsidc.org/egi/request"

    
    params = {
        'short_name': product,
        'bounding_box': ','.join(map(str, bounding_box)),
        'temporal': f"{start_date}T00:00:00Z,{end_date}T23:59:59Z",
        'format': 'GeoTIFF',
        'page_size': 100,
        'token': token,
    }

    # Make the request
    response = requests.get(base_url, params=params, auth=(USERNAME, PASSWORD))

    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.text}")
        return

    os.makedirs(save_dir, exist_ok=True)
    today = datetime.now().strftime('%Y%m%d')
    with open(os.path.join(save_dir, f"{product}_{today}.zip"), 'wb') as f:
        f.write(response.content)
    print(f"Data saved to {save_dir}")

# Example usage
if __name__ == "__main__":
    product = 'MOD10A1'  # MODIS/Terra Snow Cover Daily L3 Global 500m SIN Grid
    start_date = '2023-09-01'
    end_date = '2024-09-01'
    bounding_box = [69, 38, 79, 45]  # Your region of interest
    save_dir = '/home/joel/sim/modis'

    download_modis_data(product, start_date, end_date, bounding_box, save_dir)
