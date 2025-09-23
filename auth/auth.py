def auth_func():
    import requests, configparser, json
    config = configparser.ConfigParser()
    config.read('auth/config-panw.ini')   
    keys = ['cspm_api_url', 'cwp_api_url', 'username', 'password']
    login_url, cwp_url, username, password = [config.get('prismacloud', key) for key in keys]
    url = f'{login_url}/login'
    payload = json.dumps({
        "username": username,
        "password": password
    })
    headers = {"Content-Type": "application/json"}
    try:               
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        response.raise_for_status()
        token = response.json().get("token")
        print("Retrived Token")
        return token, login_url, cwp_url
    except requests.exceptions.HTTPError as err:
        print(f"HTTP Error during authentication: {err}")
        print(f"   Response Body: {err.response.text}")
    except Exception as e:
        print(f"An error occurred during authentication: {e}")
    return None
