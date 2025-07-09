import requests

payload = {
    "account": "LIMITEST",  # ← 英語表記でも日本語アカウント名と一致するように
    "filename": "normal-item_20250402_132633_SMATAN_LPnameP10-0404.csv"
}

response = requests.post("http://127.0.0.1:5000/upload_sftp", json=payload)

print("STATUS:", response.status_code)
print("BODY:", response.text)
