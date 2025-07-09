from flask import Flask, request, jsonify
import gspread
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import paramiko
import base64
import os
import io

app = Flask(__name__)

SPREADSHEET_ID = "1_t8pThdb0kFyIyRfNtC-VLsGa6HopgGQoEOqKyisjME"
SHEET_ACCOUNTS = "アカウント管理"
FOLDER_ID = "1ykCNsVXqi619OzXwLTqVJIm1WbqWcMgn"
SFTP_HOST = "upload.rakuten.ne.jp"
SFTP_PORT = 22
SFTP_UPLOAD_PATH = "/ritem/batch"

GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")


try:
    print("🧪 base64デコード開始...")
    decoded_json = base64.b64decode(GOOGLE_CREDENTIALS_JSON).decode("utf-8")
    creds_dict = json.loads(decoded_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    print("✅ Google認証成功")
except Exception as e:
    raise RuntimeError(f"❌ 認証エラー: {e}")

gspread_client = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

@app.route("/")
def index():
    return "✅ Flask is running!"

@app.route("/upload_sftp", methods=["POST"])
def upload_sftp():
    try:
        data = request.get_json()
        account = data.get("account")
        filename = data.get("filename")
        print(f"📦 リクエスト受信: {account}, {filename}")

        # スプレッドシートからアカウント情報取得
        sheet = gspread_client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_ACCOUNTS)
        rows = sheet.get_all_values()
        headers = rows[0]
        found = None
        for row in rows[1:]:
            row_dict = dict(zip(headers, row))
            if row_dict.get("アカウント名") == account:
                found = row_dict
                break

        if not found:
            return jsonify({"status": "error", "message": "FTPアカウント情報が見つかりません"}), 400

        ftp_user = found.get("FTP用ユーザー名")
        ftp_pass = found.get("FTP用パスワード")

        # Google Drive からファイルID取得
        response = drive_service.files().list(q=f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false", fields="files(id)").execute()
        files = response.get("files", [])
        if not files:
            return jsonify({"status": "error", "message": "Google Drive にファイルが見つかりません"}), 404

        file_id = files[0]["id"]
        request_drive = drive_service.files().get_media(fileId=file_id)

        tmp_path = f"./tmp_{filename}"
        with open(tmp_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request_drive)
            done = False
            while not done:
                status, done = downloader.next_chunk()

        # SFTPアップロード
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=ftp_user, password=ftp_pass)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(tmp_path, f"{SFTP_UPLOAD_PATH}/{filename}")
        sftp.close()
        transport.close()

        print(f"✅ アップロード成功: {filename}")
        return jsonify({"status": "success", "message": f"{filename} をアップロードしました"})

    except Exception as e:
        print(f"❌ エラー: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

def test_fetch_accounts():
    try:
        print(f"📄 SPREADSHEET_ID: {SPREADSHEET_ID}")
        print(f"📄 SHEET_ACCOUNTS: {SHEET_ACCOUNTS}")
        sheet = gspread_client.open_by_key(SPREADSHEET_ID)
        print("✅ スプレッドシートオープン成功")

        print("🧾 シート一覧:")
        for ws in sheet.worksheets():
            print(f"- {ws.title}")

        worksheet = sheet.worksheet(SHEET_ACCOUNTS)
        print("✅ シート名取得成功")

        data = worksheet.get_all_values()
        print("✅ データ取得成功：")
        for row in data:
            print(row)
    except Exception as e:
        print(f"❌ スプレッドシート読み取り失敗: {e}")
        print(f"📛 詳細: {repr(e)}")

if __name__ == "__main__":
    import sys
    if "test" in sys.argv:
        test_fetch_accounts()
    else:
        app.run(debug=True)
