from flask import Flask, request, jsonify
import paramiko
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv
import os
import json
import base64
import platform
import io

# 📌 Flask アプリの初期化
app = Flask(__name__)

# 📌 環境変数をロード
load_dotenv()

# 📌 Google 認証情報を Base64 からデコード
creds_json_base64 = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not creds_json_base64:
    raise ValueError("❌ 環境変数 GOOGLE_CREDENTIALS_JSON が設定されていません")

try:
    creds_json_str = base64.b64decode(creds_json_base64).decode("utf-8")
    creds_dict = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_dict)
except Exception as e:
    raise ValueError(f"❌ GOOGLE_CREDENTIALS_JSON のデコードに失敗しました: {e}")

# 📌 Google Sheets & Google Drive 設定
SPREADSHEET_ID = "1_t8pThdb0kFyIyRfNtC-VLsGa6HopgGQoEOqKyisjME"
SHEET_ACCOUNTS = "アカウント管理"
SHEET_RESERVATIONS = "アップロード予約"
FOLDER_ID = "1ykCNsVXqi619OzXwLTqVJIm1WbqWcMgn"

# 📌 Gspread & Google Drive API の初期化
client = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

# 📌 SFTP 設定
SFTP_HOST = "upload.rakuten.ne.jp"
SFTP_PORT = 22
SFTP_UPLOAD_PATH = "/ritem/batch"

# 📌 アカウント情報を取得
def get_sftp_credentials(account_name):
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_ACCOUNTS)
        data = sheet.get_all_values()

        headers = data[0]
        account_data = [dict(zip(headers, row)) for row in data[1:]]

        account_mapping = {
            "アウトスタイル": "outstyle-r",
            "LIMITEST": "limitest"
        }

        for row in account_data:
            if account_mapping.get(row["アカウント名"].strip(), row["アカウント名"].strip()) == account_name.strip():
                return row["FTP用ユーザー名"], row["FTP用パスワード"]

        return None, None
    except Exception as e:
        print(f"❌ アカウント情報取得エラー: {e}")
        return None, None

# 📌 予約データを取得
@app.route("/get_reservations", methods=["GET"])
def get_reservations():
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()

        headers = data[0]
        records = [
            {key: value for key, value in zip(headers, row)}
            for row in data[1:] if any(row)
        ]

        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 📌 Google Drive 内のファイル ID を取得
def get_google_drive_file_path(filename):
    try:
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false",
            fields="files(id, name)"
        ).execute()
        
        files = results.get("files", [])
        return files[0]["id"] if files else None
    except Exception as e:
        print(f"❌ Google Drive ファイル検索エラー: {e}")
        return None

# 📌 SFTPへアップロード
@app.route("/upload_sftp", methods=["POST"])
def upload_sftp():
    try:
        data = request.get_json()
        account = data["account"]
        filename = data["filename"]

        username, password = get_sftp_credentials(account)
        if not username or not password:
            update_sheet_status(filename, "エラー", "FTPアカウント情報が見つかりません")
            return jsonify({"status": "error", "message": "FTPアカウント情報が見つかりません"}), 400

        file_id = get_google_drive_file_path(filename)
        if not file_id:
            update_sheet_status(filename, "エラー", "Google Drive にファイルが見つかりません")
            return jsonify({"status": "error", "message": "Google Drive にファイルが見つかりません"}), 404

        tmp_dir = "/tmp" if platform.system() != "Windows" else "./tmp"
        os.makedirs(tmp_dir, exist_ok=True)

        file_path = os.path.join(tmp_dir, filename)
        request_drive = drive_service.files().get_media(fileId=file_id)

        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request_drive)
            done = False
            while not done:
                status, done = downloader.next_chunk()

        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_file_path = f"{SFTP_UPLOAD_PATH}/{filename}"
        sftp.put(file_path, remote_file_path)
        sftp.close()
        transport.close()

        update_sheet_status(filename, "アップロード完了")
        return jsonify({"status": "success", "message": f"{filename} のアップロード完了"}), 200

    except Exception as e:
        update_sheet_status(filename, "エラー", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

# 📌 スプレッドシートのステータスを更新
def update_sheet_status(filename, status, error_message=""):
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()

        headers = data[0]
        filename_col = headers.index("ファイル名")
        status_col = headers.index("ステータス")

        if "エラーメッセージ" not in headers:
            error_col = len(headers)
            sheet.update_cell(1, error_col + 1, "エラーメッセージ")
        else:
            error_col = headers.index("エラーメッセージ")

        for i, row in enumerate(data[1:], start=2):
            if row[filename_col] == filename:
                sheet.update_cell(i, status_col + 1, status)
                sheet.update_cell(i, error_col + 1, error_message)
                return
    except Exception as e:
        print(f"❌ スプレッドシート更新エラー: {str(e)}")

# 📌 API ステータス確認
@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "running"}), 200

# 📌 ルートページ
@app.route("/")
def home():
    return "Flask API is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
