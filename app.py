from flask import Flask, request, jsonify
import paramiko
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import os
import platform
import io
import re

app = Flask(__name__)

SERVICE_ACCOUNT_FILE = "credentials.json"
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
print("✅ Google 認証情報を正常にロードしました")

SPREADSHEET_ID = "1_t8pThdb0kFyIyRfNtC-VLsGa6HopgGQoEOqKyisjME"
FOLDER_ID = "1ykCNsVXqi619OzXwLTqVJIm1WbqWcMgn"
SHEET_ACCOUNTS = "アカウント管理"
SHEET_RESERVATIONS = "アップロード予約"

SFTP_HOST = "upload.rakuten.ne.jp"
SFTP_PORT = 22
SFTP_UPLOAD_PATH = "/ritem/batch"

drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)
gspread_client = gspread.authorize(creds)

def normalize(text):
    if not isinstance(text, str):
        return ""
    return re.sub(r"[\u3000\u200b\s\r\n]", "", text.strip().lower())

def get_sftp_credentials(account_name):
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_ACCOUNTS}!A1:C"
        ).execute()

        values = result.get("values", [])
        if not values or len(values) < 2:
            print("❌ アカウント情報が取得できません")
            return None, None

        headers = values[0]
        rows = values[1:]

        idx_account = headers.index("アカウント名")
        idx_user = headers.index("FTP用ユーザー名")
        idx_pass = headers.index("FTP用パスワード")

        normalized_input = normalize(account_name)
        for row in rows:
            try:
                if normalize(row[idx_account]) == normalized_input:
                    return row[idx_user].strip(), row[idx_pass].strip()
            except IndexError:
                continue
        print("❌ アカウント一致なし")
        return None, None
    except Exception as e:
        print(f"❌ Sheets API 取得エラー: {e}")
        return None, None

def update_sheet_status(filename, status, error_message=""):
    try:
        sheet = gspread_client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()

        headers = data[0]
        filename_col = headers.index("ファイル名")
        status_col = headers.index("ステータス")
        error_col = headers.index("エラーメッセージ") if "エラーメッセージ" in headers else len(headers)

        if "エラーメッセージ" not in headers:
            sheet.update_cell(1, error_col + 1, "エラーメッセージ")

        for i, row in enumerate(data[1:], start=2):
            if row[filename_col] == filename:
                sheet.update_cell(i, status_col + 1, status)
                sheet.update_cell(i, error_col + 1, error_message)
                return
    except Exception as e:
        print(f"❌ スプレッドシート更新エラー: {e}")

def get_google_drive_file_path(filename):
    try:
        query = f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false"
        result = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = result.get("files", [])
        if files:
            return files[0]["id"]
        return None
    except Exception as e:
        print(f"❌ Google Drive ファイル取得エラー: {e}")
        return None

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "running"})

@app.route("/upload_sftp", methods=["POST"])
def upload_sftp():
    try:
        data = request.get_json()
        account = data.get("account")
        filename = data.get("filename")

        print(f"📥 POSTデータ: account={account}, filename={filename}")

        if not account or not filename:
            return jsonify({"status": "error", "message": "アカウントまたはファイル名が不足"}), 400

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
        sftp.put(file_path, f"{SFTP_UPLOAD_PATH}/{filename}")
        sftp.close()
        transport.close()

        update_sheet_status(filename, "アップロード完了")
        return jsonify({"status": "success", "message": f"{filename} のアップロード成功"})

    except Exception as e:
        print(f"❌ `/upload_sftp` でエラー: {e}")
        update_sheet_status(data.get("filename", "不明"), "エラー", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)