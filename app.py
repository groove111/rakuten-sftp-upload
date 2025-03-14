from flask import Flask, request, jsonify
import paramiko
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import datetime
import json  # json モジュールをインポート
import platform  # ← 追加
import time

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

import io

app = Flask(__name__)

# Google Sheets 設定
SPREADSHEET_ID = "1_t8pThdb0kFyIyRfNtC-VLsGa6HopgGQoEOqKyisjME"
SHEET_ACCOUNTS = "アカウント管理"
SHEET_RESERVATIONS = "アップロード予約"

# Google Drive 設定
FOLDER_ID = "1ykCNsVXqi619OzXwLTqVJIm1WbqWcMgn"

# Google Sheets 認証
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
client = gspread.authorize(creds)

# Google Drive API 認証
drive_service = build("drive", "v3", credentials=creds)

# SFTP 設定
SFTP_HOST = "upload.rakuten.ne.jp"
SFTP_PORT = 22
SFTP_UPLOAD_PATH = "/ritem/batch"

# 📌 アカウント情報を取得
def get_sftp_credentials(account_name):
    sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_ACCOUNTS)
    data = sheet.get_all_values()

    print(f"📌 デバッグ: スプレッドシートのアカウント一覧 → {data}")

    headers = data[0]
    account_data = [dict(zip(headers, row)) for row in data[1:]]

    # `アカウント名` のマッピング（日本語 → 英語）
    account_mapping = {
        "アウトスタイル": "outstyle-r",
        "LIMITEST": "limitest"
    }

    for row in account_data:
        if account_mapping.get(row["アカウント名"].strip(), row["アカウント名"].strip()) == account_name.strip():
            print(f"✅ アカウント情報取得成功: {row}")
            return row["FTP用ユーザー名"], row["FTP用パスワード"]

    print(f"❌ アカウント情報が見つかりません: {account_name}")
    return None, None

# 📌 予約データを取得（日本語対応）
@app.route("/get_reservations", methods=["GET"])
def get_reservations():
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()

        headers = data[0]
        valid_headers = ["予約日時", "アップロード先アカウント", "ファイル名", "ステータス"]

        records = [
            {key: value for key, value in zip(headers, row) if key in valid_headers}
            for row in data[1:] if any(row)
        ]

        return jsonify(records), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# 📌 Google Drive 内のファイル ID を取得
def get_google_drive_file_path(filename):
    results = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false",
        fields="files(id, name)"
    ).execute()
    
    files = results.get("files", [])
    
    if not files:
        print(f"❌ Google Drive に {filename} が見つかりません")
        return None
    
    file_id = files[0]["id"]
    print(f"✅ Google Drive からファイル取得成功: {filename} (ID: {file_id})")
    
    return file_id

# 📌 SFTPへアップロード
@app.route("/upload_sftp", methods=["POST"])
def upload_sftp():
    try:
        data = request.get_json()
        account = data["account"]
        filename = data["filename"]

        print(f"📌 リクエスト受信: アカウント={account}, ファイル名={filename}")

        username, password = get_sftp_credentials(account)
        if not username or not password:
            update_sheet_status(filename, "エラー", "FTPアカウント情報が見つかりません")
            return jsonify({"status": "error", "message": "FTPアカウント情報が見つかりません"}), 400

        print(f"📌 FTP接続情報: ユーザー名={username}, パスワード={password}")

        file_id = get_google_drive_file_path(filename)
        if not file_id:
            update_sheet_status(filename, "エラー", "Google Drive にファイルが見つかりません")
            return jsonify({"status": "error", "message": "Google Drive にファイルが見つかりません"}), 404

        # ✅ OS に応じて `/tmp/` か `./tmp/` を使用
        tmp_dir = "/tmp" if platform.system() != "Windows" else "./tmp"

        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
            print(f"📁 {tmp_dir} ディレクトリを作成しました")

        file_path = os.path.join(tmp_dir, filename)
        request_drive = drive_service.files().get_media(fileId=file_id)

        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request_drive)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"📥 ダウンロード進行中: {int(status.progress() * 100)}%")

        print(f"✅ Google Drive のファイルを {file_path} に保存完了")

        # 📌 SFTP 接続
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_file_path = f"{SFTP_UPLOAD_PATH}/{filename}"
        print(f"📌 {file_path} を {remote_file_path} にアップロード開始")

        sftp.put(file_path, remote_file_path)

        sftp.close()
        transport.close()

        print(f"✅ {filename} のアップロード完了！")

        # ✅ スプレッドシートのステータスを更新
        update_sheet_status(filename, "アップロード完了")

        return jsonify({"status": "success", "message": f"{filename} のアップロード完了"}), 200

    except Exception as e:
        print(f"❌ エラー発生: {str(e)}")
        update_sheet_status(filename, "エラー", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


# 📌 APIステータス確認
@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "running"}), 200

# 📌 スプレッドシートの `アップロード予約` のステータスを更新
def update_sheet_status(filename, status, error_message=""):
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()

        headers = data[0]
        filename_col = headers.index("ファイル名")
        status_col = headers.index("ステータス")

        # ✅ `エラーメッセージ` 列がなければ自動追加
        if "エラーメッセージ" not in headers:
            error_col = len(headers)
            sheet.update_cell(1, error_col + 1, "エラーメッセージ")  # ✅ 新しい列を作成
            headers.append("エラーメッセージ")  # ✅ ヘッダーリストにも追加
        else:
            error_col = headers.index("エラーメッセージ")

        for i, row in enumerate(data[1:], start=2):  # 2行目以降を走査
            if row[filename_col] == filename:
                sheet.update_cell(i, status_col + 1, status)
                sheet.update_cell(i, error_col + 1, error_message)
                print(f"✅ スプレッドシート更新: {filename} → {status}")
                return

        print(f"⚠️ スプレッドシート更新失敗: {filename} が見つかりません")
    except Exception as e:
        print(f"❌ スプレッドシート更新エラー: {str(e)}")

import time

def upload_sftp():
    try:
        data = request.get_json()
        account = data["account"]
        filename = data["filename"]

        print(f"📌 リクエスト受信: アカウント={account}, ファイル名={filename}")

        username, password = get_sftp_credentials(account)
        if not username or not password:
            return jsonify({"status": "error", "message": "FTPアカウント情報が見つかりません"}), 400

        print(f"📌 FTP接続情報: ユーザー名={username}, パスワード={password}")

        # ✅ Google Drive からファイルを取得（リトライ処理追加）
        max_retries = 3
        file_id = None
        for i in range(max_retries):
            file_id = get_google_drive_file_path(filename)
            if file_id:
                break
            print(f"⏳ リトライ中 ({i+1}/{max_retries})...")
            time.sleep(5)

        if not file_id:
            return jsonify({"status": "error", "message": f"Google Drive に {filename} が見つかりません"}), 404

        # ✅ Google Drive からダウンロード
        file_path = f"./tmp/{filename}"
        request = drive_service.files().get_media(fileId=file_id)
        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"📥 ダウンロード進行中: {int(status.progress() * 100)}%")

        print(f"✅ Google Drive のファイルを {file_path} に保存完了")

        # 📌 SFTPアップロード
        transport = paramiko.Transport(("upload.rakuten.ne.jp", 22))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_file_path = f"/ritem/batch/{filename}"
        print(f"📌 {file_path} を {remote_file_path} にアップロード開始")
        sftp.put(file_path, remote_file_path)

        # 📌 SFTP接続を閉じる
        sftp.close()
        transport.close()

        print(f"✅ {filename} のアップロード完了！")
        return jsonify({"status": "success", "message": f"✅ {filename} のアップロード完了"}), 200

    except Exception as e:
        print(f"❌ エラー発生: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# 📌 ルートページ
@app.route("/")
def home():
    return "Flask API is running!", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
