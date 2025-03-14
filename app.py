from flask import Flask, request, jsonify
import paramiko
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from dotenv import load_dotenv
import os
import json
import base64
import platform
import io

# ✅ Flask アプリの初期化
app = Flask(__name__)

# ✅ 環境変数をロード
load_dotenv()

# ✅ Google 認証情報のデコード（エラー処理追加）
creds_json_base64 = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not creds_json_base64:
    raise ValueError("❌ 環境変数 GOOGLE_CREDENTIALS_JSON が設定されていません")

try:
    creds_json_str = base64.b64decode(creds_json_base64).decode("utf-8")
    creds_dict = json.loads(creds_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    print("✅ Google 認証情報を正常にロードしました")
except Exception as e:
    print(f"❌ GOOGLE_CREDENTIALS_JSON のデコードに失敗: {e}")
    raise ValueError(f"❌ GOOGLE_CREDENTIALS_JSON のデコードに失敗: {e}")

# ✅ Google Sheets & Google Drive 設定
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1_t8pThdb0kFyIyRfNtC-VLsGa6HopgGQoEOqKyisjME")
SHEET_ACCOUNTS = "アカウント管理"
SHEET_RESERVATIONS = "アップロード予約"
FOLDER_ID = os.getenv("FOLDER_ID", "1ykCNsVXqi619OzXwLTqVJIm1WbqWcMgn")

# ✅ Gspread & Google Drive API の初期化
try:
    client = gspread.authorize(creds)
    drive_service = build("drive", "v3", credentials=creds)
    print("✅ Google Sheets & Google Drive API を正常に初期化しました")
except Exception as e:
    print(f"❌ Google API の初期化エラー: {e}")
    raise ValueError(f"❌ Google API の初期化エラー: {e}")

# ✅ SFTP 設定
SFTP_HOST = "upload.rakuten.ne.jp"
SFTP_PORT = 22
SFTP_UPLOAD_PATH = "/ritem/batch"

# ✅ スプレッドシートのステータス更新（エラー処理改善）
def update_sheet_status(filename, status, error_message=""):
    """スプレッドシートのステータスを更新"""
    try:
        print(f"📌 スプレッドシート更新: {filename} → {status} (エラー: {error_message})")
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()

        if not data:
            print("❌ スプレッドシートが空です")
            return

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

# ✅ SFTPアカウント情報を取得
def get_sftp_credentials(account_name):
    """Google Sheets から SFTP 認証情報を取得"""
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_ACCOUNTS)
        data = sheet.get_all_values()

        headers = data[0]
        account_data = [dict(zip(headers, row)) for row in data[1:]]

        for row in account_data:
            if row.get("アカウント名") == account_name:
                return row.get("FTP用ユーザー名"), row.get("FTP用パスワード")

        return None, None
    except Exception as e:
        print(f"❌ アカウント情報取得エラー: {e}")
        return None, None

# ✅ Google Drive 内のファイル ID を取得（最適化）
def get_google_drive_file_id(filename):
    """Google Drive から指定ファイルの ID を取得"""
    try:
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false",
            fields="files(id, name)"
        ).execute()

        files = results.get("files", [])
        if files:
            print(f"✅ Google Drive で {filename} のファイル ID を取得: {files[0]['id']}")
            return files[0]["id"]
        else:
            print(f"❌ Google Drive に {filename} は存在しません")
            return None
    except Exception as e:
        print(f"❌ Google Drive ファイル検索エラー: {e}")
        return None

# ✅ 予約状況取得（404 解決）
@app.route("/get_reservations", methods=["GET"])
def get_reservations():
    """Google Sheets から予約データを取得"""
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_RESERVATIONS)
        data = sheet.get_all_values()
        return jsonify(data), 200
    except Exception as e:
        print(f"❌ /get_reservations エラー: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ✅ SFTP アカウント情報を取得（エラーハンドリング強化）
def get_sftp_credentials(account_name):
    """スプレッドシートからSFTPアカウント情報を取得"""
    try:
        sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_ACCOUNTS)
        data = sheet.get_all_values()

        if not data or len(data) < 2:
            print("❌ スプレッドシートにアカウントデータがありません")
            return None, None

        headers = data[0]
        account_data = [dict(zip(headers, row)) for row in data[1:]]

        # 受け取ったアカウント名とマッチするものを検索
        for row in account_data:
            print(f"🔍 チェック中: {row.get('アカウント名')}")  # デバッグ用
            if row.get("アカウント名") and row["アカウント名"].strip() == account_name.strip():
                return row.get("FTP用ユーザー名"), row.get("FTP用パスワード")

        print(f"❌ `{account_name}` の認証情報が見つかりません")
        return None, None
    except Exception as e:
        print(f"❌ アカウント情報取得エラー: {e}")
        return None, None


# ✅ Google Drive 内のファイル ID を取得（エラーハンドリング強化）
def get_google_drive_file_path(filename):
    """Google Drive 内で指定したファイルの ID を取得"""
    try:
        results = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and name='{filename}' and trashed=false",
            fields="files(id, name)"
        ).execute()
        
        files = results.get("files", [])
        if not files:
            print(f"❌ Google Drive に {filename} が見つかりません")
            return None

        print(f"✅ Google Drive で {filename} の ID を取得: {files[0]['id']}")
        return files[0]["id"]
    except Exception as e:
        print(f"❌ Google Drive ファイル検索エラー: {e}")
        return None


# ✅ SFTPへアップロード（ログとエラーハンドリング強化）
@app.route("/upload_sftp", methods=["POST"])
def upload_sftp():
    """Google Drive からファイルをダウンロードし SFTP へアップロード"""
    try:
        data = request.get_json()
        print(f"📌 受信データ: {data}")

        if not data:
            return jsonify({"status": "error", "message": "リクエストが空です"}), 400

        account = data.get("account")
        filename = data.get("filename")

        if not account or not filename:
            return jsonify({"status": "error", "message": "アカウントまたはファイル名が不足しています"}), 400

        # ✅ SFTP 認証情報の取得と確認
        username, password = get_sftp_credentials(account)
        if not username or not password:
            update_sheet_status(filename, "エラー", "FTPアカウント情報が見つかりません")
            return jsonify({"status": "error", "message": "FTPアカウント情報が見つかりません"}), 400

        # ✅ Google Drive のファイル ID 取得
        file_id = get_google_drive_file_path(filename)
        if not file_id:
            update_sheet_status(filename, "エラー", "Google Drive にファイルが見つかりません")
            return jsonify({"status": "error", "message": "Google Drive にファイルが見つかりません"}), 404

        # ✅ 一時ディレクトリの作成
        tmp_dir = "/tmp" if platform.system() != "Windows" else "./tmp"
        os.makedirs(tmp_dir, exist_ok=True)

        file_path = os.path.join(tmp_dir, filename)
        request_drive = drive_service.files().get_media(fileId=file_id)

        # ✅ Google Drive からファイルをダウンロード
        with open(file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request_drive)
            done = False
            while not done:
                status, done = downloader.next_chunk()

        print(f"📂 ダウンロード完了: {file_path}")

        # ✅ SFTP へアップロード
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_path = f"{SFTP_UPLOAD_PATH}/{filename}"
        sftp.put(file_path, remote_path)
        print(f"✅ SFTP アップロード成功: {filename}")

        sftp.close()
        transport.close()

        # ✅ Google Drive からファイルを削除
        drive_service.files().delete(fileId=file_id).execute()
        print(f"🗑 Google Drive から {filename} を削除しました")

        update_sheet_status(filename, "アップロード完了")
        return jsonify({"status": "success", "message": f"{filename} のアップロード成功"}), 200

    except Exception as e:
        print(f"❌ `/upload_sftp` でエラー: {str(e)}")
        update_sheet_status(filename, "エラー", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


# ✅ ルートページとステータス確認
@app.route("/")
def home():
    return "Flask API is running!", 200

@app.route("/status", methods=["GET"])
def status():
    return jsonify({"status": "running"}), 200


if __name__ == "__main__":
    print("🚀 Flask サーバー起動: ポート 10000")
    app.run(host="0.0.0.0", port=10000, debug=True)

