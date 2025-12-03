import os
import random
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

# .envの読み込み
load_dotenv()

# --- 設定値の取得 ---
ELASTIC_HOST = os.getenv("ELASTIC_HOST")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "synthetic-storage-history")

# --- データ生成の設定 ---
DATA_POINTS = 1000
CHANGE_POINT_RATIO = 0.6
START_PCT = 0.30
MID_PCT = 0.45
END_PCT = 0.85

def generate_series():
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(hours=DATA_POINTS)
    
    change_point_index = int(DATA_POINTS * CHANGE_POINT_RATIO)
    
    # ★変化点の時刻を計算
    change_point_time = start_time + timedelta(hours=change_point_index)

    slope_1 = (MID_PCT - START_PCT) / change_point_index
    slope_2 = (END_PCT - MID_PCT) / (DATA_POINTS - change_point_index)

    print(f"接続先: {ELASTIC_HOST}")
    print(f"データ期間: {start_time} 〜 {now}")
    print("-" * 40)
    print(f"★ トレンド変化時刻: {change_point_time}")
    print(f"   (データ生成開始から {change_point_index} 時間後)")
    print("-" * 40)

    # ★前回の値を保持する変数を初期化
    previous_val = START_PCT

    for i in range(DATA_POINTS):
        current_time = start_time + timedelta(hours=i)
        
        # 基本トレンド（直線の計算）
        if i < change_point_index:
            base_val = START_PCT + (slope_1 * i)
        else:
            steps_after_change = i - change_point_index
            base_val = MID_PCT + (slope_2 * steps_after_change)
        
        # ノイズ (少し小さめに設定: ±0.5%)
        noise = random.uniform(-0.005, 0.005)
        calculated_val = base_val + noise
        
        # ★ここが重要: 今回の値は「前回の値」を下回らないようにする
        final_val = max(previous_val, calculated_val)
        
        # 100%を超えないようにキャップ
        final_val = min(1.0, final_val)

        # 次回の比較用に更新
        previous_val = final_val

        doc = {
            "_index": ELASTIC_INDEX,
            "_source": {
                "@timestamp": current_time.isoformat(),
                "host": { "name": "synthetic-server-01" },
                "system": {
                    "filesystem": {
                        "used": { "pct": round(final_val, 4) }
                    }
                }
            }
        }
        yield doc

def main():
    if not ELASTIC_HOST or not ELASTIC_API_KEY:
        print("エラー: .envファイルの設定を確認してください。")
        return

    client = Elasticsearch(
        ELASTIC_HOST,
        api_key=ELASTIC_API_KEY,
        request_timeout=30
    )

    try:
        info = client.info()
        print(f"Elasticsearch接続成功: v{info['version']['number']}")
    except Exception as e:
        print(f"接続エラー: {e}")
        return

    # インデックスの再作成（マッピングエラー防止）
    if client.indices.exists(index=ELASTIC_INDEX):
        print(f"既存のインデックス '{ELASTIC_INDEX}' を削除しています...")
        client.indices.delete(index=ELASTIC_INDEX)
        print("削除完了。")
    
    print(f"インデックス '{ELASTIC_INDEX}' へデータを送信中...")
    
    try:
        success, failed = helpers.bulk(client, generate_series())
        print(f"完了: 成功 {success} 件")
            
    except helpers.BulkIndexError as e:
        print("\n=== 送信エラー詳細 ===")
        print(f"{len(e.errors)} 件のデータ送信に失敗しました。")
        print(e.errors[0])

    except Exception as e:
        print(f"予期せぬエラー: {e}")

if __name__ == "__main__":
    main()
