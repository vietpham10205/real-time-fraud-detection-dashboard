import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

def create_kafka_producer():
    """Khởi tạo Kafka Producer với cấu hình bảo mật SASL"""
    return KafkaProducer(
        bootstrap_servers='localhost:9093',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='admin',
        sasl_plain_password='admin',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_movie_rating():
    """Tạo ngẫu nhiên dữ liệu đánh giá phim (Người dùng thật & Hacker)"""
    
    # 80% là bình thường, 20% là Hacker cố tình spam
    is_anomaly = random.random() < 0.2
    
    if is_anomaly:
        # Hacker cố tình dìm giá: Toàn đánh giá 0.5 hoặc 1.0 (Bị AI và Z-Score tóm)
        rating = random.choice([0.5, 1.0])
        user_id = "9999" # Dùng chung 1 tài khoản spam liên tục
    else:
        rating = round(random.uniform(3.0, 5.0), 1)
        user_id = str(random.randint(1, 1000))
        
    return {
        "userId": user_id,
        "movie": {
            "movieId": str(random.randint(1, 100)),
            "title": f"Movie_Demo_{random.randint(1, 10)}"
        },
        "rating": rating,
        "timestamp": str(int(time.time()))
    }

if __name__ == "__main__":
    producer = create_kafka_producer()
    print("🚀 BẮT ĐẦU GIẢ LẬP HACKER (REVIEW BOMBING) TẤN CÔNG VÀO HỆ THỐNG...")
    print("Chờ một lát để xem AI và Z-Score trên Dashboard bắt quả tang nhé!\n")
    
    try:
        while True:
            rating_data = generate_movie_rating()
            
            # Gửi thẳng vào topic 'ratings' chung mâm với dữ liệu thật
            producer.send('ratings', value=rating_data)
            
            if rating_data["userId"] == "9999":
                print(f"🚨 [HACKER] Tài khoản 9999 vừa spam đánh giá {rating_data['rating']} sao!")
                # Bắn siêu nhanh 0.03s để xả rác vào hệ thống
                time.sleep(0.03)
            else:
                print(f"✅ [NORMAL] Tài khoản {rating_data['userId']} đánh giá {rating_data['rating']} sao.")
                # Gửi nhanh hơn gấp 3 lần (0.3s)
                time.sleep(0.3)
                
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng công cụ Hacker.")
    finally:
        producer.close()
