import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

def get_kafka_producer():
    """Khởi tạo Kafka Producer với cấu hình bảo mật SASL (chuẩn theo docker-compose.yml)"""
    return KafkaProducer(
        bootstrap_servers='localhost:9093',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username='admin',
        sasl_plain_password='admin',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_transaction():
    """Tạo ngẫu nhiên dữ liệu giao dịch giả lập"""
    
    # 90% giao dịch bình thường (dưới 5,000 USD)
    # 10% giao dịch bất thường (rất lớn, trên 15,000 USD để trigger Rule 1)
    is_anomaly = random.random() < 0.1
    
    if is_anomaly:
        amount = round(random.uniform(15000.0, 50000.0), 2)
    else:
        amount = round(random.uniform(10.0, 4999.0), 2)
        
    return {
        "transactionId": str(uuid.uuid4()),
        "accountId": f"ACC_{random.randint(100, 105)}", # Có 6 tài khoản để dễ bị trùng lặp -> Trigger Rule 2
        "amount": amount,
        "location": random.choice(["Hanoi", "HoChiMinh", "New York", "London", "Tokyo", "Singapore"]),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

def main():
    topic_name = "financial_transactions"
    
    try:
        producer = get_kafka_producer()
        print(f"✅ Kết nối thành công tới Kafka tại localhost:9093. Đang đẩy dữ liệu vào topic '{topic_name}'...")
        
        while True:
            # Sinh ra 1 giao dịch
            transaction = generate_transaction()
            
            # Đôi khi gửi dồn dập nhiều giao dịch (Mô phỏng hacker quẹt thẻ liên tục để trigger Rule 2)
            if random.random() < 0.05:
                print(f"⚠️ [MÔ PHỎNG] Gửi dồn dập 5 giao dịch cho tài khoản {transaction['accountId']}")
                for _ in range(5):
                    spam_tx = transaction.copy()
                    spam_tx["transactionId"] = str(uuid.uuid4())
                    spam_tx["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    producer.send(topic_name, value=spam_tx)
                    time.sleep(0.1) # Dồn dập
            else:
                producer.send(topic_name, value=transaction)
            
            # In ra console để theo dõi
            print(f"Đã gửi: {transaction}")
            
            # Đợi 1 thời gian ngẫu nhiên từ 0.5s đến 2s
            time.sleep(random.uniform(0.5, 2.0))
            
    except Exception as e:
        print(f"❌ Lỗi khi gửi dữ liệu tới Kafka: {e}")
        print("💡 Gợi ý: Hãy chắc chắn bạn đã cài đặt thư viện 'kafka-python' và Kafka Docker đang chạy!")

if __name__ == "__main__":
    main()
