# notification.py
import smtplib
from email.mime.text import MIMEText
import os

def send_error_email(script_name, error_log):
    """
    Hàm gửi email cảnh báo lỗi
    """
    SENDER_EMAIL = os.getenv("MAIL_USER")
    SENDER_PASSWORD = os.getenv("MAIL_PASS")
    RECEIVER_EMAIL = os.getenv("MAIL_RECEIVER", "")
 # list danh sách email

# Chuyển chuỗi thành List để Python xử lý, loại bỏ khoảng trắng thừa
    receiver_list = [email.strip() for email in RECEIVER_EMAIL.split(',')]

    subject = f"[ALERT] Lỗi ETL tại script: {script_name}"
    body = f"""
    Hệ thống phát hiện lỗi khi chạy script: {script_name}
    
    --------------------------------------------------
    CHI TIẾT LỖI (LOG):
    {error_log}
    --------------------------------------------------
    
    Vui lòng kiểm tra ngay.
    """

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(receiver_list)

    try:
        if not SENDER_EMAIL or not SENDER_PASSWORD:
            print("Chưa cấu hình MAIL_USER hoặc MAIL_PASS")
            return False
        # Kết nối tới Gmail Server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
            smtp_server.login(SENDER_EMAIL, SENDER_PASSWORD)
            smtp_server.sendmail(SENDER_EMAIL, receiver_list, msg.as_string())
        return True
    except Exception as e:
        print(f"Không thể gửi email: {e}")
        return False