import os

CHECKPOINT_DIR = "checkpoint"

def _get_checkpoint_path(job_name):
    """Xác định đường dẫn file checkpoint dựa trên tên job."""
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    return os.path.join(CHECKPOINT_DIR, f"{job_name}_checkpoint.txt")

def get_checkpoint(job_name):
    """Đọc checkpoint hiện tại của một job từ file."""
    path = _get_checkpoint_path(job_name)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return f.read().strip()
    return None

def save_checkpoint(job_name, value):
    """Lưu giá trị checkpoint mới cho một job vào file."""
    path = _get_checkpoint_path(job_name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(str(value))

# Giữ lại hàm này để tránh break các chỗ đang gọi, 
# nhưng thay đổi nó để trả về một "manager" giả lập hoặc đơn giản là dùng trực tiếp các hàm trên.
def get_checkpoint_manager(job_name):
    """
    Tiện ích tạo nhanh manager dựa trên tên job.
    Trả về một object có các method get_checkpoint và save_checkpoint 
    để không phải sửa code ở các file khác.
    """
    class SimpleManager:
        def get_checkpoint(self):
            return get_checkpoint(job_name)
        def save_checkpoint(self, value):
            return save_checkpoint(job_name, value)
    
    return SimpleManager()
