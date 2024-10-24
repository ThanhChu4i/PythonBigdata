import os
import pickle
import json

def convert_pickle_to_json(pickle_folder, json_folder):
    # Tạo thư mục JSON nếu chưa tồn tại
    os.makedirs(json_folder, exist_ok=True)

    # Lặp qua tất cả các file trong thư mục
    for filename in os.listdir(pickle_folder):
        if filename.endswith('.pickle'):
            pickle_file_path = os.path.join(pickle_folder, filename)
            json_file_path = os.path.join(json_folder, filename.replace('.pickle', '.json'))

            # Đọc file .pickle
            with open(pickle_file_path, 'rb') as pickle_file:
                data = pickle.load(pickle_file)

            # Ghi dữ liệu vào file .json
            with open(json_file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)

            print(f"Đã chuyển đổi: {pickle_file_path} sang {json_file_path}")

# Sử dụng hàm
pickle_folder_path = r"D:\archive\Cleaned Analyses\Cleaned Analyses"  # Đường dẫn tới thư mục chứa file .pickle
json_folder_path = r"D:\archive\Cleaned Analyses\toJson"                # Đường dẫn tới thư mục lưu file .json

convert_pickle_to_json(pickle_folder_path, json_folder_path)
