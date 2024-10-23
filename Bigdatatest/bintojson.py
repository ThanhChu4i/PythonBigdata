import json
import struct

def bin_to_json(bin_file, json_file):
    data_list = []

    # Đọc dữ liệu từ file .bin
    with open(bin_file, 'rb') as file:
        while chunk := file.read(4):  # Đọc mỗi lần 4 byte (giả sử dữ liệu là số nguyên 32-bit)
            value = struct.unpack('i', chunk)[0]  # Giải mã nhị phân thành số nguyên (int)
            data_list.append(value)
    
    # Ghi dữ liệu ra file .json
    with open(json_file, 'w') as json_f:
        json.dump(data_list, json_f, indent=4)

    print(f"Dữ liệu đã được chuyển đổi từ {bin_file} sang {json_file}")

# Sử dụng hàm
bin_to_json('data.bin', 'data.json')
