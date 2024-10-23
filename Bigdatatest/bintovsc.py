import csv
import struct

def bin_to_csv(bin_file, csv_file):
    data_list = []

    # Đọc dữ liệu từ file .bin
    with open(bin_file, 'rb') as file:
        while chunk := file.read(4):  # Đọc mỗi lần 4 byte (giả sử dữ liệu là số nguyên 32-bit)
            value = struct.unpack('i', chunk)[0]  # Giải mã nhị phân thành số nguyên (int)
            data_list.append([value])  # Thêm giá trị vào danh sách (dưới dạng hàng)

    # Ghi dữ liệu ra file .csv
    with open(csv_file, 'w', newline='') as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(['Value'])  # Tạo tiêu đề cột
        writer.writerows(data_list)

    print(f"Dữ liệu đã được chuyển đổi từ {bin_file} sang {csv_file}")

# Sử dụng hàm
bin_to_csv('data.bin', 'data.csv')
