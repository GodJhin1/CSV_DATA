import sys
import os
import shutil
import zipfile
from datetime import datetime, timedelta
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QTextEdit
from PyQt5.QtCore import Qt, QTimer
import concurrent.futures
import timeit

start = timeit.default_timer()
class DataTransferApp(QWidget):
    def __init__(self):
        super().__init__()

        self.local_csv_path = r'D:\path'
        self.local_image_path = r'D:\Image'
        self.server1_path = r'\\server1\storage'
        self.server2_path = r'\\server2\backup'

        self.log_info = []

        self.init_ui()

    def init_ui(self):
        self.setWindowTitle('Data Transfer App')
        self.setGeometry(100, 100, 600, 500)

        layout = QVBoxLayout()

        self.log_text_edit = QTextEdit(self)
        self.log_text_edit.setReadOnly(True)
        layout.addWidget(self.log_text_edit)

        btn_csv_transfer = QPushButton('CSV 转存', self)
        btn_csv_transfer.clicked.connect(self.transfer_csv)
        layout.addWidget(btn_csv_transfer)

        btn_image_transfer = QPushButton('Image 转存', self)
        btn_image_transfer.clicked.connect(self.transfer_image)
        layout.addWidget(btn_image_transfer)

        btn_image_backup = QPushButton('Image 压缩备份', self)
        btn_image_backup.clicked.connect(self.backup_image)
        layout.addWidget(btn_image_backup)

        self.setLayout(layout)

        self.show()

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f'{timestamp}: {message}'
        print(log_message)
        self.log_info.append(log_message)
        self.log_text_edit.setPlainText('\n'.join(self.log_info))

    def transfer_csv(self):
        try:
            destination = self.server1_path
            backup_server = self.server2_path

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                csv_tasks = {executor.submit(self.copy_csv, os.path.join(self.local_csv_path, file_name),
                                             os.path.join(destination, file_name)): file_name
                             for file_name in os.listdir(self.local_csv_path) if file_name.endswith('.csv')}

                concurrent.futures.wait(csv_tasks.keys())

                for file_name in os.listdir(self.local_csv_path):
                    if file_name.endswith('.csv'):
                        backup_path = os.path.join(backup_server, file_name)
                        shutil.copy(os.path.join(self.local_csv_path, file_name), backup_path)
                        self.log(f'Auto Transfer CSV: {file_name} to {backup_server}')

                        # 删除本地CSV文件
                        os.remove(os.path.join(self.local_csv_path, file_name))

        except Exception as e:
            self.log(f'Error during CSV Transfer: {e}')

    def transfer_image(self):
        try:
            destination = self.server1_path

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                image_tasks = {executor.submit(self.copy_image, os.path.join(self.local_image_path, file_name),
                                               os.path.join(destination, file_name)): file_name
                               for file_name in os.listdir(self.local_image_path)}

                concurrent.futures.wait(image_tasks.keys())

                for file_name in os.listdir(self.local_image_path):
                    backup_path = os.path.join(destination, file_name)
                    self.log(f'Auto Transfer Image: {file_name} to {destination}')

                    # 删除本地图片文件
                    os.remove(os.path.join(self.local_image_path, file_name))

        except Exception as e:
            self.log(f'Error during Image Transfer: {e}')

    def backup_image(self):
        try:
            backup_server = self.server2_path
            zip_name = 'image_backup.zip'
            zip_path = os.path.join(backup_server, zip_name)

            with zipfile.ZipFile(zip_path, 'w') as zipf:
                for file_name in os.listdir(self.local_image_path):
                    file_path = os.path.join(self.local_image_path, file_name)
                    zipf.write(file_path, file_name)

            self.log(f'Image Compression Backup to {backup_server}')

        except Exception as e:
            self.log(f'Error during Image Backup: {e}')

    def copy_csv(self, source_path, destination_path):
        shutil.copy(source_path, destination_path)
        self.log(f'Initial Storage CSV: {source_path} to {destination_path}')

    def copy_image(self, source_path, destination_path):
        shutil.copy(source_path, destination_path)
        self.log(f'Initial Storage Image: {source_path} to {destination_path}')

end = timeit.default_timer()
print(f"运行时间: {end - start} 秒")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    transfer_app = DataTransferApp()
    sys.exit(app.exec_())




