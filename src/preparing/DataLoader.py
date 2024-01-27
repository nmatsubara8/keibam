# import urllib.request
# import shutil
import os


class CustomDataLoader:
    def __init__(self, data_location="./data/tmp/", file_name="test.csv"):
        self.data_location = data_location
        self.file_name = file_name
        self.folder_path = os.path.dirname(self.data_location)
        self.file_path = os.path.join(self.folder_path, self.file_name)

    def load_data_from_local_folder(self):
        if not os.path.exists(self.folder_path):
            try:
                os.mkdir(self.folder_path)
            except Exception as e:
                print(f"Error creating folder: {e}")

        if not os.path.exists(self.file_path):
            try:
                with open(self.file_path, "wb") as f:
                    f.write("test data")

            except Exception as e:
                print(f"Failed to load data from local folder: {e}")

        else:
            with open(self.file_path, "a+") as f:
                f.write("data added")
            print("file loading done")


"""

        def load_data(self,data_location):
            if self.data_location.startswith("http://") or self.data_location.startswith("https://"):
                self.load_data_from_url()
            elif os.path.isdir(self.data_location):
                self.load_data_from_local_folder()
            else:
                raise ValueError("Unsupported data location")


    def load_data_from_url(self):
        try:
            with urllib.request.urlopen(self.data_location) as response:
                self.data = response.read()
        except Exception as e:
            raise RuntimeError(f"Failed to load data from URL: {str(e)}")



    def save_data(self):
        if self.data is None:
            raise RuntimeError("Data not loaded. Call load_data() first.")

        if not os.path.exists(self.destination_folder):
            os.makedirs(self.destination_folder)

        destination_path = os.path.join(self.destination_folder, 'loaded_data')
        with open(destination_path, 'wb') as file:
            file.write(self.data)

        print(f"Data saved to: {destination_path}")

# 使用例
data_location = "https://example.com/sample_data.txt"  # ダウンロード対象の外部URL
destination_folder = "./data_folder"  # データを格納するフォルダ

data_loader = CustomDataLoader(data_location, destination_folder)
data_loader.load_data()
data_loader.save_data()
"""
