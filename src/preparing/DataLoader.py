from src.constants._url_paths import UrlPaths


class DataLoader:
    def __init__(
        self,
        alias="",
        from_location="",
        save_location="",
        save_file_name="",
        rerun=False,
    ):
        self.alias = alias
        self.from_location = from_location
        self.save_location = save_location
        self.save_file_name = save_file_name
        self.rerun = rerun

    def alias_existence_check(self):
        # クラスの属性を取得
        url_paths = UrlPaths()
        attributes = [attr for attr in dir(url_paths) if not attr.startswith("_")]
        # クラスの属性をたどって、alias_listを作成
        alias_list = [getattr(url_paths, attr)[0] for attr in attributes if isinstance(getattr(url_paths, attr), tuple)]

        # タプルの[0]の中にaliasと等しいものがあれば
        if self.alias in alias_list:
            # 該当する属性のタプルを取得
            attr = [attr for attr in attributes if getattr(url_paths, attr)[0] == self.alias][0]

            # タプルの[1]番目と[2]番目の要素を取得
            self.from_location = getattr(url_paths, attr)[1]
            self.to_location = getattr(url_paths, attr)[2]

            print(f"{self.alias} is from:{self.from_location} to:{self.to_location}")
        else:
            print("No such data")
