from src.preparing.DataLoader import DataLoader


class URL_Loader(DataLoader):
    def __init__(self, from_date=None, to_date=None):
        super().__init__()
        self.from_date = from_date
        self.to_date = to_date
