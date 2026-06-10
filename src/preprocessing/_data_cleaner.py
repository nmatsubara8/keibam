def convert_to_datetime(date_str, time_str):
    # 日付と時刻を結合して datetime オブジェクトに変換
    datetime_str = date_str.split()[0] + " " + time_str
    return datetime_str


def dict_selector(dict_name):
    if dict_name == "_results":
        column_types_dict = {
            "開催": ("float", "str", 2, True),
            # "race_id": ("float", "int", 2, False),
            # "time": ("str", "datetime", 0, False),
            "days": ("float", "int", 2, False),
            "times": ("float", "int", 2, False),
            "teiryo": ("float", "int", 1, False),
            "barei": ("float", "int", 1, False),
            "kokusai": ("float", "int", 1, False),
            "shitei": ("float", "int", 1, False),
            "tokushi": ("float", "int", 1, False),
            "toku": ("float", "int", 1, False),
            "handi": ("float", "int", 1, False),
            "bettei": ("float", "int", 1, False),
            "minarai": ("float", "int", 1, False),
            "wakate": ("float", "int", 1, False),
            "shogai": ("float", "int", 1, False),
            "chiho": ("float", "int", 1, False),
            "gai": ("float", "int", 1, False),
            "kongo": ("float", "int", 1, False),
            "hinba": ("float", "int", 1, False),
            "trainer_id": ("int", "str", 5, True),
            "jockey_id": ("int", "str", 5, True),
            "owner_id": ("int", "str", 6, True),
        }
    elif dict_name == "_horse_results":
        column_types_dict = {
            "R": ("float", "int", 2, False),
            "枠番": ("float", "int", 2, False),
            "人気": ("float", "int", 2, False),
        }

    else:
        column_types_dict = {
            "horse_id": ("int", "str", 10, False),
        }
    return column_types_dict


def convert_column_types(df, col_types):
    converted_df = df.copy()
    for column, (from_data_type, to_data_type, width, padding) in col_types.items():
        if column not in converted_df.columns:
            continue
        if from_data_type == "float" and to_data_type == "int":
            converted_df[column] = converted_df[column].fillna(0).astype(to_data_type)
        elif to_data_type == "str" and padding:
            if from_data_type == "float":
                converted_df[column] = (
                    converted_df[column].fillna(0).astype(float).astype(int).astype(str).str.zfill(width)
                )
            else:
                converted_df[column] = converted_df[column].astype(str).str.zfill(width)
        else:
            converted_df[column] = converted_df[column].astype(to_data_type)
    return converted_df
