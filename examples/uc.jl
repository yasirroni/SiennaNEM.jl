using SiennaNEM

data_dir = "data/nem12"

data = read_data_csv(data_dir)
create_system!(data)

