def clean_data(data):
    for key, value in data.items():
        if isinstance(value, dict):
            clean_data(value)
        elif isinstance(value, str):
            data[key] = value.replace('\x00', '')
