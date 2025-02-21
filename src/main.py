from pipelines.extract import extract_data
from pipelines.transform import transform_data
from pipelines.load import load_data

def main():
    raw_data = extract_data()
    clean_data = transform_data(raw_data)
    load_data(clean_data)

if __name__ == "__main__":
    main()
