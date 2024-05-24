import glob
from csv_to_bq import upload_csv_to_bigquery


def main():
    filenames = glob.glob("../raw_data/*")

    dataset_id = "raw"

    for filename in filenames:
        table_name = filename.split("/")[-1].split(".")[0]
        csv_file_path = f"{filename}"
        try:
            upload_csv_to_bigquery(
                csv_file_path,
                dataset_id,
                table_name,
            )
        except Exception as e:
            print(f"Failed to upload data for {filename}: {e}")


if __name__ == "__main__":
    main()
