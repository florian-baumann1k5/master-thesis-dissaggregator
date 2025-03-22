import pandas as pd

def get_unique_attribute_codes(csv_path: str) -> set:
    """
    Reads a CSV at the given path and extracts all unique values from
    the '2_variable_attribute_code' column.
    
    :param csv_path: Path to the CSV file (e.g. "data/raw/dimensionless/ugr_2000to2020.csv")
    :return: A set of unique values in the '2_variable_attribute_code' column.
    """
    df = pd.read_csv(
        csv_path, 
        sep=";",       # if the file uses semicolons instead of commas
        engine="python", 
        on_bad_lines="skip"     # newer pandas >= 1.3.0
    )
    unique_codes = df["2_variable_attribute_code"].unique()
    return set(unique_codes)

# Example usage:
if __name__ == "__main__":
    csv_file = "data/raw/dimensionless/ugr_2000to2020.csv"
    codes = get_unique_attribute_codes(csv_file)
    print("Unique codes:", codes)
    print("len(codes):", len(codes))