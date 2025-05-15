import pandas as pd
from io import StringIO


def process_csv_and_sum_rows():
    csv_data = """
        Ottokraftstoff,Flugturbinenkraftstoff,Diesel-Kraftstoff,Heizöl,Flüssiggas
        0.17,0.0,2.49,1.26,1.66
        0.0,0.0,0.0,8.74,0.27
        0.0,0.0,0.0,0.10,0.0
        0.0,0.0,0.0,5.22,4.14
        0.0,0.0,0.0,3.12,0.0
        0.0,0.0,0.0,6.56
        0.0,0.0,0.0,0.02,0.01
        0.0,0.0,0.0,0.27
        1.68,0.0,24.95,3.87,0.04
        0.04,0.0,0.54,0.38,0.0
        0.04,1.04,0.59,0.12,0.0
        0.0,0.0,0.0,2.90
        0.0,0.0,0.0,0.13
    """
    # Read the CSV data from the string
    df = pd.read_csv(StringIO(csv_data))

    # Replace missing values with 0
    df.fillna(0, inplace=True)

    # Add a new column with row sums
    df['Row Sum'] = df.sum(axis=1)


    # Compute row sums
    row_sums = df.sum(axis=1)

    # Normalize the row sums to total 1
    normalized = (row_sums / row_sums.sum() ) * 100

    # Add the normalized row sums as a new column
    df['Normalized Row Sum'] = normalized

    normalized_df = pd.DataFrame({'Normalized Row Sum': normalized})

    print(normalized_df.to_csv(index=False))

    # save to csv
    normalized_df.to_csv('utils_flo/application_petrol/normalized_row_sum.csv', index=False)

    print('x')

# Run the function
process_csv_and_sum_rows()


"""
Normalized Row Sum
0.079317697228145
0.12807391613361763
0.0014214641080312724
0.13304904051172708
0.044349680170575695
0.09324804548685146
0.0004264392324093817
0.0038379530916844355
0.4341151385927506
0.013646055437100216
0.025444207533759776
0.0412224591329069
0.001847903340440654
"""