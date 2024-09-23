import pandas as pd

def read_large_csv(file_path, chunksize=100000, delimiter=','):
    data = pd.DataFrame()
    try:
        chunk_iter = pd.read_csv(file_path, chunksize=chunksize, sep=delimiter, on_bad_lines='skip')
        for chunk in chunk_iter:
            data = pd.concat([data, chunk], ignore_index=True)
    except pd.errors.ParserError as e:
        print(f"Eroare la citirea fișierului: {e}")
    return data

file_path_facebook = 'facebook_dataset.csv'
file_path_google = 'google_dataset.csv'
file_path_website = 'website_dataset.csv'

facebook_data = read_large_csv(file_path_facebook)
google_data = read_large_csv(file_path_google)
website_data = read_large_csv(file_path_website, delimiter=';')

print(facebook_data.columns)
print(google_data.columns)
print(website_data.columns)

merged_data = pd.merge(facebook_data, google_data, on='domain', how='inner')
merged_data = pd.merge(merged_data, website_data, left_on='domain', right_on='root_domain', how='inner')

duplicate_columns = merged_data.columns[merged_data.columns.duplicated()]

if len(duplicate_columns) > 0:
    print(f"Coloane duplicate găsite: {duplicate_columns}")
else:
    print("Nu s-au găsit coloane duplicate.")

merged_data = merged_data.loc[:, ~merged_data.columns.duplicated()]

print(merged_data.head())

merged_data.to_csv('merged_dataset_cleaned.csv', index=False)
