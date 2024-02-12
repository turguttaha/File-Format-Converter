import glob
import sys
import os
import json
import re
import pandas as pd


 
def schema_to_column_names(schema, ds_name, sorting_key='column_position'):
    column_details = schema[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file_path, schema):
    file_path_elements = re.split('[/\\\]', file_path)
    ds_name = file_path_elements[-2]
    column_names = schema_to_column_names(schema, ds_name)
    return pd.read_csv(file_path, names=column_names)

def to_json(df, tgt_base_dir, ds_name, file_name):
    json_file_path = f'{tgt_base_dir}/{ds_name}'
    os.makedirs(json_file_path, exist_ok=True)
    df.to_json(f'{json_file_path}/{file_name}', orient='records', lines=True)

def file_converter(src_base_dir, tgt_base_dir, ds_name):
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df,tgt_base_dir, ds_name, file_name)

def process_files(ds_names=None):
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    tgt_base_dir = os.environ.get('TGT_BASE_DIR')
    schemas = json.load(open(f'{src_base_dir}/schemas.json'))
    if ds_names is None: # == if not ds_names
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            file_converter(src_base_dir, tgt_base_dir, ds_name)
            print('processing done') 
        except NameError as ne:
            print(f'Error: {ds_name}')
            print(ne)
            pass
if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = sys.argv[1]
        ds_names = json.loads(ds_names)
        process_files(ds_names)
    else:
        process_files()