# %%
# this cell enables relative path imports
import os
from dotenv import load_dotenv
load_dotenv()
_PROJECT_PATH: str = os.environ["_project_path"]
_PICKLED_DATA_FILENAME: str = os.environ["_pickled_data_filename"]

import sys
from pathlib import Path
project_path = Path(_PROJECT_PATH)
sys.path.append(str(project_path))

# %%
# import all your modules here
import pandas as pd
import nbformat as nbf

import config_v2 as cfg
from library_report_v2 import Processing as pro

# %%
# this is where you specify the blueprint notebook to be replicated
blueprint_filepath = project_path / 'tools' / 'modelo_tipo_mensual.ipynb'
nb_blueprint = nbf.read(blueprint_filepath, as_version=4)

# %%
#df = pd.read_pickle(project_path / 'data' / _PICKLED_DATA_FILENAME)
df = pd.read_pickle(r"/Users/jpocampo/Library/CloudStorage/OneDrive-CELSIAS.AE.S.P/Proyectos Digitalización/Bancolombia/CB_informes_Ubi/Informe_semanal_v2/data/data_weekly_report.pkl")
df_bl, df_st = pro.split_into_baseline_and_study(df, baseline=cfg.BASELINE, study=cfg.STUDY, inclusive='both')
set_devices = set(df['device_name'])

# %%
# We only want to create notebooks for offices with data in both the baseline and study periods.

#df = pd.read_pickle(project_path / 'data' / _PICKLED_DATA_FILENAME)
#df_bl, df_st = pro.split_into_baseline_and_study(df, baseline=cfg.BASELINE, study=cfg.STUDY, inclusive='both')

# we don't need this data anymore and it might be very large
# df = None

# To find the offices with data in both periods
# we can take the intersection of the sets
# set_devices_bl = set(df_bl['device_name'])
# set_devices_st = set(df_st['device_name'])

# set_devices = set_devices_bl.intersection(set_devices_st)

set_devices = set(df['device_name'])

# We would like to sort the offices in the same order as
# in Ubidots.
df_notebooks = pd.DataFrame(set_devices, columns=['device'])

# A dash always splits the number and the name. We take the
# first part.
df_notebooks['code'] = df_notebooks['device'].str.split('-',expand=True)[0]

# stripping "BC " leaves only the number
df_notebooks['number'] = df_notebooks['code'].str.strip('BC ')

# we then coerce to numeric
df_notebooks['number'] = pd.to_numeric(df_notebooks['number'], errors='coerce')

# finally we sort and make a list to iterate over
df_notebooks = df_notebooks.sort_values(by='number')
sorted_devices = list(df_notebooks['device'])

# %%
# We want to iterate over the office names
# and save a copy of the blueprint notebook
# for every office.

for device_name in sorted_devices:
    # extract the blueprint cells
    nb = nb_blueprint.copy()
    nb_cells = nb['cells']

    # rename the target notebook header cell
    cell_0 = nb_cells[0]
    cell_0['source'] = f'# {device_name}'

    # Format the first code cell (always the second cell)
    # which contains the device_name used to filter the data.
    # Also suppress warnings since this is client-side.
    cell_1 = nb_cells[1]
    cell_1['source'] = f'DEVICE_NAME = \'{device_name}\'\nimport warnings\nwarnings.filterwarnings("ignore")'

    # Combine the office-specific cells with the
    # body of the notebook.
    cell_rest = nb_cells[2:]
    new_cells = [cell_0] + [cell_1] + cell_rest
    nb['cells'] = new_cells

    # store the replicated notebook
    filename = f"Notebook {device_name}"
    write_path = project_path/'main'/'notebooks'/'individual'/f"{filename}.ipynb"
    nbf.write(nb, write_path)

    # print a chapter file line for the table-of-contents (_toc.yml)
    print(f"  - file: notebooks/individual/{filename}")



