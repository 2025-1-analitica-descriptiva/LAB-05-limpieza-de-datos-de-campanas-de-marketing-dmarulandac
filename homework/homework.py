"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import zipfile
    import pandas as pd
    from io import TextIOWrapper
    
    input_dir = "files/input"
    output_dir = "files/output"
    os.makedirs(output_dir, exist_ok=True)

    # Leer todos los .csv.zip en la carpeta input
    dataframes = []
    for file in os.listdir(input_dir):
        if file.endswith(".zip"):
            zip_path = os.path.join(input_dir, file)
            with zipfile.ZipFile(zip_path, 'r') as archive:
                for name in archive.namelist():
                    if name.endswith(".csv"):
                        with archive.open(name) as csv_file:
                            df = pd.read_csv(TextIOWrapper(csv_file, encoding='utf-8'))
                            dataframes.append(df)

    # Unir todos los DataFrames en uno
    if not dataframes:
        raise ValueError("No se encontraron archivos CSV en los ZIP.")

    df = pd.concat(dataframes, ignore_index=True)

    # -------------------------------
    # CLIENT.CSV
    # -------------------------------
    client_cols = [
        "client_id", "age", "job", "marital", "education", "credit_default", "mortgage"
    ]
    client = df[[col for col in client_cols if col in df.columns]].copy()

    if "job" in client.columns:
        client["job"] = client["job"].str.replace(".", "", regex=False)
        client["job"] = client["job"].str.replace("-", "_", regex=False)

    if "education" in client.columns:
        client["education"] = client["education"].str.replace(".", "_", regex=False)
        client["education"] = client["education"].replace("unknown", pd.NA)

    if "credit_default" in client.columns:
        client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)

    if "mortgage" in client.columns:
        client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)

    client.to_csv(os.path.join(output_dir, "client.csv"), index=False)

    # -------------------------------
    # CAMPAIGN.CSV
    # -------------------------------
    campaign_cols = [
        "client_id", "number_contacts", "contact_duration", "previous_campaign_contacts",
        "previous_outcome", "campaign_outcome", "day", "month"
    ]
    campaign = df[[col for col in campaign_cols if col in df.columns]].copy()

    if "previous_outcome" in campaign.columns:
        campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)

    if "campaign_outcome" in campaign.columns:
        campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

    if "day" in campaign.columns and "month" in campaign.columns:
        month_map = {
            'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
            'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
        }
        campaign["month"] = campaign["month"].str.lower().map(month_map)
        campaign["last_contact_date"] = pd.to_datetime(
            "2022-" + campaign["month"] + "-" + campaign["day"].astype(str),
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        campaign.drop(columns=["day", "month"], inplace=True)

    campaign.to_csv(os.path.join(output_dir, "campaign.csv"), index=False)

    # -------------------------------
    # ECONOMICS.CSV
    # -------------------------------
    df_econ = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    # Guardar economics.csv
    df_econ.to_csv(os.path.join(output_dir, "economics.csv"), index=False)
if __name__ == "__main__":
    clean_campaign_data()
