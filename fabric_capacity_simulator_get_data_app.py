import streamlit as st
import pandas as pd
import requests
import datetime as dt
import os  # <== para manipular pastas/arquivos

# =============================
# FUNÇÕES ORIGINAIS
# =============================
def get_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default"
    }
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

def list_workspaces(token):
    url = "https://api.powerbi.com/v1.0/myorg/groups"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])

def list_items(token, workspace_id):
    headers = {"Authorization": f"Bearer {token}"}

    reports_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
    dashboards_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/dashboards"
    datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"

    reports = requests.get(reports_url, headers=headers).json().get("value", [])
    dashboards = requests.get(dashboards_url, headers=headers).json().get("value", [])
    datasets = requests.get(datasets_url, headers=headers).json().get("value", [])

    items = []
    for r in reports:
        items.append({"id": r["id"], "name": r["name"], "type": "Report"})
    for d in dashboards:
        items.append({"id": d["id"], "name": d["displayName"], "type": "Dashboard"})
    for ds in datasets:
        items.append({"id": ds["id"], "name": ds["name"], "type": "Dataset"})

    return items

def fn_iterate_dates(start_date, end_date):
    dates = []
    start = dt.datetime.strptime(start_date, '%Y-%m-%d')
    end = dt.datetime.strptime(end_date, '%Y-%m-%d')
    current_date = start.date()
    while current_date <= end.date():
        dates.append({
            "date": current_date,
            "year": current_date.year,
            "month": current_date.month,
            "day": current_date.day
        })
        current_date += dt.timedelta(days=1)
    return dates

def execute_dax_query(token, dataset_id, dax_query):
    url = f"https://api.powerbi.com/v1.0/myorg/datasets/{dataset_id}/executeQueries"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    response = requests.post(url, json=dax_query, headers=headers)
    response.raise_for_status()
    data = response.json()
    rows = data['results'][0]['tables'][0]['rows']
    return pd.DataFrame(rows)

# Funções específicas de queries
def execute_dax_query_itens(token, dataset_id):
    dax_query = {"queries": [{"query": "EVALUATE Items"}], "serializerSettings": {"includeNulls": True}}
    df = execute_dax_query(token, dataset_id, dax_query)
    return df.rename(columns={
        'Items[Capacity Id]': 'Capacity_Id',
        'Items[Item Id]': 'Item_Id',
        'Items[Item kind]': 'Item_kind',
        'Items[Item name]': 'Item_name',
        'Items[Users]': 'Users',
        'Items[Timestamp]': 'Timestamp',
        'Items[Workspace Id]': 'Workspace_Id',
        'Items[Workspace name]': 'Workspace_name',
        'Items[Billable type]': 'Billable_type',
        'Items[Virtualised item]': 'Virtualised_item',
        'Items[Virtualised workspace]': 'Virtualised_workspace',
        'Items[Is virtual  item status]': 'Is_virtual_item_status',
        'Items[Is virtual workspace status]': 'Is_virtual_workspace_status',
        'Items[Unique key]': 'Unique_key',
        'Items[Item key]': 'Item_key'
    })

def execute_dax_query_sku(token, dataset_id):
    dax_query = {
        "queries": [{
            "query": """
                DEFINE
                VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        'Capacities'[capacity Id],
                        'Capacities'[Capacity memory in GB],
                        'Capacities'[Capacity number of Vcores],
                        'Capacities'[Capacity plan],
                        'Capacities'[Creation date],
                        'Capacities'[mode],
                        'Capacities'[Owners],
                        'Capacities'[Region],
                        'Capacities'[SKU],
                        'Capacities'[Capacity name],
                        "Sku_CU", 'All Measures'[SKU CU by timepoint]
                    )
                EVALUATE __DS0Core
            """
        }],
        "serializerSettings": {"includeNulls": True}
    }
    df = execute_dax_query(token, dataset_id, dax_query)
    return df.rename(columns={
        "Capacities[capacity Id]": 'Capacity_Id',
        "Capacities[Capacity memory in GB]": 'Capacity_memory_GB',
        "Capacities[Capacity number of Vcores]": 'Capacity_vcores',
        "Capacities[Capacity plan]": 'Capacity_plan',
        "Capacities[Creation date]": 'Creation_date',
        "Capacities[mode]": 'Mode',
        "Capacities[Owners]": 'Owners',
        "Capacities[Region]": 'Region',
        "Capacities[SKU]": 'SKU',
        "Capacities[Capacity name]": 'Capacity_name',
        "[Sku_CU]": 'Sku_CU'
    })

def execute_dax_query_itens_utilization(token, dataset_id):
    dax_query = {
        "queries": [{
            "query": """
                EVALUATE
                'Metrics By Item Operation And Day'
            """
        }],
        "serializerSettings": {"includeNulls": True}
    }
    df = execute_dax_query(token, dataset_id, dax_query)
    return df.rename(columns={
        'Metrics By Item Operation And Day[Datetime]': 'Datetime',
        'Metrics By Item Operation And Day[Date]': 'Date',
        'Metrics By Item Operation And Day[Item Id]': 'Item_Id',
        'Metrics By Item Operation And Day[Operation name]': 'Operation_name',
        'Metrics By Item Operation And Day[CU (s)]': 'CU',
        'Metrics By Item Operation And Day[Duration (s)]': 'Duration',
        'Metrics By Item Operation And Day[Operations]': 'Operations',
        'Metrics By Item Operation And Day[Users]': 'Users',
        'Metrics By Item Operation And Day[Percentile duration (ms) 50]': 'Percentile_duration_ms_50',
        'Metrics By Item Operation And Day[Percentile duration (ms) 90]': 'Percentile_duration_ms_90',
        'Metrics By Item Operation And Day[Avg duration (ms)]': 'Avg_duration_ms',
        'Metrics By Item Operation And Day[Capacity Id]': 'Capacity_Id',
        'Metrics By Item Operation And Day[Throttling (min)]': 'Throttling_min',
        'Metrics By Item Operation And Day[Failed operations]': 'Failed_operations',
        'Metrics By Item Operation And Day[Rejected operations]': 'Rejected_operations',
        'Metrics By Item Operation And Day[Successful operations]': 'Successful_operations',
        'Metrics By Item Operation And Day[Inprogress operations]': 'Inprogress_operations',
        'Metrics By Item Operation And Day[Cancelled operations]': 'Cancelled_operations',
        'Metrics By Item Operation And Day[Invalid operations]': 'Invalid_operations',
        'Metrics By Item Operation And Day[Workspace Id]': 'Workspace_Id',
        'Metrics By Item Operation And Day[Unique key]': 'Unique_key'
    })


def execute_dax_query_timepoint_utilization(token, dataset_id):
    dax_query = {
        "queries": [{
            "query": """
                DEFINE
                VAR __DS0Core = 
                    SUMMARIZECOLUMNS(
                        'Timepoints'[Time-point], 
                        'Timepoints'[Date],
                        'Capacities'[capacity Id],
                        "Background_CU", 'All Measures'[Background billable CU],
                        "Interactive_CU", 'All Measures'[Interactive billable CU]
                    )
                EVALUATE
                    __DS0Core
            """
        }],
        "serializerSettings": {"includeNulls": True}
    }
    df = execute_dax_query(token, dataset_id, dax_query)
    return df.rename(columns={
        'Timepoints[Time-point]': 'Time_point',
        'Timepoints[Date]': 'Date',
        'Capacities[capacity Id]': 'Capacity_Id',
        '[Background_CU]': 'Background_CU',
        '[Interactive_CU]': 'Interactive_CU'
    })

def execute_dax_query_timepoint_detail_utilization(token, dataset_id):
    start_date = (dt.date.today() - dt.timedelta(days=14)).strftime('%Y-%m-%d')
    end_date = dt.date.today().strftime('%Y-%m-%d')
    dates_list = fn_iterate_dates(start_date, end_date)

    all_data = pd.DataFrame()

    for d in dates_list:
        dax_query = {
            "queries": [
                {
                    "query": f"""
                        DEFINE
                        MPARAMETER 'TimePoint' = 
                            (DATE({d['year']}, {d['month']}, {d['day']}) + TIME(23, 59, 0))

                        VAR __DS0FilterTable = 
                            FILTER(
                                KEEPFILTERS(VALUES('Timepoints'[Timepoint])),
                                'Timepoints'[Timepoint] = (DATE({d['year']}, {d['month']}, {d['day']}) + TIME(23, 59, 0))
                            )

                        VAR __DS0Core = 
                            SUMMARIZECOLUMNS(
                                'Timepoint Background Detail'[Capacity Id],
                                'Items'[Workspace Id],
                                'Items'[Item Id],
                                'Timepoint Background Detail'[Operation],
                                'Timepoint Background Detail'[Start],
                                __DS0FilterTable,
                                "Total_CUs", CALCULATE(SUM('Timepoint Background Detail'[Total CU (s)])),
                                "Timepoint_CUs", CALCULATE(SUM('Timepoint Background Detail'[Timepoint CU (s)]))
                            )

                        EVALUATE
                            __DS0Core
                    """
                }
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }

        df = execute_dax_query(token, dataset_id, dax_query)
        if not df.empty:
            df["Query_Date"] = d["date"]
            all_data = pd.concat([all_data, df], ignore_index=True)

    if not all_data.empty:
        all_data = all_data.rename(columns={
            "Timepoint Background Detail[Capacity Id]": 'Capacity_Id',
            "Items[Workspace Id]": 'Workspace_Id',
            "Items[Item Id]": 'Item_Id',
            "Timepoint Background Detail[Operation]": 'Operation',
            "Timepoint Background Detail[Start]": 'Start_Date',
            "[Total_CUs]": 'Total_CUs',
            "[Timepoint_CUs]": 'Timepoint_CUs',
        })

    return all_data

# =============================
# APP STREAMLIT
# =============================
st.set_page_config(page_title="Fabric Metrics Extractor", layout="wide")
st.title("📊 Fabric Metrics Extractor")

# Variáveis no session_state para guardar seleções
if "token" not in st.session_state:
    st.session_state.token = None
if "workspaces" not in st.session_state:
    st.session_state.workspaces = []
if "selected_workspace" not in st.session_state:
    st.session_state.selected_workspace = None
if "items" not in st.session_state:
    st.session_state.items = []
if "selected_item" not in st.session_state:
    st.session_state.selected_item = None

tenant_id = st.text_input("Tenant ID", value="")
client_id = st.text_input("Client ID", value="")
client_secret = st.text_input("Client Secret", type="password", value="")

folder_path = st.text_input("📁 Informe a pasta destino para salvar os arquivos CSV:", value="")

if st.button("🔑 Conectar e listar Workspaces"):
    try:
        token = get_token(tenant_id, client_id, client_secret)
        st.session_state.token = token
        st.session_state.workspaces = list_workspaces(token)
        st.session_state.selected_workspace = None
        st.session_state.items = []
        st.session_state.selected_item = None
        st.success("Conectado com sucesso!")
    except Exception as e:
        st.error(f"Erro ao obter token: {e}")

if st.session_state.token:
    workspaces = st.session_state.workspaces
    workspace_names = {ws["name"]: ws["id"] for ws in workspaces}

    selected_workspace_name = st.selectbox(
        "Escolha o Workspace",
        options=[""] + list(workspace_names.keys()),
        index=0 if st.session_state.selected_workspace is None else ([""] + list(workspace_names.keys())).index(st.session_state.selected_workspace),
    )

    if selected_workspace_name != "":
        if (not st.session_state.items) or (st.session_state.selected_workspace != selected_workspace_name):
            workspace_id = workspace_names[selected_workspace_name]
            st.session_state.items = list_items(st.session_state.token, workspace_id)
            st.session_state.selected_item = None
            st.session_state.selected_workspace = selected_workspace_name
        else:
            workspace_id = workspace_names[st.session_state.selected_workspace]

        items = st.session_state.get("items", [])
        if not isinstance(items, list):
            st.warning("Erro: 'items' não é uma lista. Corrigindo...")
            items = []
            st.session_state.items = items

        items = st.session_state.get("items",[])
        if not isinstance(items, list):
            st.warning("Erro: 'items' não é uma lista, resetando.")
            items = []
            st.session_state.items = items
        item_names = {item["name"]: item for item in items}

        selected_item_name = st.selectbox(
            "Escolha o item",
            options=[""] + list(item_names.keys()),
            index=0 if st.session_state.selected_item is None else ([""] + list(item_names.keys())).index(st.session_state.selected_item),
        )
        if selected_item_name != "":
            st.session_state.selected_item = selected_item_name
            item_selected = item_names[selected_item_name]

            if item_selected["type"] == "Dataset":
                if st.button("▶️ Executar consultas DAX"):
                    try:
                        df_itens = execute_dax_query_itens(st.session_state.token, item_selected['id'])
                        df_sku = execute_dax_query_sku(st.session_state.token, item_selected['id'])

                        df_itens_utilization = execute_dax_query_itens_utilization(st.session_state.token, item_selected['id'])
                        df_timepoint_utilization = execute_dax_query_timepoint_utilization(st.session_state.token, item_selected['id'])
                        df_timepoint_detail_utilization = execute_dax_query_timepoint_detail_utilization(st.session_state.token, item_selected['id'])

                        # Salva arquivos CSV na pasta indicada
                        if folder_path:
                            try:
                                os.makedirs(folder_path, exist_ok=True)
                                path_itens = os.path.join(folder_path, "capacities_metrics_itens.csv")
                                df_itens.to_csv(path_itens, index=False, encoding="utf-8-sig")

                                path_sku = os.path.join(folder_path, "capacities_metrics_sku.csv")
                                df_sku.to_csv(path_sku, index=False, encoding="utf-8-sig")

                                path_itens_utilization = os.path.join(folder_path, "capacities_metrics_itens_utilization.csv")
                                df_itens_utilization.to_csv(path_itens_utilization, index=False, encoding="utf-8-sig")

                                path_timepoint_utilization = os.path.join(folder_path, "capacities_metrics_timepoint_utilization.csv")
                                df_timepoint_utilization.to_csv(path_timepoint_utilization, index=False, encoding="utf-8-sig")

                                path_timepoint_detail_utilization = os.path.join(folder_path, "capacities_metrics_timepoint_detail_utilization.csv")
                                df_timepoint_detail_utilization.to_csv(path_timepoint_detail_utilization, index=False, encoding="utf-8-sig")

                                st.success(f"Arquivos salvos em: {folder_path}")
                            except Exception as e:
                                st.error(f"Erro ao salvar arquivos na pasta destino: {e}")
                        else:
                            st.warning("Informe uma pasta destino para salvar os arquivos CSV.")

                        # Mostrar tabelas e permitir download via navegador também
                        st.write("### Itens")
                        st.dataframe(df_itens)
                        st.download_button("⬇️ Baixar Itens CSV", df_itens.to_csv(index=False, encoding="utf-8-sig"), "capacities_metrics_itens.csv")

                        st.write("### SKU")
                        st.dataframe(df_sku)
                        st.download_button("⬇️ Baixar SKU CSV", df_sku.to_csv(index=False, encoding="utf-8-sig"), "capacities_metrics_sku.csv")

                        st.write("### Itens Utilizations")
                        st.dataframe(df_itens_utilization)
                        st.download_button("⬇️ Baixar Itens Utilizations CSV", df_sku.to_csv(index=False, encoding="utf-8-sig"), "capacities_metrics_itens_utilization.csv")

                        st.write("### Timepoint Utilizations")
                        st.dataframe(df_timepoint_utilization)
                        st.download_button("⬇️ Baixar Timepoint Utilizations CSV", df_sku.to_csv(index=False, encoding="utf-8-sig"), "capacities_metrics_timepoint_utilization.csv")

                        st.write("### Timepoint Detail Utilizations")
                        st.dataframe(df_timepoint_detail_utilization)
                        st.download_button("⬇️ Baixar Timepoint Detail Utilizations CSV", df_sku.to_csv(index=False, encoding="utf-8-sig"), "capacities_metrics_timepoint_detail_utilization.csv")
                    except Exception as e:
                        st.error(f"Erro ao executar consultas DAX: {e}")
            else:
                st.warning("Selecione um Dataset para executar consultas.")
