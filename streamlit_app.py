import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import io
import xlsxwriter
import plotly.express as px

# ==========================================
# 1. STREAMLIT PAGE CONFIGURATION
# ==========================================
st.set_page_config(page_title="Multi-Brand Team Performance Dashboard", page_icon="📊", layout="wide")

st.markdown("""
    <style>
    div[data-testid="metric-container"] {
        background-color: #f8f9fa; border: 1px solid #e9ecef;
        padding: 5% 5% 5% 10%; border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    .profile-card {
        background-color: #f0f2f6; padding: 20px;
        border-radius: 10px; margin-bottom: 20px;
        border-left: 5px solid #4CAF50;
    }
    .drilldown-header {
        color: #2c3e50; font-size: 1.1em; margin-top: 15px; margin-bottom: 10px; font-weight: bold;
        padding-left: 10px; border-left: 4px solid #3498db;
    }
    .drilldown-header-sku {
        color: #8e44ad; font-size: 1.1em; margin-top: 15px; margin-bottom: 10px; font-weight: bold;
        padding-left: 10px; border-left: 4px solid #9b59b6;
    }
    /* Force right alignment as a fallback */
    .stDataFrame [data-testid="stTable"] td:not(:first-child) {
        text-align: right !important;
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def find_col(df, keywords):
    for col in df.columns:
        if any(k.upper() in str(col).upper() for k in keywords):
            return col
    return None

def find_date_col(df):
    exact_matches = ['DATE', 'INVOICE DATE', 'ATTENDANCE DATE', 'CREATED ON', 'VISIT DATE', 'ORDER DATE']
    for ext in exact_matches:
        for col in df.columns:
            if ext == str(col).upper().strip(): return col
    for col in df.columns:
        if 'DATE' in str(col).upper(): return col
    return None

def parse_dates_safely(df, col_name):
    if col_name and col_name in df.columns:
        dt_series = pd.to_datetime(df[col_name], errors='coerce', dayfirst=True)
        dt_series = dt_series.where((dt_series.dt.year >= 2000) & (dt_series.dt.year <= 2030), pd.NaT)
        df['Month'] = dt_series.dt.to_period('M').astype(str)
        df['Month'] = df['Month'].replace('NaT', np.nan)
    return df

@st.cache_data
def process_data(sales_file, user_file, att_file, cov_file, cc_file, ful_file):
    df_sales = pd.read_excel(sales_file)
    df_user = pd.read_excel(user_file)
    df_attendance = pd.read_excel(att_file)
    df_coverage = pd.read_excel(cov_file)
    df_call_cycle = pd.read_excel(cc_file)
    df_fulfill = pd.read_excel(ful_file)

    for df in [df_sales, df_attendance, df_coverage, df_call_cycle, df_user, df_fulfill]:
        df.columns = df.columns.astype(str).str.strip()

    id_keys = ['EMPLOYEE CODE', 'EMP CODE', 'EMPLOYE I', 'EMP ID']
    emp_s = find_col(df_sales, id_keys)
    emp_u = find_col(df_user, id_keys)
    emp_a = find_col(df_attendance, id_keys)
    emp_cov = find_col(df_coverage, id_keys)
    emp_f = find_col(df_fulfill, id_keys)

    ticket_s = find_col(df_sales, ['TICKET NO', 'TICKET NC', 'TICKET_NO', 'INVOICE NO'])
    ticket_f = find_col(df_fulfill, ['TICKET NO', 'TICKET NC', 'TICKET_NO', 'INVOICE NO'])
    price_col = find_col(df_sales, ['SALE PRICE', 'SALE PRIC', 'PRICE']) 
    signoff_col = find_col(df_fulfill, ['SIGNOFF QTY', 'SIGN OFF', 'QTY'])
    sales_val_col = find_col(df_sales, ['TOTAL SALES VALUE', 'TOTAL SAL', 'SALES VALUE', 'VALUE'])
    qty_case_col = find_col(df_sales, ['QTY IN CASE', 'CASE QTY', 'CASES', 'QUANTITY'])
    
    col_visited = find_col(df_coverage, ['VISITED', 'VISIT'])
    col_billed = find_col(df_coverage, ['BILLED', 'BILL'])

    # Separation of Brand vs Category
    brand_col = find_col(df_sales, ['BRAND'])
    cat_col = find_col(df_sales, ['CATEGORY', 'SEGMENT', 'PRODUCT GROUP', 'LINE'])
    sku_col = find_col(df_sales, ['SKU', 'PRODUCT NAME', 'ITEM NAME', 'DESCRIPTION', 'MATERIAL'])

    date_sales = find_date_col(df_sales)
    date_att = find_date_col(df_attendance)
    date_cov = find_date_col(df_coverage)

    df_sales = parse_dates_safely(df_sales, date_sales)
    df_attendance = parse_dates_safely(df_attendance, date_att)
    df_coverage = parse_dates_safely(df_coverage, date_cov)

    desig_col = find_col(df_sales, ['DESIGNATION'])
    if desig_col:
        tsi_sales = df_sales[df_sales[desig_col].astype(str).str.contains('TERRITORY SALES INCHARGE', na=False, case=False)].copy()
    else:
        tsi_sales = df_sales.copy()

    # Merge EVERYTHING first to ensure we don't lose Region/State/City
    base = tsi_sales.drop_duplicates(subset=[emp_s])
    master = pd.merge(base, df_user, left_on=emp_s, right_on=emp_u, how='left')

    # Now intelligently find geographic columns across the newly merged master data
    reg_col = find_col(master, ['REGION', 'ZONE'])
    state_col = find_col(master, ['STATE', 'PROVINCE'])
    city_col = find_col(master, ['CITY', 'TOWN', 'LOCATION'])

    if not reg_col: master['REGION'] = "N/A"; reg_col = 'REGION'
    if not state_col: master['STATE'] = "N/A"; state_col = 'STATE'
    if not city_col: master['CITY'] = "N/A"; city_col = 'CITY'

    df_sales[sales_val_col] = pd.to_numeric(df_sales[sales_val_col], errors='coerce').fillna(0)
    df_sales[qty_case_col] = pd.to_numeric(df_sales[qty_case_col], errors='coerce').fillna(0)
    df_coverage[col_visited] = pd.to_numeric(df_coverage[col_visited], errors='coerce').fillna(0)
    df_coverage[col_billed] = pd.to_numeric(df_coverage[col_billed], errors='coerce').fillna(0)

    l_val = df_sales.groupby(emp_s)[sales_val_col].sum().reset_index(name='L_val')
    master = master.merge(l_val, on=emp_s, how='left')
    master = master.fillna('')
    master['emp_s'] = emp_s

    return master, df_sales, df_attendance, df_coverage, df_fulfill, emp_s, emp_a, emp_cov, emp_f, sales_val_col, qty_case_col, col_visited, col_billed, ticket_s, ticket_f, price_col, signoff_col, cat_col, brand_col, sku_col, reg_col, state_col, city_col


# ==========================================
# 3. APP LAYOUT & SIDEBAR
# ==========================================
st.title("📈 Multi-Brand Team Performance Dashboard")
st.sidebar.header("📂 Data Upload")

f_sales = st.sidebar.file_uploader("1. Sales Report", type=['xlsx'])
f_user = st.sidebar.file_uploader("2. User Master", type=['xlsx'])
f_att = st.sidebar.file_uploader("3. Daily Attendance", type=['xlsx'])
f_cov = st.sidebar.file_uploader("4. Coverage", type=['xlsx'])
f_cc = st.sidebar.file_uploader("5. Call Cycle", type=['xlsx'])
f_ful = st.sidebar.file_uploader("6. Order Fulfillment", type=['xlsx'])

if all([f_sales, f_user, f_att, f_cov, f_cc, f_ful]):
    with st.spinner("Processing data, please wait..."):
        try:
            master_df, df_sales, df_att, df_cov, df_ful, emp_s, emp_a, emp_cov, emp_f, sales_val_col, qty_case_col, col_visited, col_billed, ticket_s, ticket_f, price_col, signoff_col, cat_col, brand_col, sku_col, reg_col, state_col, city_col = process_data(f_sales, f_user, f_att, f_cov, f_cc, f_ful)

            # Common Number Configs
            currency_format = st.column_config.NumberColumn(format="₹ %,.0f")
            qty_format = st.column_config.NumberColumn(format="%,.1f")
            int_format = st.column_config.NumberColumn(format="%d")

            # ==========================================
            # CASCADING FILTERS 
            # ==========================================
            st.markdown("### 👤 Employee Profile (Interactive)")
            f_col1, f_col2, f_col3, f_col4 = st.columns(4)
            
            with f_col1:
                regions = ["All"] + sorted([str(x) for x in master_df[reg_col].unique() if str(x).strip() not in ['', 'nan', 'N/A']])
                sel_region = st.selectbox("🌍 Filter Region:", regions)
            filtered_df = master_df if sel_region == "All" else master_df[master_df[reg_col].astype(str) == sel_region]

            with f_col2:
                states = ["All"] + sorted([str(x) for x in filtered_df[state_col].unique() if str(x).strip() not in ['', 'nan', 'N/A']])
                sel_state = st.selectbox("📍 Filter State:", states)
            filtered_df = filtered_df if sel_state == "All" else filtered_df[filtered_df[state_col].astype(str) == sel_state]

            with f_col3:
                cities = ["All"] + sorted([str(x) for x in filtered_df[city_col].unique() if str(x).strip() not in ['', 'nan', 'N/A']])
                sel_city = st.selectbox("🏢 Filter City:", cities)
            filtered_df = filtered_df if sel_city == "All" else filtered_df[filtered_df[city_col].astype(str) == sel_city]

            with f_col4:
                emp_list = filtered_df['EMPLOYEE NAME'].unique().tolist()
                selected_emp = st.selectbox("👤 Select Employee:", sorted([x for x in emp_list if str(x).strip() != '']))

            if selected_emp:
                emp_data = master_df[master_df['EMPLOYEE NAME'] == selected_emp].iloc[0]
                emp_id_val = emp_data[emp_s]

                st.markdown(f"""
                <div class="profile-card">
                    <h3 style='margin-top:0px;'>About Employee</h3>
                    <table style='width:100%; border:none; text-align:left;'>
                        <tr>
                            <td><b>Employee Name:</b> {emp_data.get('EMPLOYEE NAME', 'N/A')}</td>
                            <td><b>Employee Code:</b> {emp_id_val}</td>
                            <td><b>Designation:</b> {emp_data.get('DESIGNATION', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td><b>Date of Joining:</b> {str(emp_data.get('DATE OF JOINING', 'N/A'))[:10]}</td>
                            <td><b>Supervisor Name:</b> {emp_data.get('LEVEL2 EMPLOYEE NAME', 'N/A')}</td>
                            <td><b>Status:</b> {emp_data.get('STATUS', 'N/A')}</td>
                        </tr>
                        <tr>
                            <td><b>City/State:</b> {emp_data.get(city_col, '')}, {emp_data.get(state_col, '')}</td>
                            <td><b>Channel:</b> {emp_data.get('EMPLOYEE CHANNEL TYPE', 'N/A')}</td>
                            <td><b>SS Name:</b> {emp_data.get('LEVEL3 EMPLOYEE NAME', 'N/A')}</td>
                        </tr>
                    </table>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("### 📅 High-Level Monthly Overview")
                st.info("👆 **Level 1 Drill-Down:** Click a month (or 'Total / All Months') to view its Categories.")
                
                emp_sales = df_sales[df_sales[emp_s] == emp_id_val]
                emp_att = df_att[df_att[emp_a] == emp_id_val] if 'Month' in df_att.columns else pd.DataFrame()
                emp_coverage = df_cov[df_cov[emp_cov] == emp_id_val] if 'Month' in df_cov.columns else pd.DataFrame()

                months = set()
                if 'Month' in emp_sales.columns: months.update(emp_sales['Month'].dropna().unique())
                if 'Month' in emp_att.columns: months.update(emp_att['Month'].dropna().unique())
                if 'Month' in emp_coverage.columns: months.update(emp_coverage['Month'].dropna().unique())
                months = sorted([m for m in months if m != 'nan' and pd.notnull(m)])

                if not months:
                    st.warning("No valid date data found for this employee.")
                else:
                    monthly_records = []
                    drill_down_data = {} 
                    m_counter = 1
                    
                    price_lookup = df_sales[[ticket_s, price_col]].drop_duplicates(subset=[ticket_s]) if ticket_s else None

                    total_md = 0
                    total_mw_billed = 0
                    total_perf_sales = 0.0
                    total_full_val = 0.0

                    for m in months:
                        md_val = 0
                        if 'Month' in emp_att.columns:
                            att_col = find_col(emp_att, ['ATTENDANCE', 'STATUS'])
                            if att_col: md_val = len(emp_att[(emp_att['Month'] == m) & (emp_att[att_col].astype(str).str.upper() == 'PRESENT')])

                        mw_billed = 0
                        if 'Month' in emp_coverage.columns:
                            m_cov = emp_coverage[emp_coverage['Month'] == m]
                            mw_billed = m_cov[col_billed].sum() if col_billed else 0

                        m_sales = emp_sales[emp_sales['Month'] == m]
                        
                        # New Advanced Metrics
                        total_bills = int(m_sales[ticket_s].nunique()) if ticket_s else 0
                        total_lines = int(len(m_sales))
                        avg_line_billed = float(total_lines / total_bills) if total_bills > 0 else 0.0
                        brands_sold = int(m_sales[brand_col].nunique()) if brand_col else 0
                        cats_sold = int(m_sales[cat_col].nunique()) if cat_col else 0

                        perf_sales = float(m_sales[sales_val_col].sum()) if sales_val_col else 0.0

                        full_val = 0.0
                        if ticket_s and ticket_f and signoff_col and price_lookup is not None:
                            m_tickets = m_sales[ticket_s].dropna()
                            m_ful = df_ful[df_ful[ticket_f].isin(m_tickets)]
                            merged_f = pd.merge(m_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                            full_val = float((pd.to_numeric(merged_f[price_col], errors='coerce').fillna(0) * pd.to_numeric(merged_f[signoff_col], errors='coerce').fillna(0)).sum())

                        timeline_name = f"M{m_counter} ({m})"
                        monthly_records.append({
                            "Timeline": timeline_name,
                            "Mandays (MD)": md_val,
                            "Market Working (Billed)": mw_billed,
                            "Total Bills (Invoices)": total_bills,
                            "Total Lines Sold": total_lines,
                            "Avg Line Billed": avg_line_billed,
                            "Brands Sold": brands_sold,
                            "Categories Sold": cats_sold,
                            "Performance (Sales ₹)": perf_sales,
                            "Order Fullfilment (₹)": full_val
                        })
                        
                        drill_down_data[timeline_name] = {'m_sales': m_sales, 'md_val': md_val}
                        
                        total_md += md_val
                        total_mw_billed += mw_billed
                        total_perf_sales += perf_sales
                        total_full_val += full_val
                        m_counter += 1

                    # TOTAL / ALL MONTHS
                    total_bills_all = int(emp_sales[ticket_s].nunique()) if ticket_s else 0
                    total_lines_all = int(len(emp_sales))
                    avg_line_all = float(total_lines_all / total_bills_all) if total_bills_all > 0 else 0.0
                    total_brands_all = int(emp_sales[brand_col].nunique()) if brand_col else 0
                    total_cats_all = int(emp_sales[cat_col].nunique()) if cat_col else 0

                    monthly_records.insert(0, {
                        "Timeline": "Total / All Months",
                        "Mandays (MD)": total_md,
                        "Market Working (Billed)": total_mw_billed,
                        "Total Bills (Invoices)": total_bills_all,
                        "Total Lines Sold": total_lines_all,
                        "Avg Line Billed": avg_line_all,
                        "Brands Sold": total_brands_all,
                        "Categories Sold": total_cats_all,
                        "Performance (Sales ₹)": total_perf_sales,
                        "Order Fullfilment (₹)": total_full_val
                    })
                    drill_down_data["Total / All Months"] = {'m_sales': emp_sales, 'md_val': total_md}

                    # Render Level 1 Table
                    df_trend = pd.DataFrame(monthly_records)
                    
                    # FORCE PANDAS DTYPES
                    df_trend["Mandays (MD)"] = pd.to_numeric(df_trend["Mandays (MD)"])
                    df_trend["Market Working (Billed)"] = pd.to_numeric(df_trend["Market Working (Billed)"])
                    df_trend["Total Bills (Invoices)"] = pd.to_numeric(df_trend["Total Bills (Invoices)"])
                    df_trend["Total Lines Sold"] = pd.to_numeric(df_trend["Total Lines Sold"])
                    df_trend["Avg Line Billed"] = pd.to_numeric(df_trend["Avg Line Billed"])
                    df_trend["Brands Sold"] = pd.to_numeric(df_trend["Brands Sold"])
                    df_trend["Categories Sold"] = pd.to_numeric(df_trend["Categories Sold"])
                    df_trend["Performance (Sales ₹)"] = pd.to_numeric(df_trend["Performance (Sales ₹)"])
                    df_trend["Order Fullfilment (₹)"] = pd.to_numeric(df_trend["Order Fullfilment (₹)"])
                    
                    col_configs_L1 = {
                        "Mandays (MD)": int_format,
                        "Market Working (Billed)": int_format,
                        "Total Bills (Invoices)": int_format,
                        "Total Lines Sold": int_format,
                        "Avg Line Billed": st.column_config.NumberColumn(format="%.2f"),
                        "Brands Sold": int_format,
                        "Categories Sold": int_format,
                        "Performance (Sales ₹)": currency_format,
                        "Order Fullfilment (₹)": currency_format
                    }

                    selected_timeline = None
                    try:
                        event_1 = st.dataframe(df_trend, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", column_config=col_configs_L1)
                        if len(event_1.selection.rows) > 0:
                            selected_timeline = df_trend.iloc[event_1.selection.rows[0]]['Timeline']
                    except TypeError:
                        st.dataframe(df_trend, use_container_width=True, hide_index=True, column_config=col_configs_L1)
                        selected_timeline = st.selectbox("Select Timeline:", df_trend['Timeline'].tolist())

                    # ==========================================
                    # LEVEL 2: DRILL-DOWN CATEGORY TABLE
                    # ==========================================
                    if selected_timeline:
                        st.markdown(f"<div class='drilldown-header'>🔽 Category Breakdown for {selected_timeline}</div>", unsafe_allow_html=True)
                        st.info("👆 **Level 2 Drill-Down:** Click on any Category below to see the individual Products/SKUs.")
                        
                        data = drill_down_data[selected_timeline]
                        m_sales = data['m_sales']
                        
                        if m_sales.empty:
                            st.warning("No sales data available.")
                        else:
                            grouping_col = cat_col if cat_col else (brand_col if brand_col else (sku_col if sku_col else None))
                            
                            if grouping_col:
                                detail_list = []
                                grouped = m_sales.groupby(grouping_col)
                                
                                for name, group in grouped:
                                    types_of_products = int(group[sku_col].nunique()) if sku_col else len(group)
                                    stores_billed = int(group[ticket_s].nunique()) if ticket_s else 0
                                    cat_sales_val = float(group[sales_val_col].sum()) if sales_val_col else 0.0
                                    cat_qty = float(group[qty_case_col].sum()) if qty_case_col else 0.0
                                    
                                    cat_full_val = 0.0
                                    if ticket_s and ticket_f and signoff_col and price_lookup is not None:
                                        cat_tickets = group[ticket_s].dropna()
                                        cat_ful = df_ful[df_ful[ticket_f].isin(cat_tickets)]
                                        if not cat_ful.empty:
                                            c_merged = pd.merge(cat_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                                            cat_full_val = float((pd.to_numeric(c_merged[price_col], errors='coerce').fillna(0) * pd.to_numeric(c_merged[signoff_col], errors='coerce').fillna(0)).sum())

                                    detail_list.append({
                                        "Category / Brand": name,
                                        "Billed Stores": int(stores_billed),
                                        "Types of Products Sold": int(types_of_products),
                                        "Total Sales Value": float(cat_sales_val),
                                        "Qty Sold": float(cat_qty),
                                        "Order Fulfillment": float(cat_full_val)
                                    })
                                
                                df_detail = pd.DataFrame(detail_list)
                                
                                df_detail["Billed Stores"] = pd.to_numeric(df_detail["Billed Stores"])
                                df_detail["Types of Products Sold"] = pd.to_numeric(df_detail["Types of Products Sold"])
                                df_detail["Total Sales Value"] = pd.to_numeric(df_detail["Total Sales Value"])
                                df_detail["Qty Sold"] = pd.to_numeric(df_detail["Qty Sold"])
                                df_detail["Order Fulfillment"] = pd.to_numeric(df_detail["Order Fulfillment"])
                                
                                col_configs_L2 = {
                                    "Billed Stores": int_format,
                                    "Types of Products Sold": int_format,
                                    "Total Sales Value": currency_format,
                                    "Qty Sold": qty_format,
                                    "Order Fulfillment": currency_format
                                }

                                selected_category = None
                                try:
                                    event_2 = st.dataframe(df_detail, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row", column_config=col_configs_L2)
                                    if len(event_2.selection.rows) > 0:
                                        selected_category = df_detail.iloc[event_2.selection.rows[0]]['Category / Brand']
                                except TypeError:
                                    st.dataframe(df_detail, use_container_width=True, hide_index=True, column_config=col_configs_L2)
                                    selected_category = st.selectbox("Select Category:", df_detail['Category / Brand'].tolist())

                                # ==========================================
                                # LEVEL 3: DRILL-DOWN PRODUCT/SKU TABLE
                                # ==========================================
                                if selected_category:
                                    st.markdown(f"<div class='drilldown-header-sku'>📦 Product Breakdown for: {selected_category} ({selected_timeline})</div>", unsafe_allow_html=True)
                                    
                                    cat_sales = m_sales[m_sales[grouping_col] == selected_category]
                                    
                                    product_list = []
                                    if sku_col:
                                        sku_grouped = cat_sales.groupby(sku_col)
                                        
                                        for sku_name, s_group in sku_grouped:
                                            sku_billed = int(s_group[ticket_s].nunique()) if ticket_s else 0
                                            sku_sales_val = float(s_group[sales_val_col].sum()) if sales_val_col else 0.0
                                            sku_qty = float(s_group[qty_case_col].sum()) if qty_case_col else 0.0
                                            
                                            sku_full_val = 0.0
                                            if ticket_s and ticket_f and signoff_col and price_lookup is not None:
                                                sku_tickets = s_group[ticket_s].dropna()
                                                sku_ful = df_ful[df_ful[ticket_f].isin(sku_tickets)]
                                                if not sku_ful.empty:
                                                    s_merged = pd.merge(sku_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                                                    sku_full_val = float((pd.to_numeric(s_merged[price_col], errors='coerce').fillna(0) * pd.to_numeric(s_merged[signoff_col], errors='coerce').fillna(0)).sum())
                                            
                                            product_list.append({
                                                "Product Name": sku_name,
                                                "Billed Stores": int(sku_billed),
                                                "Qty Sold": float(sku_qty),
                                                "Total Sales Value": float(sku_sales_val),
                                                "Order Fulfillment": float(sku_full_val)
                                            })
                                        
                                        df_product = pd.DataFrame(product_list)
                                        
                                        df_product["Billed Stores"] = pd.to_numeric(df_product["Billed Stores"])
                                        df_product["Qty Sold"] = pd.to_numeric(df_product["Qty Sold"])
                                        df_product["Total Sales Value"] = pd.to_numeric(df_product["Total Sales Value"])
                                        df_product["Order Fulfillment"] = pd.to_numeric(df_product["Order Fulfillment"])

                                        col_configs_L3 = {
                                            "Billed Stores": int_format,
                                            "Qty Sold": qty_format,
                                            "Total Sales Value": currency_format,
                                            "Order Fulfillment": currency_format
                                        }
                                        st.dataframe(df_product, use_container_width=True, hide_index=True, column_config=col_configs_L3)
                                    else:
                                        st.warning("No Product/SKU column found to generate this view.")

                            else:
                                st.warning("Could not find a 'Category' column to create the breakdown.")

        except Exception as e:
            st.error("🚨 Data Processing Error")
            st.warning(str(e))

else:
    st.info("👈 Please upload all 6 required Excel reports in the sidebar to generate the dashboard.")