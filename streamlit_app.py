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
st.set_page_config(page_title="Multi-Brand Performance Dashboard", page_icon="📊", layout="wide")

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
    dist_col = find_col(df_sales, ['DISTRIBUTOR NAME', 'DISTRIBUT'])
    
    col_visited = find_col(df_coverage, ['VISITED', 'VISIT'])
    col_billed = find_col(df_coverage, ['BILLED', 'BILL'])

    # Category and SKU for drill-downs
    cat_col = find_col(df_sales, ['CATEGORY', 'BRAND', 'LINE', 'PRODUCT GROUP', 'SEGMENT'])
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

    base_cols = [c for c in ['EMPLOYEE CHANNEL TYPE', emp_s, 'EMPLOYEE NAME', desig_col, 'CITY', 'STATE', 'REGION', dist_col] if c and c in df_sales.columns]
    base = tsi_sales[base_cols].drop_duplicates(subset=[emp_s])

    user_cols = [c for c in [emp_u, 'STATUS', 'DATE OF JOINING', 'LEVEL3 EMPLOYEE NAME', 'LEVEL2 EMPLOYEE NAME'] if c and c in df_user.columns]
    user_info = df_user[user_cols]
    master = pd.merge(base, user_info, left_on=emp_s, right_on=emp_u, how='left')

    df_sales[sales_val_col] = pd.to_numeric(df_sales[sales_val_col], errors='coerce').fillna(0)
    df_sales[qty_case_col] = pd.to_numeric(df_sales[qty_case_col], errors='coerce').fillna(0)
    df_coverage[col_visited] = pd.to_numeric(df_coverage[col_visited], errors='coerce').fillna(0)
    df_coverage[col_billed] = pd.to_numeric(df_coverage[col_billed], errors='coerce').fillna(0)

    l_val = df_sales.groupby(emp_s)[sales_val_col].sum().reset_index(name='L_val')
    master = master.merge(l_val, on=emp_s, how='left')
    master = master.fillna('')
    master['emp_s'] = emp_s

    return master, df_sales, df_attendance, df_coverage, df_fulfill, emp_s, emp_a, emp_cov, emp_f, sales_val_col, qty_case_col, col_visited, col_billed, ticket_s, ticket_f, price_col, signoff_col, cat_col, sku_col


# ==========================================
# 3. APP LAYOUT & SIDEBAR
# ==========================================
st.title("📈 Multi-Brand Master Performance Dashboard")
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
            master_df, df_sales, df_att, df_cov, df_ful, emp_s, emp_a, emp_cov, emp_f, sales_val_col, qty_case_col, col_visited, col_billed, ticket_s, ticket_f, price_col, signoff_col, cat_col, sku_col = process_data(f_sales, f_user, f_att, f_cov, f_cc, f_ful)

            tab1, tab2 = st.tabs(["👤 Employee Profile (Interactive)", "📊 Overall Summary Data"])

            with tab1:
                col_sel, _ = st.columns([1, 2])
                with col_sel:
                    emp_list = master_df['EMPLOYEE NAME'].unique().tolist()
                    selected_emp = st.selectbox("Search / Select Employee:", sorted([x for x in emp_list if str(x).strip() != '']))

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
                                <td><b>City/State:</b> {emp_data.get('CITY', '')}, {emp_data.get('STATE', '')}</td>
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

                        # --- VARIABLES TO CALCULATE "ALL MONTHS" TOTAL ---
                        total_md = 0
                        total_mw_billed = 0
                        total_perf_sales = 0
                        total_full_val = 0

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
                            perf_sales = m_sales[sales_val_col].sum() if sales_val_col else 0
                            lines_billed = m_sales[cat_col].nunique() if cat_col else 0

                            full_val = 0
                            if ticket_s and ticket_f and signoff_col and price_lookup is not None:
                                m_tickets = m_sales[ticket_s].dropna()
                                m_ful = df_ful[df_ful[ticket_f].isin(m_tickets)]
                                merged_f = pd.merge(m_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                                full_val = (pd.to_numeric(merged_f[price_col], errors='coerce').fillna(0) * pd.to_numeric(merged_f[signoff_col], errors='coerce').fillna(0)).sum()

                            timeline_name = f"M{m_counter} ({m})"
                            monthly_records.append({
                                "Timeline": timeline_name,
                                "Mandays (MD)": md_val,
                                "Market Working (Billed)": mw_billed,
                                "Lines Billed (Categories)": lines_billed,
                                "Performance (Sales ₹)": f"₹ {perf_sales:,.0f}",
                                "Order Fullfilment (₹)": f"₹ {full_val:,.0f}"
                            })
                            
                            drill_down_data[timeline_name] = {'m_sales': m_sales, 'md_val': md_val}
                            
                            # Add to totals
                            total_md += md_val
                            total_mw_billed += mw_billed
                            total_perf_sales += perf_sales
                            total_full_val += full_val
                            m_counter += 1

                        # --- CREATE "ALL MONTHS" RECORD ---
                        total_lines_billed = emp_sales[cat_col].nunique() if cat_col else 0
                        monthly_records.insert(0, {
                            "Timeline": "Total / All Months",
                            "Mandays (MD)": total_md,
                            "Market Working (Billed)": total_mw_billed,
                            "Lines Billed (Categories)": total_lines_billed,
                            "Performance (Sales ₹)": f"₹ {total_perf_sales:,.0f}",
                            "Order Fullfilment (₹)": f"₹ {total_full_val:,.0f}"
                        })
                        drill_down_data["Total / All Months"] = {'m_sales': emp_sales, 'md_val': total_md}

                        # Render Level 1 Table
                        df_trend = pd.DataFrame(monthly_records)
                        selected_timeline = None
                        
                        try:
                            # Streamlit >= 1.35 Clickable Dataframe
                            event_1 = st.dataframe(df_trend, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
                            if len(event_1.selection.rows) > 0:
                                selected_timeline = df_trend.iloc[event_1.selection.rows[0]]['Timeline']
                        except TypeError:
                            # Fallback
                            st.dataframe(df_trend, use_container_width=True, hide_index=True)
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
                                grouping_col = cat_col if cat_col else (sku_col if sku_col else None)
                                
                                if grouping_col:
                                    detail_list = []
                                    grouped = m_sales.groupby(grouping_col)
                                    
                                    for name, group in grouped:
                                        types_of_products = group[sku_col].nunique() if sku_col else len(group)
                                        stores_billed = group[ticket_s].nunique() if ticket_s else 0
                                        cat_sales_val = group[sales_val_col].sum() if sales_val_col else 0
                                        cat_qty = group[qty_case_col].sum() if qty_case_col else 0
                                        
                                        cat_full_val = 0
                                        if ticket_s and ticket_f and signoff_col and price_lookup is not None:
                                            cat_tickets = group[ticket_s].dropna()
                                            cat_ful = df_ful[df_ful[ticket_f].isin(cat_tickets)]
                                            if not cat_ful.empty:
                                                c_merged = pd.merge(cat_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                                                cat_full_val = (pd.to_numeric(c_merged[price_col], errors='coerce').fillna(0) * pd.to_numeric(c_merged[signoff_col], errors='coerce').fillna(0)).sum()

                                        detail_list.append({
                                            "Category (Line)": name,
                                            "Billed Stores": stores_billed,
                                            "Types of Products Sold": types_of_products,
                                            "Total Sales Value": f"₹ {cat_sales_val:,.0f}",
                                            "Qty Sold": f"{cat_qty:,.1f}",
                                            "Order Fulfillment": f"₹ {cat_full_val:,.0f}"
                                        })
                                    
                                    df_detail = pd.DataFrame(detail_list)
                                    selected_category = None
                                    
                                    try:
                                        # Render Clickable Category Table
                                        event_2 = st.dataframe(df_detail, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row")
                                        if len(event_2.selection.rows) > 0:
                                            selected_category = df_detail.iloc[event_2.selection.rows[0]]['Category (Line)']
                                    except TypeError:
                                        st.dataframe(df_detail, use_container_width=True, hide_index=True)
                                        selected_category = st.selectbox("Select Category:", df_detail['Category (Line)'].tolist())

                                    # ==========================================
                                    # LEVEL 3: DRILL-DOWN PRODUCT/SKU TABLE
                                    # ==========================================
                                    if selected_category:
                                        st.markdown(f"<div class='drilldown-header-sku'>📦 Product Breakdown for: {selected_category} ({selected_timeline})</div>", unsafe_allow_html=True)
                                        
                                        # Filter sales for just this category
                                        cat_sales = m_sales[m_sales[grouping_col] == selected_category]
                                        
                                        product_list = []
                                        if sku_col:
                                            sku_grouped = cat_sales.groupby(sku_col)
                                            
                                            for sku_name, s_group in sku_grouped:
                                                sku_billed = s_group[ticket_s].nunique() if ticket_s else 0
                                                sku_sales_val = s_group[sales_val_col].sum() if sales_val_col else 0
                                                sku_qty = s_group[qty_case_col].sum() if qty_case_col else 0
                                                
                                                sku_full_val = 0
                                                if ticket_s and ticket_f and signoff_col and price_lookup is not None:
                                                    sku_tickets = s_group[ticket_s].dropna()
                                                    sku_ful = df_ful[df_ful[ticket_f].isin(sku_tickets)]
                                                    if not sku_ful.empty:
                                                        s_merged = pd.merge(sku_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                                                        sku_full_val = (pd.to_numeric(s_merged[price_col], errors='coerce').fillna(0) * pd.to_numeric(s_merged[signoff_col], errors='coerce').fillna(0)).sum()
                                                
                                                product_list.append({
                                                    "Timeline": selected_timeline,
                                                    "Category": selected_category,
                                                    "Product Name": sku_name,
                                                    "Billed Stores": sku_billed,
                                                    "Qty Sold": f"{sku_qty:,.1f}",
                                                    "Total Sales Value": f"₹ {sku_sales_val:,.0f}",
                                                    "Order Fulfillment": f"₹ {sku_full_val:,.0f}"
                                                })
                                            
                                            df_product = pd.DataFrame(product_list)
                                            st.dataframe(df_product, use_container_width=True, hide_index=True)
                                        else:
                                            st.warning("No Product/SKU column found to generate this view.")

                                else:
                                    st.warning("Could not find a 'Category' column to create the breakdown.")

            with tab2:
                st.subheader("All Employees Dataset")
                display_df = master_df.copy()
                st.dataframe(display_df, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error("🚨 Data Processing Error")
            st.warning(str(e))

else:
    st.info("👈 Please upload all 6 required Excel reports in the sidebar to generate the dashboard.")