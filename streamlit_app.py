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
    """Smarter date column finder to prevent picking up Invoice Numbers or IDs"""
    # 1. Look for exact known date column names first
    exact_matches = ['DATE', 'INVOICE DATE', 'ATTENDANCE DATE', 'CREATED ON', 'VISIT DATE', 'ORDER DATE']
    for ext in exact_matches:
        for col in df.columns:
            if ext == str(col).upper().strip():
                return col
                
    # 2. Fallback: Look for columns that contain the word 'DATE'
    for col in df.columns:
        if 'DATE' in str(col).upper():
            return col
    return None

def parse_dates_safely(df, col_name):
    """Robustly parses Excel dates, forces DD/MM/YYYY logic, and removes 1970 errors"""
    if col_name and col_name in df.columns:
        # Convert to datetime using dayfirst=True (handles DD/MM/YYYY standard)
        dt_series = pd.to_datetime(df[col_name], errors='coerce', dayfirst=True)
        # Drop junk years (like 1970 epoch errors or extreme future dates)
        dt_series = dt_series.where((dt_series.dt.year >= 2000) & (dt_series.dt.year <= 2030), pd.NaT)
        
        # Create a clean 'Month' column (YYYY-MM)
        df['Month'] = dt_series.dt.to_period('M').astype(str)
        df['Month'] = df['Month'].replace('NaT', np.nan) # Clean up missing text
    return df

def dv(n, d): 
    return (n / d) if (pd.notnull(d) and d != 0) else 0

@st.cache_data
def process_data(sales_file, user_file, att_file, cov_file, cc_file, ful_file):
    # Load Data
    df_sales = pd.read_excel(sales_file)
    df_user = pd.read_excel(user_file)
    df_attendance = pd.read_excel(att_file)
    df_coverage = pd.read_excel(cov_file)
    df_call_cycle = pd.read_excel(cc_file)
    df_fulfill = pd.read_excel(ful_file)

    for df in [df_sales, df_attendance, df_coverage, df_call_cycle, df_user, df_fulfill]:
        df.columns = df.columns.astype(str).str.strip()

    # Identify Key Columns
    id_keys = ['EMPLOYEE CODE', 'EMP CODE', 'EMPLOYE I', 'EMP ID']
    emp_s = find_col(df_sales, id_keys)
    emp_u = find_col(df_user, id_keys)
    emp_a = find_col(df_attendance, id_keys)
    emp_cov = find_col(df_coverage, id_keys)
    emp_f = find_col(df_fulfill, id_keys)

    ticket_s = find_col(df_sales, ['TICKET NO', 'TICKET NC', 'TICKET_NO'])
    ticket_f = find_col(df_fulfill, ['TICKET NO', 'TICKET NC', 'TICKET_NO'])
    price_col = find_col(df_sales, ['SALE PRICE', 'SALE PRIC', 'PRICE']) 
    signoff_col = find_col(df_fulfill, ['SIGNOFF QTY', 'SIGN OFF', 'QTY'])
    sales_val_col = find_col(df_sales, ['TOTAL SALES VALUE', 'TOTAL SAL', 'SALES VALUE'])
    qty_case_col = find_col(df_sales, ['QTY IN CASE', 'CASE QTY', 'CASES'])
    dist_col = find_col(df_sales, ['DISTRIBUTOR NAME', 'DISTRIBUT'])
    
    col_visited = find_col(df_coverage, ['VISITED', 'VISIT'])
    col_billed = find_col(df_coverage, ['BILLED', 'BILL'])

    # Find and Clean Date Columns safely
    date_sales = find_date_col(df_sales)
    date_att = find_date_col(df_attendance)
    date_cov = find_date_col(df_coverage)

    df_sales = parse_dates_safely(df_sales, date_sales)
    df_attendance = parse_dates_safely(df_attendance, date_att)
    df_coverage = parse_dates_safely(df_coverage, date_cov)

    # Base Processing for Master Excel Data
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

    # Safe Numeric Conversions
    df_sales[sales_val_col] = pd.to_numeric(df_sales[sales_val_col], errors='coerce').fillna(0)
    df_sales[qty_case_col] = pd.to_numeric(df_sales[qty_case_col], errors='coerce').fillna(0)
    df_coverage[col_visited] = pd.to_numeric(df_coverage[col_visited], errors='coerce').fillna(0)
    df_coverage[col_billed] = pd.to_numeric(df_coverage[col_billed], errors='coerce').fillna(0)

    # Master Aggregations (Overall snapshot)
    l_val = df_sales.groupby(emp_s)[sales_val_col].sum().reset_index(name='L_val')
    master = master.merge(l_val, on=emp_s, how='left')
    master = master.fillna('')
    master['emp_s'] = emp_s

    return master, df_sales, df_attendance, df_coverage, df_fulfill, emp_s, emp_a, emp_cov, emp_f, sales_val_col, qty_case_col, col_visited, col_billed, ticket_s, ticket_f, price_col, signoff_col


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
            # Unpack processed data
            master_df, df_sales, df_att, df_cov, df_ful, emp_s, emp_a, emp_cov, emp_f, sales_val_col, qty_case_col, col_visited, col_billed, ticket_s, ticket_f, price_col, signoff_col = process_data(f_sales, f_user, f_att, f_cov, f_cc, f_ful)

            tab1, tab2 = st.tabs(["👤 Employee Profile (Monthly View)", "📊 Overall Summary Data"])

            # ==========================================
            # TAB 1: INDIVIDUAL EMPLOYEE MONTHLY PROFILE
            # ==========================================
            with tab1:
                col_sel, _ = st.columns([1, 2])
                with col_sel:
                    emp_list = master_df['EMPLOYEE NAME'].unique().tolist()
                    selected_emp = st.selectbox("Search / Select Employee:", sorted([x for x in emp_list if str(x).strip() != '']))

                if selected_emp:
                    # Get Employee Master Details
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

                    st.markdown("### 📅 Month-by-Month Performance Trend")
                    
                    emp_sales = df_sales[df_sales[emp_s] == emp_id_val]
                    emp_att = df_att[df_att[emp_a] == emp_id_val] if 'Month' in df_att.columns else pd.DataFrame()
                    emp_coverage = df_cov[df_cov[emp_cov] == emp_id_val] if 'Month' in df_cov.columns else pd.DataFrame()

                    # Gather all valid months
                    months = set()
                    if 'Month' in emp_sales.columns: months.update(emp_sales['Month'].dropna().unique())
                    if 'Month' in emp_att.columns: months.update(emp_att['Month'].dropna().unique())
                    if 'Month' in emp_coverage.columns: months.update(emp_coverage['Month'].dropna().unique())
                    
                    # Remove any leftover blanks/NaTs
                    months = sorted([m for m in months if m != 'nan' and pd.notnull(m)])

                    if not months:
                        st.warning("No valid date data found for this employee. Ensure your Excel files have a proper Date column.")
                    else:
                        monthly_records = []
                        m_counter = 1
                        
                        for m in months:
                            # MD (Mandays): Count of 'Present'
                            md_val = 0
                            if 'Month' in emp_att.columns:
                                att_col = find_col(emp_att, ['ATTENDANCE', 'STATUS'])
                                if att_col:
                                    md_val = len(emp_att[(emp_att['Month'] == m) & (emp_att[att_col].astype(str).str.upper() == 'PRESENT')])

                            # MW (Market Working): Visited & Billed
                            mw_visited = 0
                            mw_billed = 0
                            if 'Month' in emp_coverage.columns:
                                m_cov = emp_coverage[emp_coverage['Month'] == m]
                                mw_visited = m_cov[col_visited].sum() if col_visited else 0
                                mw_billed = m_cov[col_billed].sum() if col_billed else 0

                            # Perf (Performance): Sales Value
                            m_sales = emp_sales[emp_sales['Month'] == m]
                            perf_sales = m_sales[sales_val_col].sum() if sales_val_col else 0
                            perf_cases = m_sales[qty_case_col].sum() if qty_case_col else 0

                            # Fulfillment (Full.):
                            full_val = 0
                            if ticket_s and ticket_f and signoff_col:
                                m_tickets = m_sales[ticket_s].dropna()
                                m_ful = df_ful[df_ful[ticket_f].isin(m_tickets)]
                                
                                price_lookup = df_sales[[ticket_s, price_col]].drop_duplicates(subset=[ticket_s])
                                merged_f = pd.merge(m_ful, price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
                                full_val = (pd.to_numeric(merged_f[price_col], errors='coerce').fillna(0) * pd.to_numeric(merged_f[signoff_col], errors='coerce').fillna(0)).sum()

                            # Productivity (Prod): Average Cases per Billed Store
                            prod_val = round(dv(perf_cases, mw_billed), 2)

                            monthly_records.append({
                                "Timeline": f"M{m_counter} ({m})",
                                "Mandays (MD)": md_val,
                                "Market Working (MW) Visited": mw_visited,
                                "Market Working (MW) Billed": mw_billed,
                                "Performance (Sales ₹)": f"₹ {perf_sales:,.0f}",
                                "Order Fullfilment (₹)": f"₹ {full_val:,.0f}",
                                "Productivity (Avg Cases)": prod_val
                            })
                            m_counter += 1

                        # Display Table
                        df_trend = pd.DataFrame(monthly_records)
                        st.dataframe(df_trend, use_container_width=True, hide_index=True)

                        # Display Chart
                        st.markdown("##### 📈 Performance vs Fulfillment Trend")
                        df_trend['Perf Raw'] = df_trend['Performance (Sales ₹)'].str.replace('₹', '', regex=False).str.replace(',', '', regex=False).astype(float)
                        df_trend['Full Raw'] = df_trend['Order Fullfilment (₹)'].str.replace('₹', '', regex=False).str.replace(',', '', regex=False).astype(float)
                        
                        fig = px.bar(df_trend, x='Timeline', y=['Perf Raw', 'Full Raw'], barmode='group', 
                                     labels={'value': 'Value in ₹', 'variable': 'Category'},
                                     color_discrete_map={'Perf Raw': '#3498db', 'Full Raw': '#2ecc71'})
                        st.plotly_chart(fig, use_container_width=True)

            with tab2:
                st.subheader("All Employees Dataset")
                display_df = master_df.copy()
                st.dataframe(display_df, use_container_width=True, hide_index=True)

        except Exception as e:
            st.error("🚨 Data Processing Error")
            st.warning(str(e))

else:
    st.info("👈 Please upload all 6 required Excel reports in the sidebar to generate the dashboard.")