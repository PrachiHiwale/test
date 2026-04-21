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
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        padding: 5% 5% 5% 10%;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.05);
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def find_col(df, keywords):
    """Safely searches for a column name based on keywords."""
    for col in df.columns:
        if any(k.upper() in str(col).upper() for k in keywords):
            return col
    return None

def dv(n, d): 
    return (n / d) if (pd.notnull(d) and d != 0) else 0

@st.cache_data
def process_data(sales_file, user_file, att_file, cov_file, cc_file, ful_file):
    # Load Data (Added header=0 to ensure first row is captured)
    df_sales = pd.read_excel(sales_file)
    df_user = pd.read_excel(user_file)
    df_attendance = pd.read_excel(att_file)
    df_coverage = pd.read_excel(cov_file)
    df_call_cycle = pd.read_excel(cc_file)
    df_fulfill = pd.read_excel(ful_file)

    # Standardize Columns (Remove trailing spaces)
    for df in [df_sales, df_attendance, df_coverage, df_call_cycle, df_user, df_fulfill]:
        df.columns = df.columns.astype(str).str.strip()

    # Identify Key Columns (Added more fallback keywords for safety)
    id_keys = ['EMPLOYEE CODE', 'EMP CODE', 'EMPLOYE I', 'EMP ID']
    
    emp_s = find_col(df_sales, id_keys)
    emp_u = find_col(df_user, id_keys)
    emp_a = find_col(df_attendance, id_keys)
    emp_cov = find_col(df_coverage, id_keys)
    emp_cc = find_col(df_call_cycle, id_keys)
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
    col_store_cc = find_col(df_call_cycle, ['STORE CODE', 'STORE ID', 'OUTLET'])
    col_store_cov = find_col(df_coverage, ['STORE CODE', 'STORE ID', 'OUTLET'])

    # ==========================================
    # 🚨 ERROR CHECKER: Validates if columns exist
    # ==========================================
    mandatory_cols = {
        "Sales Report -> Employee ID": (emp_s, df_sales.columns.tolist()),
        "User Report -> Employee ID": (emp_u, df_user.columns.tolist()),
        "Attendance -> Employee ID": (emp_a, df_attendance.columns.tolist()),
        "Coverage -> Employee ID": (emp_cov, df_coverage.columns.tolist()),
        "Call Cycle -> Employee ID": (emp_cc, df_call_cycle.columns.tolist()),
        "Fulfillment -> Employee ID": (emp_f, df_fulfill.columns.tolist()),
        "Sales Report -> Ticket Number": (ticket_s, df_sales.columns.tolist()),
        "Fulfillment -> Ticket Number": (ticket_f, df_fulfill.columns.tolist()),
        "Sales Report -> Sale Price": (price_col, df_sales.columns.tolist()),
        "Fulfillment -> Signoff Qty": (signoff_col, df_fulfill.columns.tolist()),
        "Sales Report -> Total Sales Value": (sales_val_col, df_sales.columns.tolist()),
        "Sales Report -> Case Qty": (qty_case_col, df_sales.columns.tolist()),
        "Coverage Report -> 'VISITED'": (col_visited, df_coverage.columns.tolist()),
        "Coverage Report -> 'BILLED'": (col_billed, df_coverage.columns.tolist()),
        "Call Cycle Report -> 'STORE CODE'": (col_store_cc, df_call_cycle.columns.tolist()),
        "Coverage Report -> 'STORE CODE'": (col_store_cov, df_coverage.columns.tolist()),
    }

    missing_errors = []
    for mapping_name, (found_col, available_cols) in mandatory_cols.items():
        if found_col is None:
            missing_errors.append(f"**{mapping_name}** | Found columns in your file: `{available_cols[:5]}...`")
            
    if missing_errors:
        error_msg = "\n\n".join(missing_errors)
        raise ValueError(f"We could not find matching columns for the following:\n\n{error_msg}\n\n*Tip: Check if your Excel files have a blank row at the very top, or if the header names were changed.*")

    # ==========================================
    # Fulfillment Calculation
    # ==========================================
    price_lookup = df_sales[[ticket_s, price_col]].drop_duplicates(subset=[ticket_s])
    df_f_calc = pd.merge(df_fulfill[[emp_f, ticket_f, signoff_col]], price_lookup, left_on=ticket_f, right_on=ticket_s, how='left')
    df_f_calc['Line_Total'] = pd.to_numeric(df_f_calc[price_col], errors='coerce').fillna(0) * pd.to_numeric(df_f_calc[signoff_col], errors='coerce').fillna(0)
    fulfill_final = df_f_calc.groupby(emp_f)['Line_Total'].sum().reset_index(name='F_Rs')

    # Base Processing
    # Ensure DESIGNATION exists, fallback if not
    desig_col = find_col(df_sales, ['DESIGNATION'])
    if desig_col:
        tsi_sales = df_sales[df_sales[desig_col].astype(str).str.contains('TERRITORY SALES INCHARGE', na=False, case=False)].copy()
    else:
        tsi_sales = df_sales.copy() # Fallback if no designation col found

    base_cols = [c for c in ['EMPLOYEE CHANNEL TYPE', emp_s, 'EMPLOYEE NAME', desig_col, 'CITY', 'STATE', 'REGION', dist_col] if c and c in df_sales.columns]
    base = tsi_sales[base_cols].drop_duplicates(subset=[emp_s])

    user_cols = [c for c in [emp_u, 'STATUS', 'DATE OF JOINING', 'LEVEL3 EMPLOYEE NAME', 'LEVEL2 EMPLOYEE NAME'] if c and c in df_user.columns]
    user_info = df_user[user_cols]
    master = pd.merge(base, user_info, left_on=emp_s, right_on=emp_u, how='left')

    # Metrics Calculations
    today = datetime(2025, 12, 26)
    yesterday = today - timedelta(days=1)
    start_of_month = datetime(2025, 12, 1)

    def get_plan_a(doj):
        try:
            s = max(start_of_month, pd.to_datetime(doj, dayfirst=True))
            dr = pd.date_range(start=s, end=yesterday)
            return len([d for d in dr if d.weekday() != 6 and d.strftime('%Y-%m-%d') != '2025-12-25'])
        except: return 21

    if 'DATE OF JOINING' in master.columns:
        master['A_val'] = master['DATE OF JOINING'].apply(get_plan_a)
    else:
        master['A_val'] = 21 # Fallback

    # Ensure Attendance Col exists
    att_col = find_col(df_attendance, ['ATTENDANCE', 'STATUS'])
    if att_col:
        b_cnt = df_attendance[df_attendance[att_col].astype(str).str.upper() == 'PRESENT'].groupby(emp_a).size().reset_index(name='B_val')
    else:
        b_cnt = pd.DataFrame({emp_a: [], 'B_val': []})

    # Calculations safely using validated columns
    df_coverage[col_visited] = pd.to_numeric(df_coverage[col_visited], errors='coerce').fillna(0)
    df_coverage[col_billed] = pd.to_numeric(df_coverage[col_billed], errors='coerce').fillna(0)
    df_sales[sales_val_col] = pd.to_numeric(df_sales[sales_val_col], errors='coerce').fillna(0)
    df_sales[qty_case_col] = pd.to_numeric(df_sales[qty_case_col], errors='coerce').fillna(0)

    d_cnt = df_coverage.groupby(emp_cov)[col_visited].sum().reset_index(name='D_val')
    e_cnt = df_coverage.groupby(emp_cov)[col_billed].sum().reset_index(name='E_val')
    f_cnt = df_call_cycle.groupby(emp_cc)[col_store_cc].nunique().reset_index(name='F_val')
    g_cnt = df_coverage.groupby(emp_cov)[col_store_cov].nunique().reset_index(name='G_val')
    h_cnt = df_coverage[df_coverage[col_billed] > 0].groupby(emp_cov)[col_store_cov].nunique().reset_index(name='H_val')
    l_val = df_sales.groupby(emp_s)[sales_val_col].sum().reset_index(name='L_val')
    j_qty = df_sales.groupby(emp_s)[qty_case_col].sum().reset_index(name='J_val')

    # Merging
    master = master.merge(b_cnt, left_on=emp_s, right_on=emp_a, how='left') \
                   .merge(d_cnt, left_on=emp_s, right_on=emp_cov, how='left') \
                   .merge(e_cnt, left_on=emp_s, right_on=emp_cov, how='left') \
                   .merge(f_cnt, left_on=emp_s, right_on=emp_cc, how='left') \
                   .merge(g_cnt, left_on=emp_s, right_on=emp_cov, how='left') \
                   .merge(h_cnt, left_on=emp_s, right_on=emp_cov, how='left') \
                   .merge(l_val, left_on=emp_s, right_on=emp_s, how='left') \
                   .merge(fulfill_final, left_on=emp_s, right_on=emp_f, how='left') \
                   .merge(j_qty, left_on=emp_s, right_on=emp_s, how='left')

    metric_cols = ['B_val', 'D_val', 'E_val', 'F_val', 'G_val', 'H_val', 'L_val', 'F_Rs', 'J_val']
    for m_col in metric_cols:
        if m_col in master.columns:
            master[m_col] = master[m_col].fillna(0)

    master = master.fillna('')
    master['emp_s'] = emp_s  
    master['dist_col'] = dist_col 
    return master

def generate_excel(master):
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('Dashboard')

    # Formats
    fmt_green_h = workbook.add_format({'bold':True, 'align':'center', 'border':1, 'bg_color':'#92D050', 'text_wrap':True})
    fmt_blue_h = workbook.add_format({'bold':True, 'align':'center', 'border':1, 'bg_color':'#BDD7EE', 'text_wrap':True})
    fmt_yellow_h = workbook.add_format({'bold':True, 'align':'center', 'border':1, 'bg_color':'#FFFF00', 'text_wrap':True})
    fmt_darkblue_h = workbook.add_format({'bold':True, 'align':'center', 'border':1, 'bg_color':'#ACB9CA', 'text_wrap':True})
    fmt_peach_h = workbook.add_format({'bold':True, 'align':'center', 'border':1, 'bg_color':'#FCE4D6', 'text_wrap':True})
    fmt_lbl = workbook.add_format({'bold':True, 'align':'center', 'valign':'vcenter', 'font_size':13, 'border':1})
    fmt_cell = workbook.add_format({'border':1, 'align':'center'})
    fmt_pct = workbook.add_format({'num_format':'0%', 'border':1, 'align':'center'})
    fmt_curr_y = workbook.add_format({'num_format':'₹ #,##0;₹ #,##0;₹ -', 'border':1, 'align':'center', 'bg_color':'#FFFF00'})
    fmt_curr_p = workbook.add_format({'num_format':'₹ #,##0;₹ #,##0;₹ -', 'border':1, 'align': 'center', 'bg_color': '#FCE4D6'})

    # Headers
    headers = [
        'Sr.No.', 'Channel', 'Employee Code', 'Employee Name', 'Designation', 'City', 'State', 'Region', 'Status', 'DOJ', 'SS Name', 'Distributor Name', 'Name (Supervisor)', '', 
        'Plan Mandays ( A )', 'ACTUAL MANDAYs TILL DATE ( B )', 'Mandays % (= B/A*100)',
        'VISIT PLANNED ( B )', 'VISIT PLANNED ( C )', 'ACTUAL VISITED ( D )', 'TC % (=D/C*100)', 'Actual Productive ( E )', '% PC (=E/D*100)',
        'Mapped ( F )', 'Visited ( G )', 'Unique Cov %', 'Billed ( H )', 'ECO %', 'Avg TC (= D/B)', 'Avg PC (= E/B)', 'Avg Unique /Day (= G/B)',
        'TGT in Value ( K )', 'ACH in Value -MTD ( L )', 'Val .Ach % (= L/K*100)',
        'Order Fullfilment (In Rs)', 'Order Fullfilment %',
        'PER STORE AVG CASE SOLD ( = J/H)', 'AVG. Per Day sales-Cases (= J/B)', 'PER STORE AVG Sales Value ( = L/H)', 'Achievement (L)'
    ]

    for col, text in enumerate(headers):
        if col >= 36: fmt = fmt_peach_h
        elif col >= 34: fmt = fmt_darkblue_h
        elif col >= 31: fmt = fmt_yellow_h
        elif col >= 17: fmt = fmt_green_h
        elif col >= 14: fmt = fmt_blue_h
        else: fmt = fmt_green_h
        worksheet.write(1, col, text, fmt)

    worksheet.merge_range(0, 0, 0, 13, 'Employee Details', fmt_lbl)
    worksheet.merge_range(0, 14, 0, 16, 'Manning', fmt_lbl)
    worksheet.merge_range(0, 17, 0, 30, 'Market Working', fmt_lbl)
    worksheet.merge_range(0, 31, 0, 33, 'Performance', fmt_lbl)
    worksheet.merge_range(0, 34, 0, 35, 'Fullfilment', fmt_lbl)
    worksheet.merge_range(0, 36, 0, 39, 'Productivity', fmt_lbl)

    emp_s = master['emp_s'].iloc[0] if len(master) > 0 else 'EMPLOYEE CODE'
    dist_col = master['dist_col'].iloc[0] if len(master) > 0 else None

    for i, r in master.iterrows():
        row = i + 2
        doj = r.get('DATE OF JOINING', '')
        doj_str = doj.strftime('%Y-%m-%d') if isinstance(doj, (pd.Timestamp, datetime)) else str(doj)
            
        worksheet.write(row, 0, i+1, fmt_cell)
        worksheet.write(row, 1, r.get('EMPLOYEE CHANNEL TYPE', ''), fmt_cell)
        worksheet.write(row, 2, r.get(emp_s, ''), fmt_cell)
        worksheet.write(row, 3, r.get('EMPLOYEE NAME', ''), fmt_cell)
        worksheet.write(row, 4, r.get('DESIGNATION', ''), fmt_cell)
        worksheet.write(row, 5, r.get('CITY', ''), fmt_cell)
        worksheet.write(row, 6, r.get('STATE', ''), fmt_cell)
        worksheet.write(row, 7, r.get('REGION', ''), fmt_cell)
        worksheet.write(row, 8, r.get('STATUS', ''), fmt_cell)
        worksheet.write(row, 9, doj_str, fmt_cell) 
        worksheet.write(row, 10, r.get('LEVEL3 EMPLOYEE NAME', ''), fmt_cell)
        worksheet.write(row, 11, r.get(dist_col, '') if dist_col else '', fmt_cell)
        worksheet.write(row, 12, r.get('LEVEL2 EMPLOYEE NAME', ''), fmt_cell)
        worksheet.write(row, 13, '', fmt_cell)

        worksheet.write(row, 14, r.get('A_val', 0), fmt_cell)
        worksheet.write(row, 15, r.get('B_val', 0), fmt_cell)
        worksheet.write(row, 16, dv(r.get('B_val', 0), r.get('A_val', 0)), fmt_pct)

        worksheet.write(row, 17, r.get('A_val', 0)*40, fmt_cell)
        worksheet.write(row, 18, r.get('B_val', 0)*40, fmt_cell)
        worksheet.write(row, 19, r.get('D_val', 0), fmt_cell)
        worksheet.write(row, 20, dv(r.get('D_val', 0), r.get('B_val', 0)*40), fmt_pct)
        worksheet.write(row, 21, r.get('E_val', 0), fmt_cell)
        worksheet.write(row, 22, dv(r.get('E_val', 0), r.get('D_val', 0)), fmt_pct)
        worksheet.write(row, 23, r.get('F_val', 0), fmt_cell)
        worksheet.write(row, 24, r.get('G_val', 0), fmt_cell)
        worksheet.write(row, 25, dv(r.get('G_val', 0), r.get('F_val', 0)), fmt_pct)
        worksheet.write(row, 26, r.get('H_val', 0), fmt_cell)
        worksheet.write(row, 27, dv(r.get('H_val', 0), r.get('F_val', 0)), fmt_pct)
        worksheet.write(row, 28, round(dv(r.get('D_val', 0), r.get('B_val', 0)), 2), fmt_cell)
        worksheet.write(row, 29, round(dv(r.get('E_val', 0), r.get('B_val', 0)), 2), fmt_cell)
        worksheet.write(row, 30, round(dv(r.get('G_val', 0), r.get('B_val', 0)), 2), fmt_cell)

        worksheet.write(row, 31, 0, fmt_curr_y)
        worksheet.write(row, 32, r.get('L_val', 0), fmt_curr_y)
        worksheet.write(row, 33, 0, fmt_pct)
        worksheet.write(row, 34, r.get('F_Rs', 0), fmt_cell)
        worksheet.write(row, 35, dv(r.get('F_Rs', 0), r.get('L_val', 0)), fmt_pct)

        worksheet.write(row, 36, round(dv(r.get('J_val', 0), r.get('H_val', 0)), 2), fmt_cell)
        worksheet.write(row, 37, round(dv(r.get('J_val', 0), r.get('B_val', 0)), 2), fmt_cell)
        worksheet.write(row, 38, dv(r.get('L_val', 0), r.get('H_val', 0)), fmt_curr_p)
        worksheet.write(row, 39, r.get('L_val', 0), fmt_curr_p)

    workbook.close()
    return output.getvalue()


# ==========================================
# 3. APP LAYOUT & SIDEBAR
# ==========================================
st.title("📈 Multi-Brand Master Performance Dashboard")
st.markdown("Upload your 6 operational reports on the sidebar to generate insights and formatted Excel outputs.")

st.sidebar.header("📂 Data Upload")
st.sidebar.markdown("Please upload the standard `.xlsx` reports below:")

f_sales = st.sidebar.file_uploader("1. Sales Report", type=['xlsx'])
f_user = st.sidebar.file_uploader("2. User Master Report", type=['xlsx'])
f_att = st.sidebar.file_uploader("3. Daily Attendance", type=['xlsx'])
f_cov = st.sidebar.file_uploader("4. Coverage Report", type=['xlsx'])
f_cc = st.sidebar.file_uploader("5. Call Cycle Report", type=['xlsx'])
f_ful = st.sidebar.file_uploader("6. Order Fulfillment", type=['xlsx'])

if all([f_sales, f_user, f_att, f_cov, f_cc, f_ful]):
    with st.spinner("Processing data, merging reports, and calculating metrics..."):
        try:
            master_df = process_data(f_sales, f_user, f_att, f_cov, f_cc, f_ful)
            
            # Extract basic data for visual presentation
            total_sales = master_df['L_val'].sum() if 'L_val' in master_df else 0
            total_fulfill = master_df['F_Rs'].sum() if 'F_Rs' in master_df else 0
            active_emps = len(master_df)
            avg_fulfillment_pct = (total_fulfill / total_sales * 100) if total_sales > 0 else 0

            # UI Construction
            tab1, tab2, tab3 = st.tabs(["📊 Executive Summary", "🗃️ Detail Data View", "📥 Export Excel"])

            with tab1:
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Sales Value", f"₹ {total_sales:,.0f}")
                col2.metric("Total Order Fulfillment", f"₹ {total_fulfill:,.0f}")
                col3.metric("Avg Fulfillment %", f"{avg_fulfillment_pct:.1f} %")
                col4.metric("Active TSIs", f"{active_emps}")
                st.markdown("---")

                chart_col1, chart_col2 = st.columns(2)
                with chart_col1:
                    st.subheader("Sales by Region")
                    if 'REGION' in master_df.columns:
                        reg_sales = master_df.groupby('REGION')['L_val'].sum().reset_index()
                        fig1 = px.pie(reg_sales, values='L_val', names='REGION', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
                        st.plotly_chart(fig1, use_container_width=True)

                with chart_col2:
                    st.subheader("Top Employees by Sales")
                    if 'L_val' in master_df.columns and 'EMPLOYEE NAME' in master_df.columns:
                        top_emps = master_df.nlargest(10, 'L_val')
                        fig2 = px.bar(top_emps, x='EMPLOYEE NAME', y='L_val', text_auto='.2s', color='L_val', color_continuous_scale='Blues')
                        fig2.update_traces(textfont_size=12, textangle=0, textposition="outside", cliponaxis=False)
                        st.plotly_chart(fig2, use_container_width=True)

            with tab2:
                st.subheader("Processed Master Table")
                display_df = master_df.copy()
                st.dataframe(display_df, use_container_width=True, hide_index=True)

            with tab3:
                st.subheader("Download Formatted Excel")
                st.info("Click the button below to download the fully formatted Excel report, preserving exact color coding, grouped headers, and metrics as the original script.")
                excel_data = generate_excel(master_df)
                st.download_button(
                    label="📥 Download Master Performance Dashboard.xlsx",
                    data=excel_data,
                    file_name="Master_Performance_Dashboard.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        except Exception as e:
            st.error("🚨 Data Processing Error")
            st.warning(str(e))

else:
    st.info("👈 Please upload all 6 required Excel reports in the sidebar to generate the dashboard.")