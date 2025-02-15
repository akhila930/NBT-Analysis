from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import numpy as np
from datetime import datetime, date
import os
import json
import plotly
import plotly.express as px
import plotly.graph_objects as go
import plotly.utils
import calendar

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 32 * 1024 * 1024  # 32MB max file size

COLUMNS = [
    'Email Address', 'City', 'RM', 'Old Due', 'Total Fees', 'Received',
    'Pending', 'Subscription Start Date', 'Subscription End Date',
    'Full_Payment_Date', 'Full_Payment_Amount', 'Full_Payment_Bill_Id', 'Batch'
]

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.strftime('%Y-%m-%d')
        return super().default(obj)

app.json_encoder = DateTimeEncoder

def clean_numeric_column(df, column):
    """Convert numeric columns to float, replacing any non-numeric values with 0"""
    df[column] = pd.to_numeric(df[column].replace('[\$,]', '', regex=True).fillna(0), errors='coerce').fillna(0)
    return df

def process_excel_file(file_path):
    """Process the uploaded Excel file and return a DataFrame"""
    return pd.read_excel(file_path, sheet_name=None)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.xlsx'):
        return jsonify({'error': 'Please upload an Excel file'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    file.save(file_path)
    
    try:
        data = process_excel_file(file_path)
        xls = pd.ExcelFile(file_path)
        years = xls.sheet_names
        return jsonify({'message': 'File uploaded successfully', 'years': years})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_available_years', methods=['GET'])
def get_available_years():
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        xls = pd.ExcelFile(file_path)
        years = xls.sheet_names
        return jsonify({'years': years})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_payment_details', methods=['POST'])
def get_payment_details():
    data = request.json
    name = data.get('name')
    year = data.get('year')
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        all_data = []
        xls = pd.ExcelFile(file_path)
        
        # If year is specified and not 'all', only search in that sheet
        sheets_to_search = [year] if year and year != 'all' else xls.sheet_names
        
        for sheet_name in sheets_to_search:
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Search in both Name and Email Address columns
                name_mask = df['Name'].str.lower().str.contains(name.lower(), na=False)
                email_mask = df['Email Address'].str.lower().str.contains(name.lower(), na=False)
                mask = name_mask | email_mask
                
                if mask.any():
                    matching_data = df[mask].copy()
                    matching_data['Year'] = sheet_name
                    
                    # Clean numeric columns
                    numeric_columns = ['Total Fees', 'Received', 'Pending', 'Old Due']
                    for col in numeric_columns:
                        if col in matching_data.columns:
                            matching_data[col] = pd.to_numeric(matching_data[col].replace(r'[\$,]', '', regex=True).fillna(0), errors='coerce').fillna(0)
                    
                    # Select required columns
                    required_columns = ['Name', 'Email Address', 'Year', 'Old Due', 'Total Fees', 'Received', 'Pending']
                    matching_data = matching_data[required_columns]
                    
                    all_data.extend(matching_data.to_dict('records'))
            except Exception as e:
                print(f"Error processing sheet {sheet_name}: {str(e)}")
                continue
        
        return jsonify(all_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_upcoming_renewals', methods=['POST'])
def get_upcoming_renewals():
    data = request.json
    year = data.get('year')
    month = data.get('month')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        today = pd.Timestamp.now()
        df = pd.read_excel(file_path, sheet_name=year)
        
        # Convert subscription end date to datetime
        df['Subscription End Date'] = pd.to_datetime(df['Subscription End Date'], errors='coerce')
        
        # Drop rows with invalid dates
        df = df.dropna(subset=['Subscription End Date'])
        
        # Calculate days until renewal
        df['Days Until Renewal'] = (df['Subscription End Date'] - today).dt.days
        
        # Filter by month if specified
        if month:
            df = df[df['Subscription End Date'].dt.month == int(month)]
        
        # Create a copy to avoid SettingWithCopyWarning
        result_df = df.copy()
        
        # Add status column
        result_df.loc[result_df['Days Until Renewal'] < 0, 'Status'] = 'Past Due'
        result_df.loc[result_df['Days Until Renewal'] >= 0, 'Status'] = 'Upcoming'
        
        # Format the results
        result_df['Subscription End Date'] = result_df['Subscription End Date'].dt.strftime('%Y-%m-%d')
        
        # Select and rename columns for output
        output_columns = ['Name', 'Email Address', 'Subscription End Date', 'Days Until Renewal', 'Status']
        result = result_df[output_columns].to_dict('records')
        
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export_renewals', methods=['GET'])
def export_renewals():
    year = request.args.get('year')
    month = request.args.get('month')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        today = pd.Timestamp.now()
        df = pd.read_excel(file_path, sheet_name=year)
        
        # Convert subscription end date to datetime
        df['Subscription End Date'] = pd.to_datetime(df['Subscription End Date'], errors='coerce')
        
        # Drop rows with invalid dates
        df = df.dropna(subset=['Subscription End Date'])
        
        # Calculate days until renewal
        df['Days Until Renewal'] = (df['Subscription End Date'] - today).dt.days
        
        # Filter by month if specified
        if month:
            df = df[df['Subscription End Date'].dt.month == int(month)]
        
        # Add status column
        df.loc[df['Days Until Renewal'] < 0, 'Status'] = 'Past Due'
        df.loc[df['Days Until Renewal'] >= 0, 'Status'] = 'Upcoming'
        
        # Format the date
        df['Subscription End Date'] = df['Subscription End Date'].dt.strftime('%Y-%m-%d')
        
        # Select relevant columns
        columns = ['Name', 'Email Address', 'City', 'RM', 'Subscription End Date', 'Days Until Renewal', 'Status']
        export_df = df[columns].sort_values('Days Until Renewal')
        
        # Export to Excel
        month_name = f"_{calendar.month_name[int(month)]}" if month else ""
        output_file = os.path.join('/tmp', f'renewals_{year}{month_name}.xlsx')
        export_df.to_excel(output_file, index=False)
        
        return send_file(output_file, as_attachment=True, download_name=f'renewals_{year}{month_name}.xlsx')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export_batch_pending_summary', methods=['GET'])
def export_batch_pending_summary():
    year = request.args.get('year')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        df = clean_numeric_column(df, 'Pending')
        
        batch_stats = df.groupby('Batch').agg({
            'Pending': 'sum',
            'Name': 'count'
        }).reset_index()
        
        batch_stats.columns = ['Batch', 'Total Pending', 'Number of Customers']
        
        output_file = os.path.join('/tmp', f'batch_pending_summary_{year}.xlsx')
        batch_stats.to_excel(output_file, index=False)
        
        return send_file(output_file, as_attachment=True, download_name=f'batch_pending_summary_{year}.xlsx')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export_selected_batch_pending', methods=['GET'])
def export_selected_batch_pending():
    year = request.args.get('year')
    batch = request.args.get('batch')
    
    if not year or not batch:
        return jsonify({'error': 'Please select both year and batch'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        
        # Filter for selected batch
        df = df[df['Batch'] == batch]
        
        # Clean numeric columns
        numeric_columns = ['Total Fees', 'Received', 'Pending', 'Old Due']
        for col in numeric_columns:
            if col in df.columns:
                df = clean_numeric_column(df, col)
        
        # Select relevant columns
        columns = ['Name', 'Email Address', 'Total Fees', 'Received', 'Pending', 'Old Due']
        export_df = df[columns].sort_values('Pending', ascending=False)
        
        output_file = os.path.join('/tmp', f'batch_{batch}_pending_{year}.xlsx')
        export_df.to_excel(output_file, index=False)
        
        return send_file(output_file, as_attachment=True, download_name=f'batch_{batch}_pending_{year}.xlsx')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_rm_pending', methods=['POST'])
def get_rm_pending():
    data = request.json
    year = data.get('year')
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        df = clean_numeric_column(df, 'Pending')
        pending_by_rm = df.groupby('RM')['Pending'].sum().to_dict()
        return jsonify(pending_by_rm)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_batch_list', methods=['POST'])
def get_batch_list():
    data = request.json
    year = data.get('year')
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        batches = sorted(df['Batch'].unique().tolist())
        return jsonify({'batches': batches})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_batch_pending', methods=['POST'])
def get_batch_pending():
    data = request.json
    year = data.get('year')
    batch = data.get('batch')
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        df = clean_numeric_column(df, 'Pending')
        
        if batch:
            # Filter for specific batch
            df = df[df['Batch'] == batch]
            
        batch_stats = {
            'batch': batch,
            'total_pending': float(df['Pending'].sum()),
            'customer_count': len(df),
            'pending_participants': df[df['Pending'] > 0][['Name', 'Email Address', 'Pending']].to_dict('records')
        }
        return jsonify(batch_stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_payment_summary', methods=['POST'])
def get_payment_summary():
    data = request.json
    year = data.get('year')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        
        # Clean numeric columns
        numeric_columns = ['Total Fees', 'Received', 'Pending', 'Old Due']
        for col in numeric_columns:
            if col in df.columns:
                df = clean_numeric_column(df, col)
        
        # Calculate summary
        summary_data = {
            'Total Fees': float(df['Total Fees'].sum()),
            'Total Received': float(df['Received'].sum()),
            'Total Pending': float(df['Pending'].sum()),
            'Total Old Due': float(df['Old Due'].sum()),
            'Total Customers': int(len(df)),
            'Fully Paid Customers': int(len(df[df['Pending'] == 0]))
        }
        
        return jsonify(summary_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_city_stats', methods=['POST'])
def get_city_stats():
    data = request.json
    year = data.get('year')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        df = clean_numeric_column(df, 'Pending')
        
        city_stats = df.groupby('City').agg({
            'Pending': 'sum',
            'Name': 'count'
        }).reset_index()
        
        city_stats.columns = ['City', 'Total Pending', 'Number of Customers']
        city_stats = city_stats.sort_values('Total Pending', ascending=False)
        
        return jsonify(city_stats.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_active_analysis', methods=['POST'])
def get_active_analysis():
    data = request.json
    year = data.get('year')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        
        # Clean numeric columns
        numeric_columns = ['Total Fees', 'Received', 'Pending']
        for col in numeric_columns:
            if col in df.columns:
                df = clean_numeric_column(df, col)
        
        # Calculate active participants (those who have made some payment)
        active_df = df[df['Received'] > 0]
        
        analysis = {
            'total_participants': len(df),
            'active_participants': len(active_df),
            'total_received': float(active_df['Received'].sum()),
            'total_pending': float(active_df['Pending'].sum()),
            'fully_paid': int(len(df[df['Pending'] == 0])),
            'partially_paid': int(len(df[(df['Received'] > 0) & (df['Pending'] > 0)]))
        }
        
        return jsonify(analysis)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get_unplugged_analysis', methods=['POST'])
def get_unplugged_analysis():
    data = request.json
    year = data.get('year')
    
    if not year:
        return jsonify({'error': 'Please select a year'}), 400
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        
        # Clean numeric columns
        numeric_columns = ['Total Fees', 'Received', 'Pending']
        for col in numeric_columns:
            if col in df.columns:
                df = clean_numeric_column(df, col)
        
        # Get unplugged participants (those who haven't made any payment)
        unplugged_df = df[df['Received'] == 0]
        
        analysis = {
            'total_participants': len(df),
            'unplugged_participants': len(unplugged_df),
            'total_pending': float(unplugged_df['Pending'].sum()),
            'unplugged_list': unplugged_df[['Name', 'Email Address', 'City', 'Pending']].to_dict('records')
        }
        
        return jsonify(analysis)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export_payment_details', methods=['POST'])
def export_payment_details():
    data = request.json
    name = data.get('name')
    year = data.get('year')
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        all_data = []
        xls = pd.ExcelFile(file_path)
        
        sheets_to_search = [year] if year and year != 'all' else xls.sheet_names
        
        for sheet_name in sheets_to_search:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            name_mask = df['Name'].str.lower().str.contains(name.lower(), na=False)
            email_mask = df['Email Address'].str.lower().str.contains(name.lower(), na=False)
            mask = name_mask | email_mask
            
            if mask.any():
                matching_data = df[mask].copy()
                matching_data['Year'] = sheet_name
                all_data.append(matching_data)
        
        if all_data:
            result_df = pd.concat(all_data)
            output_file = os.path.join('/tmp', f'payment_details_{name}.xlsx')
            result_df.to_excel(output_file, index=False)
            return send_file(output_file, as_attachment=True, download_name=f'payment_details_{name}.xlsx')
        else:
            return jsonify({'error': 'No matching records found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/export_rm_pending', methods=['POST'])
def export_rm_pending():
    data = request.json
    year = data.get('year')
    
    file_path = os.path.join('/tmp', 'payment_data.xlsx')
    try:
        df = pd.read_excel(file_path, sheet_name=year)
        df = clean_numeric_column(df, 'Pending')
        
        rm_stats = df.groupby('RM').agg({
            'Pending': 'sum',
            'Name': 'count'
        }).reset_index()
        
        rm_stats.columns = ['RM', 'Total Pending', 'Number of Customers']
        
        output_file = os.path.join('/tmp', f'rm_pending_{year}.xlsx')
        rm_stats.to_excel(output_file, index=False)
        
        return send_file(output_file, as_attachment=True, download_name=f'rm_pending_{year}.xlsx')
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# NBT Active Analysis Routes
@app.route('/upload_nbt', methods=['POST'])
def upload_nbt_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    
    if not file.filename.endswith('.xlsx'):
        return jsonify({'error': 'Please upload an Excel file'}), 400
    
    file_path = os.path.join('/tmp', 'nbt_data.xlsx')
    file.save(file_path)
    
    try:
        # Validate the file has required columns
        df = pd.read_excel(file_path)
        
        # Print actual columns for debugging
        print("Actual columns in the Excel file:", df.columns.tolist())
        
        # Create a mapping of alternate column names (case-insensitive)
        column_mapping = {
            'MOBILE': ['mobile', 'mobile no.', 'mobile number', 'phone', 'contact', 'mobile no', 'contact number'],
            'DOB': ['dob', 'date of birth', 'birth date', 'birthday', 'date_of_birth', 'birth_date', 
                   'birthdate', 'date of birth', 'dob (dd-mm-yyyy)', 'birth date'],
            'BATCH': ['batch', 'batch no', 'batch number'],
            'RENEWAL DATE': ['renewal date', 'renewal', 'renewal_date', 'renewaldate'],
            'NAME': ['name', 'full name', 'participant name'],
            'EMAIL': ['email', 'email id', 'email address'],
            'NEW RM': ['new rm', 'rm', 'relationship manager'],
            'CITY': ['city', 'location'],
            'ADDRESS': ['address', 'full address'],
            'Company Name': ['company name', 'company', 'organization']
        }
        
        # Convert all column names to lowercase for comparison
        df.columns = df.columns.str.strip().str.lower()
        
        # Function to find and rename columns
        def find_and_rename_column(df, standard_name, possible_names):
            possible_names = [name.lower() for name in possible_names]
            for name in df.columns:
                if name.lower() in possible_names:
                    if name != standard_name:
                        df.rename(columns={name: standard_name}, inplace=True)
                    return True
            return False
        
        # Check for missing columns and rename existing ones
        missing_columns = []
        for standard_name, variations in column_mapping.items():
            if not find_and_rename_column(df, standard_name, variations):
                missing_columns.append(standard_name)
                print(f"Could not find column {standard_name} in variations: {variations}")
        
        if missing_columns:
            # Print more detailed error information
            print("Missing columns:", missing_columns)
            print("Available columns:", df.columns.tolist())
            return jsonify({
                'error': f'Missing required columns: {", ".join(missing_columns)}',
                'available_columns': df.columns.tolist()
            }), 400
        
        # Save the standardized DataFrame
        df.to_excel(file_path, index=False)
        return jsonify({'message': 'NBT data uploaded successfully'})
    except Exception as e:
        print("Error processing file:", str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/group_analysis', methods=['POST'])
def group_analysis():
    try:
        data = request.json
        column = data.get('column')
        
        if not column:
            return jsonify({'error': 'Please select a column'}), 400
            
        file_path = os.path.join('/tmp', 'nbt_data.xlsx')
        df = pd.read_excel(file_path)
        
        group_counts = df[column].value_counts().to_dict()
        return jsonify(group_counts)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/data_summary', methods=['GET'])
def data_summary():
    try:
        file_path = os.path.join('/tmp', 'nbt_data.xlsx')
        df = pd.read_excel(file_path)
        
        # Basic dataset information
        summary = {
            'total_participants': len(df),
            'total_columns': len(df.columns),
            'unique_batches': df['BATCH'].nunique(),
            'unique_cities': df['CITY'].nunique(),
            'unique_rms': df['NEW RM'].nunique(),
            'dataset_shape': f"{df.shape[0]} rows × {df.shape[1]} columns",
            'column_names': df.columns.tolist(),
            'missing_values': df.isnull().sum().to_dict(),
            'batch_distribution': df['BATCH'].value_counts().head().to_dict(),
            'city_distribution': df['CITY'].value_counts().head().to_dict(),
            'rm_distribution': df['NEW RM'].value_counts().head().to_dict()
        }
        
        return jsonify(summary)
    except Exception as e:
        print("Error in data_summary:", str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/generate_visualization', methods=['POST'])
def generate_visualization():
    try:
        data = request.json
        viz_type = data.get('type')
        x_column = data.get('x_column')
        y_column = data.get('y_column')
        
        if not viz_type or not x_column:
            return jsonify({'error': 'Please select visualization type and columns'}), 400
            
        file_path = os.path.join('/tmp', 'nbt_data.xlsx')
        df = pd.read_excel(file_path)
        
        if x_column not in df.columns:
            return jsonify({'error': f'Column {x_column} not found in dataset'}), 400
        
        if viz_type == 'bar':
            data = df[x_column].value_counts()
            fig = px.bar(x=data.index, y=data.values, 
                        title=f'Distribution of {x_column}',
                        labels={'x': x_column, 'y': 'Count'})
        
        elif viz_type == 'pie':
            data = df[x_column].value_counts()
            fig = px.pie(values=data.values, names=data.index, 
                        title=f'Distribution of {x_column}')
        
        elif viz_type == 'scatter':
            if not y_column or y_column not in df.columns:
                return jsonify({'error': 'Please select valid Y column for scatter plot'}), 400
            fig = px.scatter(df, x=x_column, y=y_column,
                           title=f'{x_column} vs {y_column}')
        
        elif viz_type == 'histogram':
            fig = px.histogram(df, x=x_column,
                             title=f'Histogram of {x_column}')
        
        elif viz_type == 'box':
            fig = px.box(df, y=x_column,
                        title=f'Box Plot of {x_column}')
        
        graphJSON = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        return jsonify({'plot': graphJSON, 'columns': df.columns.tolist()})
    except Exception as e:
        print("Error in generate_visualization:", str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/renewal_dates', methods=['POST'])
def renewal_dates():
    try:
        data = request.json
        group_by = data.get('group_by')
        
        file_path = os.path.join('/tmp', 'nbt_data.xlsx')
        df = pd.read_excel(file_path)
        
        # Convert renewal date to datetime
        df['RENEWAL DATE'] = pd.to_datetime(df['RENEWAL DATE'], errors='coerce')
        current_date = pd.Timestamp.now()
        
        # Filter for upcoming renewals
        upcoming_renewals = df[df['RENEWAL DATE'] > current_date].copy()
        upcoming_renewals['RENEWAL DATE'] = upcoming_renewals['RENEWAL DATE'].dt.strftime('%Y-%m-%d')
        
        if group_by:
            renewals_grouped = upcoming_renewals.groupby(group_by).agg({
                'NAME': list,  # Keep list of names
                'RENEWAL DATE': list,  # Keep list of dates
                'EMAIL': list,  # Keep list of emails
                'MOBILE': list  # Keep list of mobile numbers
            }).to_dict('index')
            return jsonify(renewals_grouped)
        else:
            renewals = upcoming_renewals[['NAME', 'RENEWAL DATE', 'EMAIL', 'MOBILE', 'NEW RM', 'BATCH']].to_dict('records')
            return jsonify(renewals)
    except Exception as e:
        print("Error in renewal_dates:", str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/upcoming_birthdays', methods=['POST'])
def upcoming_birthdays():
    try:
        data = request.json
        month = data.get('month')
        
        if not month:
            return jsonify({'error': 'Please select a month'}), 400
            
        file_path = os.path.join('/tmp', 'nbt_data.xlsx')
        df = pd.read_excel(file_path)
        
        # Convert DOB to datetime with error handling
        df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce')
        
        # Filter out invalid dates
        df = df[df['DOB'].notna()].copy()
        
        # Get month number from input
        month_num = datetime.strptime(month, '%Y-%m').month
        
        # Filter birthdays for the selected month
        birthdays = df[df['DOB'].dt.month == month_num].copy()
        
        # Sort by day of month
        birthdays['day'] = birthdays['DOB'].dt.day
        birthdays = birthdays.sort_values('day')
        
        # Format dates for display
        birthdays['DOB'] = birthdays['DOB'].dt.strftime('%d-%m-%Y')
        
        # Prepare result
        result = birthdays[['NAME', 'DOB', 'EMAIL', 'MOBILE', 'CITY', 'BATCH']].to_dict('records')
        return jsonify(result)
    except Exception as e:
        print("Error in upcoming_birthdays:", str(e))
        return jsonify({'error': str(e)}), 500

@app.route('/participant_details', methods=['POST'])
def participant_details():
    try:
        data = request.json
        name = data.get('name')
        
        if not name:
            return jsonify({'error': 'Please enter participant name'}), 400
            
        file_path = os.path.join('/tmp', 'nbt_data.xlsx')
        df = pd.read_excel(file_path)
        
        # Convert name to lowercase for case-insensitive search
        df['NAME_LOWER'] = df['NAME'].str.lower()
        name_lower = name.lower()
        
        # Search for partial matches
        mask = df['NAME_LOWER'].str.contains(name_lower, na=False)
        matching_participants = df[mask].copy()
        
        # Drop the temporary lowercase column
        matching_participants = matching_participants.drop(columns=['NAME_LOWER'])
        
        if len(matching_participants) == 0:
            return jsonify({'error': f'No participant found with name containing "{name}"'}), 404
        
        # Convert dates to string format
        if 'DOB' in matching_participants.columns:
            matching_participants['DOB'] = pd.to_datetime(matching_participants['DOB'], errors='coerce').dt.strftime('%d-%m-%Y')
        if 'RENEWAL DATE' in matching_participants.columns:
            matching_participants['RENEWAL DATE'] = pd.to_datetime(matching_participants['RENEWAL DATE'], errors='coerce').dt.strftime('%d-%m-%Y')
        
        # Convert to records format
        results = matching_participants.to_dict('records')
        
        # Clean up the results
        cleaned_results = []
        for result in results:
            cleaned_result = {}
            for key, value in result.items():
                # Convert NaN/None to empty string
                if pd.isna(value):
                    cleaned_result[key] = ''
                else:
                    cleaned_result[key] = str(value)
            cleaned_results.append(cleaned_result)
        
        return jsonify(cleaned_results)
        
    except Exception as e:
        print(f"Error in participant_details: {str(e)}")  # Log the error
        return jsonify({'error': f'Error searching for participant: {str(e)}'}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
