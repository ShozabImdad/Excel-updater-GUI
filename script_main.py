import pandas as pd
import requests
import shutil
from datetime import datetime, timezone
import schedule
import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import winsound

_sound_enabled = True

def set_sound_enabled(enabled):
    global _sound_enabled
    _sound_enabled = enabled

class ExcelFileHandler(FileSystemEventHandler):
    def __init__(self, file_path, callback):
        self.file_path = file_path
        self.callback = callback
        self.last_modified = 0
        self.cooldown = 2  # Cooldown period in seconds to prevent multiple triggers

    def on_modified(self, event):
        if not event.is_directory and event.src_path == self.file_path:
            current_time = time.time()
            if current_time - self.last_modified > self.cooldown:
                self.last_modified = current_time
                self.callback()

def setup_file_monitor(input_excel_file_path):
    def handle_file_change():
        try:
            print("\nDetected changes in the Excel file. Updating schedules...")
            
            # Clear existing schedules
            schedule.clear()
            
            # Read the updated Excel file
            input_df = pd.read_excel(input_excel_file_path, header=None)
            save_time = str(input_df.iloc[0, 2])
            total_columns = input_df.shape[1] - 3
            
            # Schedule columns to process at the specified times
            for col in range(3, input_df.shape[1]):
                time_str = str(input_df.iloc[1, col])
                exclude_col = str(input_df.iloc[2, col]).strip().upper() == 'YES'
                if not exclude_col:
                    continue
                if pd.notna(time_str):
                    try:
                        # Validate time format
                        datetime.strptime(time_str, "%H:%M:%S")
                        schedule.every().day.at(time_str).do(lambda col=col: process_column(col, total_columns, input_df))
                        print(f"Updated schedule: Column {col-2} to run at {time_str}")
                    except ValueError:
                        try:
                            datetime.strptime(time_str, "%H:%M")
                            schedule.every().day.at(time_str).do(lambda col=col: process_column(col, total_columns, input_df))
                            print(f"Updated schedule: Column {col-2} to run at {time_str}")
                        except ValueError:
                            print(f"Invalid time format for column {col-2}: {time_str}")
            
            # Reschedule daily save
            try:
                schedule.every().day.at(save_time).do(daily_save_and_restart)
                print(f"Updated daily save schedule to {save_time}")
            except Exception as e:
                print(f"Error scheduling save time: {e}")
                
            print("Schedule updates completed.")
            
        except Exception as e:
            print(f"Error handling file change: {e}")

    # Set up the file observer
    event_handler = ExcelFileHandler(
        os.path.abspath(input_excel_file_path),
        handle_file_change
    )
    observer = Observer()
    observer.schedule(event_handler, path=os.path.dirname(os.path.abspath(input_excel_file_path)), recursive=False)
    observer.start()
    return observer



def daily_save_and_restart():
    global output_excel_file_path
    try:
        print("Saving data for the day.")
        
        # Save the current day's processed DataFrame
        # input_df.to_excel("final file.xlsx", index=False)
        # print(f"Workbook saved at: {output_excel_file_path}")

        processed_columns.clear()  # Clear processed columns for the next day

        # Generate new file path for the next day
        output_excel_file_path = generate_output_file_path()
        print(f"New file generated: {output_excel_file_path}")

        # Reschedule tasks for the next day
        schedule_tasks()
        print("Rescheduled tasks for the next day.")

    except Exception as e:
        print(f"Error in daily save and restart: {e}")


# Function to schedule tasks based on Excel input
def schedule_tasks():
    try:
        # Load Excel file and fetch values
        input_df = pd.read_excel(input_excel_file_path, header=None)
        save_time = str(input_df.iloc[0, 2])

        # Print save time to the console
        print(f"Save time read from Excel: {save_time}")

        # Ensure that the save_time is in the correct format (HH:MM or HH:MM:SS)
        try:
            # Try to parse it to ensure it's in the correct format (HH:MM or HH:MM:SS)
            datetime.strptime(save_time, "%H:%M:%S")  # Support both HH:MM:SS and HH:MM formats
        except ValueError:
            # If it doesn't match, check if it's in HH:MM format
            try:
                datetime.strptime(save_time, "%H:%M")
            except ValueError:
                print(f"Error: The save time format in Excel is invalid. Expected format HH:MM or HH:MM:SS. Got: {save_time}")
                return

        total_columns = input_df.shape[1] - 3
        print("total columns: ", total_columns)
        
        # Schedule columns to process at the specified times
        for col in range(3, input_df.shape[1]):
            time_str = str(input_df.iloc[1, col])
            exclude_col = str(input_df.iloc[2, col]).strip().upper() == 'YES'
            if not exclude_col:
                continue

            if pd.notna(time_str):
                try:
                    schedule.every().day.at(time_str).do(lambda col=col: process_column(col, total_columns, input_df))
                    print(f"Scheduled Column {col-2} to run at {time_str}")
                except Exception as e:
                    print(f"Error scheduling column {col-2} at {time_str}: {e}")
        
        # Schedule daily save and restart at save_time
        try:
            schedule.every().day.at(save_time).do(daily_save_and_restart)  # Schedule daily save and restart
            print(f"Scheduled daily save at {save_time}")
        except Exception as e:
            print(f"Error scheduling save and restart at {save_time}: {e}")

    except Exception as e:
        print(f"Error scheduling tasks: {e}")


def get_today_start_timestamp():
    # Get the current Unix timestamp (current time)
    current_unix_timestamp = int(time.time())
    
    # Convert the Unix timestamp to a UTC datetime object (timezone-aware)
    dt_utc = datetime.fromtimestamp(current_unix_timestamp, timezone.utc)
    
    # Get the start of the day (12:00 AM UTC)
    day_start_utc = dt_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Get the end of the day (11:59:59.999999 PM UTC)
    day_end_utc = day_start_utc.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Return the Unix timestamps for the start and end of the day in UTC
    return int(day_start_utc.timestamp()), int(day_end_utc.timestamp())


# Generate timestamped output file path in YYYYMMDD format
def generate_output_file_path():
    try:
        timestamp = datetime.now().strftime('%Y%m%d')
        output_file_path = f'{timestamp}_VOLvsAVGVOL.xlsx'
        return output_file_path
    except Exception as e:
        print(f"Error generating output file path: {e}")
        return None


# File paths
input_excel_file_path = 'Input File.xlsx'
output_excel_file_path = generate_output_file_path()
txt_file_1 = 'list1.txt'
txt_file_2 = 'list2.txt'
api_key = '303ae4a11024c293eddeef9bcd978a64'
api_url = f'https://financialmodelingprep.com/api/v3/symbol/NASDAQ?apikey={api_key}'
exclusion_excel_file = 'excluded_strings.xlsx'


# Track processed columns and save time
processed_columns = set()


# Function to read exclusion symbols from TXT files
def read_exclusion_symbols(file_path):
    try:
        with open(file_path, 'r') as file:
            symbols = file.read().splitlines()

        return symbols
    except Exception as e:
        print(f"Error reading exclusion symbols from {file_path}: {e}")
        return []


# Fetch data from API and filter by timestamp
def fetch_api_data():
    try:
        start_timestamp, end_timestamp = get_today_start_timestamp()
        print(f"Start timestamp: {start_timestamp}")
        print(f"End timestamp: {end_timestamp}")
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        # Filter data by timestamp
        filtered_data = [
            item for item in data 
            if start_timestamp <= item.get('timestamp', 0) <= end_timestamp
        ]
        print(f"API data fetched for today: {len(filtered_data)} symbols")
        return filtered_data
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error in fetch_api_data: {e}")
        return []


# Utility function to safely convert strings to floats
def safe_convert_to_float(value, description):
    try:
        return float(value)
    except (ValueError, TypeError) as e:
        print(f"Error: Unable to convert '{value}' in {description} to float: {e}")
        return None


def check_and_drop_symbol_if_match(symbol, name, input_df, col, exclusion_excel_file):
    try:
        # Check if 'name' is None, and proceed without adding to exclusion list
        if name is None:
            return False
        
        # Load matching strings from the Excel file (assuming strings are in the first column)
        matching_strings = pd.read_excel(exclusion_excel_file).iloc[:, 0].dropna().str.strip().str.lower().tolist()
        
        # Split the name into words and convert them to lowercase
        name_list = name.split() if name else []  # Avoid split() on None or empty string
        name_list = [word.strip().lower() for word in name_list]  # Clean the words
        
        # Check if any word in name_list matches a string in matching_strings
        for word in name_list:
            if word in matching_strings:
                # Optional: Save excluded symbol to a file
                exclusion_file_name = f'Logs/excluded_symbols_column_{col-2}.txt'
                with open(exclusion_file_name, 'a') as f:
                    f.write(f"Symbol '{symbol}' matches exclusion list (word '{word}' matched). Dropping symbol.\n")  # Log the excluded symbol to the file
                
                return True  # Symbol should be excluded
        
        # If no match is found, return False (don't exclude the symbol)
        return False

    except Exception as e:
        print(f"Error checking symbol '{symbol}': {e}")
        return False

def is_file_open(file_path):
    try:
        with open(file_path, 'r+'):  # Try opening the file in read+write mode
            return False  
    except IOError:
        return True  
# Function to process a specific column in the Excel file

def fetch_stock_splits():
    """Fetch stock split data for today"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        split_api_url = f'https://financialmodelingprep.com/api/v3/stock_split_calendar?from={today}&to={today}&apikey={api_key}'
        response = requests.get(split_api_url)
        response.raise_for_status()
        splits_data = response.json()
        
        # Create a dictionary for easy lookup
        splits_dict = {}
        for split in splits_data:
            symbol = split['symbol'].split('.')[0]  # Remove .BO or .NS suffix
            splits_dict[symbol] = {
                'numerator': split['numerator'],
                'denominator': split['denominator']
            }
        return splits_dict
    except Exception as e:
        print(f"Error fetching stock splits: {e}")
        return {}


def process_column(col, total_columns, input_df):
    global output_excel_file_path
    try:
        # If this is not the first column, read the existing output file
        
        stock_splits = fetch_stock_splits()
        # Reading conditions from DataFrame
        min_price = safe_convert_to_float(input_df.iloc[3, col], 'Minimum Price')
        max_price = safe_convert_to_float(input_df.iloc[4, col], 'Maximum Price')
        min_volume = safe_convert_to_float(input_df.iloc[5, col], 'Minimum Volume')
        max_volume = safe_convert_to_float(input_df.iloc[6, col], 'Maximum Volume')
        min_avg_volume = safe_convert_to_float(input_df.iloc[7, col], 'Minimum Average Volume')
        max_avg_volume = safe_convert_to_float(input_df.iloc[8, col], 'Maximum Average Volume')
        min_volume_avg_volume = safe_convert_to_float(input_df.iloc[9, col], 'Min Volume / Average Volume')
        print(f"\nConditions for column {col-2}: {min_price}, {max_price}, {min_volume}, {max_volume}, {min_avg_volume}, {max_avg_volume}, {min_volume_avg_volume}")
        # Exclude settings in Excel
        exclude_txt_1 = str(input_df.iloc[12, col]).strip().upper() == 'YES'
        exclude_txt_2 = str(input_df.iloc[15, col]).strip().upper() == 'YES'
        print(f"Exclusion settings for column {col-2}: {exclude_txt_1}, {exclude_txt_2}")

        exclusion_symbols_1 = set(symbol.strip().upper() for symbol in read_exclusion_symbols(txt_file_1))
        exclusion_symbols_2 = set(symbol.strip().upper() for symbol in read_exclusion_symbols(txt_file_2))
        # Fetch API data
        api_data = fetch_api_data()
        if not api_data:
            print("No data fetched from API.")
            return

        matched_symbols = []
        excluded_symbols_with_reasons = []

        for data in api_data:
            symbol = data.get('symbol')
            name = data.get('name')
            price = data.get('price')
            min_volume_api = data.get('volume')
            avg_volume_api = data.get('avgVolume')
            if check_and_drop_symbol_if_match(symbol, name, input_df, col, exclusion_excel_file):
                continue  # If the symbol was dropped, skip the rest of the processing
            
            if symbol in stock_splits:
                split_info = stock_splits[symbol]
                numerator = split_info['numerator']
                denominator = split_info['denominator']
                adjusted_price = (denominator / numerator) * price
                print(f"Stock split detected for {symbol}. Original price: {price}, Adjusted price: {adjusted_price}")
                price = adjusted_price

            # Apply the conditions
            if price and min_volume_api and avg_volume_api:
                if min_price < price < max_price:
                    if min_volume < min_volume_api < max_volume:
                        if min_avg_volume < avg_volume_api < max_avg_volume:
                            if (min_volume_api / avg_volume_api) >= min_volume_avg_volume:
                                if exclude_txt_1 and symbol.strip().upper() in exclusion_symbols_1:
                                    reason = f"Symbol {symbol} dropped due to exclusion in {txt_file_1}"
                                    excluded_symbols_with_reasons.append((symbol, reason))
                                    continue
                                if exclude_txt_2 and symbol.strip().upper() in exclusion_symbols_2:
                                    reason = f"Symbol {symbol} dropped due to exclusion in {txt_file_2}"
                                    excluded_symbols_with_reasons.append((symbol, reason))
                                    continue
                                matched_symbols.append(symbol)
                            else:
                                reason = f"Symbol {symbol} dropped due to low volume-to-average-volume ratio."
                                excluded_symbols_with_reasons.append((symbol, reason))
                        else:
                            reason = f"Symbol {symbol} dropped due to average volume outside range."
                            excluded_symbols_with_reasons.append((symbol, reason))
                    else:
                        reason = f"Symbol {symbol} dropped due to volume outside range."
                        excluded_symbols_with_reasons.append((symbol, reason))
                else:
                    reason = f"Symbol {symbol} dropped due to price outside range."
                    excluded_symbols_with_reasons.append((symbol, reason))
            else:
                reason = f"Symbol {symbol} dropped price, min and avg volume."
                excluded_symbols_with_reasons.append((symbol, reason))

        # Calculate required number of rows
        print(f"Matched symbols for column {col-2}: {len(matched_symbols)}")
        required_rows = max(len(input_df), 18 + len(matched_symbols))
        
        # If this is the first column, create new DataFrame, otherwise use existing
        if col == 0:
            output_df = pd.DataFrame(index=range(required_rows), columns=input_df.columns)
            output_df.iloc[:18] = input_df.iloc[:18]  # Copy configuration rows
        else:
            if col > 0 and os.path.exists(output_excel_file_path): #type: ignore
                out_df = pd.read_excel(output_excel_file_path)
                in_df= input_df.copy()
                in_df= in_df.iloc[:18]
                out_df= out_df.iloc[18:]
                new_df = pd.concat([in_df, out_df], ignore_index=True)
            else:
                new_df = input_df.copy()
            output_df = new_df.copy()
            # Extend DataFrame if needed
            if len(output_df) < required_rows:
                new_rows = pd.DataFrame(index=range(len(output_df), required_rows), columns=output_df.columns)
                output_df = pd.concat([output_df, new_rows])
        
        # Clear existing data in current column below row 17
        output_df.iloc[18:, col] = None
        
        # Write the matched symbols to specific rows starting from row 18
        for idx, symbol in enumerate(matched_symbols, start=18):
            output_df.iloc[idx, col] = symbol
            
        # Save the modified DataFrame to Excel
        if os.path.exists(output_excel_file_path): #type: ignore
            if is_file_open(output_excel_file_path):
                os.system("taskkill /f /im excel.exe")
                print("Excel file is closed.")

        time.sleep(2)
        output_df.to_excel(output_excel_file_path, index=False)
        print(f"\nColumn {col-2} data saved successfully in {output_excel_file_path}")

        # Save the excluded symbols with reasons to a separate text file for each column
        exclusion_file_name = f'Logs/excluded_symbols_column_{col-2}.txt'
        with open(exclusion_file_name, 'w') as exclusion_file:
            exclusion_file.write(f"Exclusion report for column {col-2}\n")
            exclusion_file.write("=" * 50 + "\n")
            for symbol, reason in excluded_symbols_with_reasons:
                exclusion_file.write(f"Symbol: {symbol}, Reason: {reason}\n")

        print(f"Excluded symbols with reasons logged in '{exclusion_file_name}'")

        if _sound_enabled:  # Only play sound if enabled
            winsound.PlaySound('sound.wav', winsound.SND_FILENAME)

    except Exception as e:
        print(f"Error processing column {col-2}: {e}")
    finally:
        processed_columns.add(col)
        if len(processed_columns) >= total_columns:
            print("All columns processed for today ... waiting for next day.")
            schedule.clear()  # Clear the current schedule for the day
            output_excel_file_path = generate_output_file_path()  # Generate new file for next day
            schedule_tasks()  


# Run the scheduler
def run_scheduler(stop_event):
    try:
        # Initial schedule setup
        schedule_tasks()
        
        # Set up file monitoring
        observer = setup_file_monitor(input_excel_file_path)
        
        print("Started monitoring for file changes...")
        
        # Main loop
        while not stop_event.is_set():
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                print(f"Error in scheduler loop: {e}")
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        if 'observer' in locals():
            observer.stop()
            observer.join()

if __name__ == "__main__":
    import threading
    stop_event = threading.Event()
    run_scheduler(stop_event)