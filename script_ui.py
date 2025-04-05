# script_ui.py
import tkinter as tk
from tkinter import scrolledtext, ttk, font
import sys
import threading
import queue
import schedule
import time
import script_main

class StdoutRedirector:
    def __init__(self, text_widget, queue):
        self.text_widget = text_widget
        self.queue = queue

    def write(self, message):
        self.queue.put(message)

    def flush(self):
        pass

class ScriptControllerUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Stock Scanner Controller")
        self.root.geometry("800x600")
        
        # Create a flag to track script status
        self.script_running = False
        self.script_thread = None
        self.observer = None
        
        # Create message queue for thread-safe logging
        self.log_queue = queue.Queue()
        
        # Sound control variable
        self.sound_enabled = tk.BooleanVar(value=True)
        
        # Create custom fonts
        self.heading_font = font.Font(family="Helvetica", size=16, weight="bold")
        self.button_font = font.Font(family="Helvetica", size=12)
        
        self.create_widgets()
        self.setup_logging()
        
        # Start checking the log queue
        self.check_log_queue()
        
        # Create an event to control the script
        self.stop_event = threading.Event()

    def create_widgets(self):
        # Add main heading
        heading_label = ttk.Label(
            self.root,
            text="Stock Market Scanner Control Panel",
            font=self.heading_font
        )
        heading_label.pack(pady=15)
        
        # Create control frame with centered content
        control_frame = ttk.Frame(self.root)
        control_frame.pack(fill=tk.X, padx=20, pady=10)
        
        # Center the buttons by using a frame inside the control frame
        button_frame = ttk.Frame(control_frame)
        button_frame.pack(anchor=tk.CENTER)
        
        # Create Start button with description
        start_frame = ttk.Frame(button_frame)
        start_frame.pack(pady=8, fill=tk.X)
        
        self.start_button = ttk.Button(
            start_frame, 
            text="▶ Start Scanner",
            width=30,
            command=self.start_script,
            style='Success.TButton'
        )
        self.start_button.pack(side=tk.TOP)
        
        start_desc = ttk.Label(
            start_frame,
            text="Begins monitoring the market for stock opportunities",
            font=("Helvetica", 10),
            foreground="gray24"
        )
        start_desc.pack(side=tk.TOP)
        
        # Create Stop button with description
        stop_frame = ttk.Frame(button_frame)
        stop_frame.pack(pady=8, fill=tk.X)
        
        self.stop_button = ttk.Button(
            stop_frame, 
            text="⏹  Stop Scanner",
            width=30,
            command=self.stop_script,
            state=tk.DISABLED,
            style='Danger.TButton'
        )
        self.stop_button.pack(side=tk.TOP)
        
        stop_desc = ttk.Label(
            stop_frame,
            text="Safely stops all scanning operations",
            font=("Helvetica", 10),
            foreground="gray24"
        )
        stop_desc.pack(side=tk.TOP)
        
        # Create Clear button with description
        clear_frame = ttk.Frame(button_frame)
        clear_frame.pack(pady=8, fill=tk.X)
        
        self.clear_button = ttk.Button(
            clear_frame, 
            text="🗑 Clear Log Window",
            width=30,
            command=self.clear_log
        )
        self.clear_button.pack(side=tk.TOP)
        
        clear_desc = ttk.Label(
            clear_frame,
            text="Erases all messages from the log display",
            font=("Helvetica", 10),
            foreground="gray24"
        )
        clear_desc.pack(side=tk.TOP)
        
        # Create Sound checkbox with description
        sound_frame = ttk.Frame(button_frame)
        sound_frame.pack(pady=8, fill=tk.X)
        
        self.sound_checkbox = ttk.Checkbutton(
            sound_frame,
            text="🔊 Enable Alert Sounds",
            variable=self.sound_enabled,
            command=self.toggle_sound
        )
        self.sound_checkbox.pack(side=tk.TOP)
        
        sound_desc = ttk.Label(
            sound_frame,
            text="Plays notification sounds when process is complete",
            font=("Helvetica", 10),
            foreground="gray24"
        )
        sound_desc.pack(side=tk.TOP)
        
        # Create frame for log window
        log_frame = ttk.LabelFrame(self.root, text="Scanner Log")
        log_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=10)
        
        # Create scrolled text widget for logging
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            height=15,
            wrap=tk.WORD,
            bg='white',
            fg='dark green',
            font=("Courier", 10)
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def toggle_sound(self):
        # Update script_main's sound setting
        script_main.set_sound_enabled(self.sound_enabled.get())
        print(f"Sound {'enabled' if self.sound_enabled.get() else 'disabled'}")

    def setup_logging(self):
        # Redirect stdout to our custom handler
        sys.stdout = StdoutRedirector(self.log_text, self.log_queue)

    def check_log_queue(self):
        # Check for new log messages
        while True:
            try:
                message = self.log_queue.get_nowait()
                self.log_text.insert(tk.END, message)
                self.log_text.see(tk.END)
                self.log_text.update_idletasks()
            except queue.Empty:
                break
        
        # Schedule the next check
        self.root.after(100, self.check_log_queue)

    def run_script(self):
        try:
            script_main.set_sound_enabled(self.sound_enabled.get())
            script_main.run_scheduler(self.stop_event)
        except Exception as e:
            print(f"Error in script execution: {e}")
        finally:
            self.root.after(0, self.reset_buttons)

    def start_script(self):
        if not self.script_running:
            self.script_running = True
            self.start_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            
            # Clear the stop event
            self.stop_event.clear()
            
            # Start the script in a separate thread
            self.script_thread = threading.Thread(target=self.run_script)
            self.script_thread.daemon = True
            self.script_thread.start()
            
            print("Scanner started successfully!")

    def stop_script(self):
        if self.script_running:
            print("Scanner Stopped...")
            self.stop_event.set()
            self.script_running = False
            
            # Disable stop button while stopping
            self.stop_button.config(state=tk.DISABLED)
            
            # Enable start button after a short delay
            self.root.after(1000, self.reset_buttons)

    def reset_buttons(self):
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.script_running = False

    def clear_log(self):
        self.log_text.delete(1.0, tk.END)

    def on_closing(self):
        if self.script_running:
            self.stop_script()
        self.root.destroy()

def main():
    root = tk.Tk()
    
    # Configure styles
    style = ttk.Style()
    style.configure('Success.TButton', foreground='green', font=("Helvetica", 12))
    style.configure('Danger.TButton', foreground='red', font=("Helvetica", 12))
    style.configure('TButton', font=("Helvetica", 12))
    style.configure('TCheckbutton', font=("Helvetica", 12))
    
    app = ScriptControllerUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()

if __name__ == "__main__":
    main()