import pandas as pd
import multiprocessing
from functools import partial
import xml.etree.ElementTree as ET
import time
import os, psutil
import math
import sys
import threading

stop_thread = False #ensures exe is working

def process_element(element, element_names):
    data = {}
    sys.stdout.write("\rParsing element in columns...")
    for element_name in element_names:
        data[element_name] = element.find(element_name).text if element.find(element_name) is not None else ''
        sys.stdout.write(f"\rParsed element {element_name}: {data[element_name]}")
        
    return data

def show_progress():
    max_dots = 20
    while not stop_thread:
        for num_dots in range(1, max_dots + 1):
            sys.stdout.write(f"\rReading XML{'#' * num_dots}   ")
            sys.stdout.flush()
            sys.stdout.write(f"\rReading XML{'/' * num_dots}   ")
        sys.stdout.write("\rReading XML   ")
        sys.stdout.flush()

def main(input_xml_file, output_csv_file, element_names):
    num_processes = multiprocessing.cpu_count()

    pool = multiprocessing.Pool(processes=num_processes)
    show_progress_thread = threading.Thread(target=show_progress)
    show_progress_thread.daemon = True

    try:
        print(f"Reading XML File...\nThis will take a while\nProgram uses: {num_processes} CPU Cores\nCtrl+C to exit program")
        show_progress_thread.start()
        read_start = time.time()
        tree = ET.parse(input_xml_file)
        root = tree.getroot()
        end_read = str(round(((read_start - time.time())/60)*-1,2))
        
        global stop_thread
        stop_thread = True
        show_progress_thread.join()
        print("")

        print("File was read in "+ end_read + " minutes successfully\nConversion started")
        process_func = partial(process_element, element_names=element_names)
        rows = pool.map(process_func, root)

        print(f"RAM used:{(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2) / 1024} GB")

        pool.close()
        pool.join()

        df = pd.DataFrame(rows)

        df.to_csv(output_csv_file, quotechar='"', sep=";", index=True,decimal=",", na_rep="NULL")
        end_time = time.time()
        elapsed_time = end_time - start_time
        if (elapsed_time / 60) > 1:
            elapsed_time = str(round((elapsed_time / 60),2)) + " Minutes"
        print("Successful at " + elapsed_time + " . Press any key to exit")
    except KeyboardInterrupt:
        print("KeyboardInterrupt: Exiting the program. Please wait until data is saved")
        pool.terminate()
        pool.join()
    finally:
        pool.terminate()
        pool.join()

if __name__ == "__main__":
    try:
        multiprocessing.freeze_support()
        start_time = time.time()
        print("Program start at " + str(round(start_time,2)))
        
        input_xml_file = 'data.xml'
        output_csv_file = 'data.csv'
        element_names = ['elementA', 'elementB', 'elementC', 'elementD', 'elementE']
        
        main(input_xml_file, output_csv_file, element_names)
        input("Press Enter to exit")  
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        input("Press Enter to exit")

