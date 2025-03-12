# Imported modules
import zipfile  # Handles ZIP file extraction
import csv  # Handles reading and writing CSV files
import sys  # Provides system-specific functions and parameters
import os  # Provides functions for interacting with the file system
from io import StringIO  # Handles in-memory text streams
import time  # Handles time-related functions
from time import strftime  # Formats time for display
from tqdm import tqdm  # Displays progress bars for loops

# Dictionary to map record types to their corresponding filenames, headers, and descriptions
files_dict = {
    "write_10": {
        "filename": "ID10_Header_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CUSTODIAN_NAME","LOCAL_CUSTODIAN_NAME","PROCESS_DATE","VOLUME_NUMBER","ENTRY_DATE","TIME_STAMP","VERSION","FILE_TYPE"],
        "description": "Header Records"},
    "write_11": {
        "filename": "ID11_Street_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","USRN","RECORD_TYPE","SWA_ORG_REF_NAMING","STATE","STATE_DATE","STREET_SURFACE","STREET_CLASSIFICATION","VERSION","STREET_START_DATE","STREET_END_DATE","LAST_UPDATE_DATE","RECORD_ENTRY_DATE","STREET_START_X","STREET_START_Y","STREET_START_LAT","STREET_START_LONG","STREET_END_X","STREET_END_Y","STREET_END_LAT","STREET_END_LONG","STREET_TOLERANCE"],
        "description": "Street Records"},
    "write_15": {
        "filename": "ID15_StreetDesc_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","USRN","STREET_DESCRIPTION","LOCALITY_NAME","TOWN_NAME","ADMINSTRATIVE_AREA","LANGUAGE","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE"],
        "description": "Street Descriptor Records"},
    "write_21": {
        "filename": "ID21_BLPU_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","LOGICAL_STATUS","BLPU_STATE","BLPU_STATE_DATE","PARENT_UPRN","X_COORDINATE","Y_COORDINATE","LATITUDE","LONGITUDE","RPC","LOCAL_CUSTODIAN_CODE","COUNTRY","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE","ADDRESSBASE_POSTAL","POSTCODE_LOCATOR","MULTI_OCC_COUNT"],
        "description": "BLPU Records"},
    "write_23": {
        "filename": "ID23_XREF_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","XREF_KEY","CROSS_REFERENCE","VERSION","SOURCE","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE"],
        "description": "XRef Records"},
    "write_24": {
        "filename": "ID24_LPI_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","LPI_KEY","LANGUAGE","LOGICAL_STATUS","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE","SAO_START_NUMBER","SAO_START_SUFFIX","SAO_END_NUMBER","SAO_END_SUFFIX","SAO_TEXT","PAO_START_NUMBER","PAO_START_SUFFIX","PAO_END_NUMBER","PAO_END_SUFFIX","PAO_TEXT","USRN","USRN_MATCH_INDICATOR","AREA_NAME","LEVEL","OFFICIAL_FLAG"],
        "description": "LPI Records"},
    "write_28": {
        "filename": "ID28_DPA_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","UDPRN","ORGANISATION_NAME","DEPARTMENT_NAME","SUB_BUILDING_NAME","BUILDING_NAME","BUILDING_NUMBER","DEPENDENT_THOROUGHFARE","THOROUGHFARE","DOUBLE_DEPENDENT_LOCALITY","DEPENDENT_LOCALITY","POST_TOWN","POSTCODE","POSTCODE_TYPE","DELIVERY_POINT_SUFFIX","WELSH_DEPENDENT_THOROUGHFARE","WELSH_THOROUGHFARE","WELSH_DOUBLE_DEPENDENT_LOCALITY","WELSH_DEPENDENT_LOCALITY","WELSH_POST_TOWN","PO_BOX_NUMBER","PROCESS_DATE","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE"],
        "description": "Delivery Point Records"},
    "write_29": {
        "filename": "ID29_Metadata_Records.csv", 
        "header": ["RECORD_IDENTIFIER","GAZ_NAME","GAZ_SCOPE","TER_OF_USE","LINKED_DATA","GAZ_OWNER","NGAZ_FREQ","CUSTODIAN_NAME","CUSTODIAN_UPRN","LOCAL_CUSTODIAN_CODE","CO_ORD_SYSTEM","CO_ORD_UNIT","META_DATE","CLASS_SCHEME","GAZ_DATE","LANGUAGE","CHARACTER_SET"],
        "description": "Metadata Records"},
    "write_30": {
        "filename": "ID30_Successor_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","SUCC_KEY","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE","SUCCESSOR"],
        "description": "Successor Records"},
    "write_31": {
        "filename": "ID31_Org_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","ORG_KEY","ORGANISATION","LEGAL_NAME","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE"],
        "description": "Organisation Records"},
    "write_32": {
        "filename": "ID32_Class_Records.csv", 
        "header": ["RECORD_IDENTIFIER","CHANGE_TYPE","PRO_ORDER","UPRN","CLASS_KEY","CLASSIFICATION_CODE","CLASS_SCHEME","SCHEME_VERSION","START_DATE","END_DATE","LAST_UPDATE_DATE","ENTRY_DATE"],
        "description": "Classification Records"},
    "write_99": {
        "filename": "ID99_Trailer_Records.csv", 
        "header": ["RECORD_IDENTIFIER","NEXT_VOLUME_NUMBER","RECORD_COUNT","ENTRY_DATE","TIME_STAMP"],
        "description": "Trailer Records"}
}

def createCSV(zip_filename, dest_directory):
    """
    Extracts OS AddressBase Premium ZIP CSV file, reads its content,
    and splits records by their identifiers into separate CSV files.
    """
    print('Splitting OS AddressBase Premium CSV files by record identifier.')
    starttime = time.time()

    # Check if the zip file exists
    if not os.path.exists(zip_filename):
        print(f'Error: {zip_filename} does not exist.')
        sys.exit()
    else:
        print('File exists.')

    # Create destination directory if it doesn't exist
    if not os.path.exists(dest_directory):
        print('Creating destination folder...')
        os.makedirs(dest_directory)
    else:
        print('Destination folder exists.')

    # Dictionary to store file and writer objects
    file_objects = {}
    writer_objects = {}

    # Loop through dictionary to create CSV files and write headers
    for obj_name, file_info in files_dict.items():
        filename = os.path.join(dest_directory, file_info["filename"])
        header = file_info["header"]

        # Delete existing file if it exists
        if os.path.isfile(filename):
            os.remove(filename)

        # Open file for writing
        file_obj = open(filename, 'a', encoding='utf-8')
        writer = csv.writer(file_obj, delimiter=',', quotechar='"', lineterminator='\n')

        # Write header to the file
        writer.writerow(header)

        # Store file and writer objects for later use
        file_objects[obj_name] = file_obj
        writer_objects[obj_name] = writer

    # Counters to track the number of records for each file type
    counters = {key: 0 for key in files_dict.keys()}

    # Open ZIP file and process each CSV file inside
    with zipfile.ZipFile(zip_filename, 'r') as z:
        filenames = z.namelist()  # Get list of files in the ZIP archive

        # Progress bar to track file processing
        with tqdm(total=len(filenames), desc="Processing files", unit="file") as pbar:
            for filename in filenames:
                print(f'Processing: {filename}')

                # Read file content from ZIP
                with z.open(filename) as f:
                    for row in csv.reader(StringIO(f.read().decode('utf-8'))):
                        row_type = row[0][0:2]  # Extract the record identifier

                        # Determine where to write the row based on the identifier
                        if row_type in ["10", "11", "15", "21", "23", "24", "28", "29", "30", "31", "32", "99"]:
                            # write the row to the corresponding CSV file
                            writer_objects[f"write_{row_type}"].writerow(row)
                            # increment the counter for the corresponding CSV file
                            counters[f"write_{row_type}"] += 1

                pbar.update(1)  # Update progress bar after processing each file

    # Close all open file objects
    for file_obj in file_objects.values():
        file_obj.close()

    endtime = time.time()
    elapsed = endtime - starttime  # Calculate elapsed time

    # Summary statistics of the processed records
    print("\nSplitting complete. Summary:")
    print(f'Completed at {strftime("%a, %d %b %Y %H:%M:%S")}')
    print(f'Elapsed time: {round(elapsed / 60, 1)} minutes')

    # Print count of records processed for each category using descriptions
    for key, count in counters.items():
        description = files_dict[key]["description"]
        print(f'Number of {description}: {count:,}')

def main():
    """
    Main function to define the input ZIP filename and output directory,
    then call the CSV splitting function.
    """
    zip_filename = 'AB Premium Full Epoch 116.zip'
    #zip_filename = 'AB Premium Islands Full Epoch 116.zip'
    dest = os.path.join(os.getcwd(), 'extracted')  # Destination folder
    createCSV(zip_filename, dest)  # Call function to process ZIP file

if __name__ == '__main__':
    main()
