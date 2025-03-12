# AddressBase Premium CSV Splitter

## Overview
This script processes **Ordnance Survey AddressBase Premium** ZIP files, extracts CSV files, and splits them into multiple CSV files based on their record identifiers. Each record type is written to its respective output file.

## How It Works
1. The script checks if the specified ZIP file exists.
2. It creates an output directory (`extracted/`) if it does not exist.
3. It reads the ZIP file, extracts CSV files, and processes each row.
4. Rows are categorized based on **record identifiers** (first two characters).
5. Each category is written to its corresponding CSV file in the output directory.
6. A summary of processed records is displayed at the end.

## Requirements
- Python 3.x
- Required modules:
  - `zipfile`
  - `csv`
  - `sys`
  - `os`
  - `time`
  - `tqdm` (for progress bar)
  - `StringIO` (for handling CSV reading)

## Installation
1. Install required dependencies (if not already installed):
   ```bash
   pip install tqdm
   ```
2. Place the `adressbase_prem_splitter.py` script in the same directory as the ZIP file.

## Usage
### Running the script
Run the script from the terminal or command prompt:
```bash
python adressbase_prem_splitter.py
```
By default, it processes:
```python
zip_filename = 'AB Premium Full Epoch 116.zip'
```
To process another ZIP file, update this line in the `main()` function.

### Changing the Output Directory
By default, the extracted files are saved in:
```python
dest = os.path.join(os.getcwd(), 'extracted')
```
Modify this to change the output location.

## Output Files
Each record type is written to a separate CSV file. Example output files:
- `ID10_Header_Records.csv` → Header Records
- `ID11_Street_Records.csv` → Street Records
- `ID15_StreetDesc_Records.csv` → Street Descriptor Records
- `ID21_BLPU_Records.csv` → BLPU Records
- `ID99_Trailer_Records.csv` → Trailer Records  
...and more.

The script displays a summary of records processed.

## Troubleshooting
- **File Not Found Error**: Ensure the ZIP file exists in the correct location.
- **Missing Dependencies**: Install `tqdm` using `pip install tqdm`.
- **SyntaxError or IndentationError**: Ensure correct Python indentation (4 spaces per level).

## License
This script is open-source and can be modified for personal or professional use.
