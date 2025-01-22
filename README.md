# Hospital Dataset Processor

This project is a Python-based script that processes hospital-related datasets from the [CMS API](https://data.cms.gov). The script fetches, cleans, and saves datasets as CSV files while keeping track of processed datasets using metadata. 

---

## Features

- **API Integration:** Fetches datasets directly from the CMS API.
- **Data Cleaning:** Converts dataset column names to `snake_case` for standardization.
- **Parallel Processing:** Processes multiple datasets simultaneously for optimized performance.
- **Metadata Tracking:** Tracks processed datasets to avoid redundant work in future runs.
- **Error Handling:** Logs errors during dataset processing for debugging and retry.

---

## Prerequisites

Ensure the following are installed on your system:
1. **Python 3.7 or above**
2. Required Python libraries:
   - `os`
   - `re`
   - `json`
   - `requests`
   - `datetime`
   - `pytz`
   - `logging`
   - `pandas`
   - `concurrent.futures`

You can install the necessary libraries using:
```bash
pip install requests pandas pytz
