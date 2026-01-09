

# VTEX Image Alt Text Updater (SEO Automation)

This Python application automates the process of updating "Alt Text" (alternative text) for product images on the VTEX e-commerce platform. It is designed to enhance Search Engine Optimization (SEO) by applying natural language normalization to product names and updating the corresponding image labels.

The system is engineered for high performance and resilience, featuring multi-threading, automatic retry strategies, rate limiting, and a state-preservation mechanism (checkpointing) to handle bulk operations efficiently.

## Key Features

* **Concurrent Processing:** Utilizes `ThreadPoolExecutor` to process multiple SKUs simultaneously, significantly reducing execution time for large catalogs.
* **Resilient Network Layer:** Implements a custom HTTP adapter with exponential backoff and automatic retries for handling transient network errors (HTTP 429, 50x).
* **Smart Rate Limiting:** Includes a thread-safe rate limiter to comply with VTEX API quotas and prevent IP blocking.
* **Checkpoint System:** Automatically saves progress to a JSON file. If the process is interrupted, it can resume exactly where it left off without reprocessing completed SKUs.
* **Idempotency:** Checks existing Alt Text before sending update requests. If the image is already optimized, it skips the API call to save resources.
* **Comprehensive Logging:** Generates detailed execution and error logs for auditing and debugging purposes.

## Prerequisites

* Python 3.8 or higher.
* A valid VTEX Admin session cookie (VtexIdclientAutCookie).

## Installation

1. **Clone the repository:**
```bash
git clone https://github.com/your-username/vtex-seo-updater.git
cd vtex-seo-updater

```


2. **Install dependencies:**
This project uses `requests`. You can install it via pip:
```bash
pip install requests

```



## Configuration

### 1. Environment Variables (Security Best Practice)

To avoid hardcoding sensitive credentials, this application looks for the VTEX authentication cookie in the environment variables.

**Linux/Mac:**

```bash
export VTEX_COOKIE="your_actual_cookie_value_here"

```

**Windows (PowerShell):**

```powershell
$env:VTEX_COOKIE="your_actual_cookie_value_here"

```

### 2. Account Configuration

Open the main script and verify the `ACCOUNT_NAME` variable matches your VTEX account:

```python
# --- CONFIGURATION ---
ACCOUNT_NAME = "bemol" # Update this if deploying for a different account

```

### 3. Input Data

Create a file named `sku_ids.txt` in the root directory. Add the list of SKU IDs you wish to process, one per line.

**Example `sku_ids.txt`:**

```text
1001
1002
1003
# Comments are ignored
1004

```

## Usage

Run the script directly from the terminal:

```bash
python main.py

```

### Execution Flow

1. The script validates the existence of `sku_ids.txt`.
2. It checks for a `checkpoint.json` file.
3. You will be prompted to confirm the execution and whether to resume from the last checkpoint.
4. The application processes SKUs using the configured number of worker threads.
5. Logs are streamed to the console and saved to `execution_log.txt`.

## Project Structure

```text
.
├── main.py              # Core application logic
├── sku_ids.txt          # Input file containing SKU IDs
├── checkpoint.json      # State file (auto-generated)
├── execution_log.txt    # General operational logs (auto-generated)
├── error_log.txt        # Error-specific logs (auto-generated)
└── README.md            # Documentation

```

## DevOps & Performance Tuning

The application exposes several constants at the top of the script that can be tuned based on the environment and API limits:

* **MAX_WORKERS:** Controls the number of parallel threads. Default is `3`. Increase cautiously to avoid hitting API rate limits.
* **REQUEST_TIMEOUT:** Duration (in seconds) to wait for a response.
* **MAX_RETRIES:** Number of retry attempts for failed requests.
* **RATE_LIMIT_DELAY:** Minimum delay (in seconds) between requests to ensure compliance with VTEX API governance.

## Troubleshooting

* **HTTP 401 (Unauthorized):** Your `VTEX_COOKIE` has likely expired. Generate a new cookie via the browser developer tools (Network tab) or the VTEX CLI and update your environment variable.
* **HTTP 429 (Too Many Requests):** The script automatically handles this by sleeping for the duration specified in the `Retry-After` header. If this persists, increase `RATE_LIMIT_DELAY` or decrease `MAX_WORKERS`.

## Disclaimer

This tool interacts directly with the VTEX Catalog API. Always test with a small batch of SKUs in a staging environment before running a bulk update on production.

---


