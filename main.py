import requests
import time
import os
from datetime import datetime
from typing import Optional, Tuple, List, Dict
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION ---
ACCOUNT_NAME = "bemol"
VTEX_COOKIE = os.getenv("VTEX_COOKIE", "paste your VTEX cookie here")  #Paste your VTEX cookie here
# URLs
BASE_URL = f"https://{ACCOUNT_NAME}.vtexcommercestable.com.br/api/catalog/pvt"
LOG_FILE = "execution_log.txt"
ERROR_LOG = "error_log.txt"
CHECKPOINT_FILE = "checkpoint.json"
SKU_LIST_FILE = "sku_ids.txt"  # Arquivo com lista de SKUs

HEADERS = {
    "VtexIdclientAutCookie": VTEX_COOKIE,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# --- PERFORMANCE AND SECURITY CONFIGURATION ---
MAX_WORKERS = 3
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_FACTOR = 1
RATE_LIMIT_DELAY = 0.3
CHECKPOINT_INTERVAL = 10

# Lock for thread-safe writing to files
log_lock = threading.Lock()



# --- SESSION WITH AUTOMATIC RETRY ---
def create_session() -> requests.Session:
    """Creates a session with automatic retry to handle temporary failures."""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "PUT", "POST"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    return session

SESSION = create_session()

# --- UTILS ---
def log_message(message: str, level: str = "INFO"):
    """Thread-safe logging with severity levels."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] [{level}] {message}"
    
    with log_lock:
        print(formatted_msg)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(formatted_msg + "\n")
            
            if level in ["ERROR", "CRITICAL"]:
                with open(ERROR_LOG, "a", encoding="utf-8") as f:
                    f.write(formatted_msg + "\n")
        except Exception as e:
            print(f"Error writing log: {e}")

def normalize_product_name(name: str) -> str:
    """Normalizes product name for natural alt text."""
    if not name:
        return ""
    
    # Remove excess whitespace and convert to lowercase
    name = " ".join(name.split()).lower()
    
    # Remove special characters but keep spaces and accents
    name = name.replace("  ", " ").strip()
    
    return name

def generate_natural_alt_text(product_name: str, image_index: int, total_images: int) -> str:
    """
    Generates natural language alt text for SEO.
    Simply returns the normalized product name.
    """
    normalized_name = normalize_product_name(product_name)
    
    if not normalized_name:
        return f"produto farmacêutico"
    
    return normalized_name

# --- CHECKPOINT SYSTEM ---
class CheckpointManager:
    """Manages checkpoints to resume processing."""
    
    def __init__(self, filename: str = CHECKPOINT_FILE):
        self.filename = filename
        self.data = self.load()
    
    def load(self) -> Dict:
        """Loads existing checkpoint."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    return json.load(f)
            except:
                return {"processed_skus": []}
        return {"processed_skus": []}
    
    def save(self):
        """Saves checkpoint."""
        with log_lock:
            try:
                with open(self.filename, 'w') as f:
                    json.dump(self.data, f)
            except Exception as e:
                log_message(f"Error saving checkpoint: {e}", "ERROR")
    
    def mark_processed(self, sku_id: int):
        """Marks SKU as processed."""
        if sku_id not in self.data["processed_skus"]:
            self.data["processed_skus"].append(sku_id)
    
    def is_processed(self, sku_id: int) -> bool:
        """Checks if SKU has already been processed."""
        return sku_id in self.data["processed_skus"]
    
    def clear(self):
        """Clears checkpoint."""
        self.data = {"processed_skus": []}
        self.save()

# --- RATE LIMITER ---
class RateLimiter:
    """Controls request rate."""
    
    def __init__(self, delay: float = RATE_LIMIT_DELAY):
        self.delay = delay
        self.last_request = 0
        self.lock = threading.Lock()
    
    def wait(self):
        """Waits before making the next request."""
        with self.lock:
            elapsed = time.time() - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
            self.last_request = time.time()

rate_limiter = RateLimiter()

# --- SKU LIST LOADER ---
def load_sku_list(filename: str = SKU_LIST_FILE) -> List[int]:
    """
    Loads SKU IDs from text file.
    File format: one SKU ID per line.
    """
    if not os.path.exists(filename):
        log_message(f"ERROR: File {filename} not found!", "CRITICAL")
        return []
    
    sku_ids = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                try:
                    sku_id = int(line)
                    sku_ids.append(sku_id)
                except ValueError:
                    log_message(f"Invalid SKU ID on line {line_num}: {line}", "WARNING")
        
        log_message(f"Loaded {len(sku_ids)} SKU IDs from {filename}")
        return sku_ids
    
    except Exception as e:
        log_message(f"Error reading SKU list: {e}", "CRITICAL")
        return []

# --- API FUNCTIONS ---
def safe_request(method: str, url: str, **kwargs) -> Optional[requests.Response]:
    """Makes a request with rate limiting, timeout, and error handling."""
    rate_limiter.wait()
    
    try:
        kwargs.setdefault('timeout', REQUEST_TIMEOUT)
        kwargs.setdefault('headers', HEADERS)
        
        response = SESSION.request(method, url, **kwargs)
        
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            log_message(f"Rate limit hit. Waiting {retry_after}s...", "WARNING")
            time.sleep(retry_after)
            return safe_request(method, url, **kwargs)
        
        return response
        
    except requests.exceptions.Timeout:
        log_message(f"Timeout on {method} {url}", "ERROR")
        return None
    except requests.exceptions.ConnectionError as e:
        log_message(f"Connection error: {e}", "ERROR")
        return None
    except Exception as e:
        log_message(f"Unexpected error: {e}", "ERROR")
        return None

def update_image_alt(sku_id: int, original_image_data: Dict, new_alt_text: str) -> bool:
    """Updates the image alt text."""
    file_id = original_image_data.get('Id')
    url = f"{BASE_URL}/stockkeepingunit/{sku_id}/file/{file_id}"
    
    payload = original_image_data.copy()
    payload["Label"] = new_alt_text
    payload["Text"] = new_alt_text
    
    response = safe_request('PUT', url, json=payload)
    
    if response and response.status_code == 200:
        log_message(f"      [OK] Image updated: '{new_alt_text}'")
        return True
    elif response and response.status_code == 401:
        log_message(f"      [AUTH ERROR] Cookie expired.", "CRITICAL")
        return False
    else:
        error_msg = response.text if response else "No response"
        log_message(f"      [UPDATE ERROR] SKU {sku_id}: {error_msg}", "ERROR")
        return False

def process_sku_images(sku_id: int, product_name: str, checkpoint: CheckpointManager) -> bool:
    """Processes all images for a SKU with natural language alt text."""
    url_get = f"{BASE_URL}/stockkeepingunit/{sku_id}/file"
    
    response = safe_request('GET', url_get)
    
    if not response:
        return False
    
    if response.status_code == 200:
        images = response.json()
        
        if not images:
            # Sem imagens = considerado processado
            checkpoint.mark_processed(sku_id)
            return True

        # Check if ALL images already have alt text
        all_have_alt = all(
            img.get('Label') and img.get('Label').strip() 
            for img in images
        )
        
        if all_have_alt:
            log_message(f"      [SKIP SKU] All images already have alt text - SKU {sku_id}")
            # Marcar como processado pois já está correto
            checkpoint.mark_processed(sku_id)
            return True

        total_images = len(images)
        success = True

        for index, img in enumerate(images, start=1):
            current_label = img.get('Label', '')
            new_alt = generate_natural_alt_text(product_name, index, total_images)
            
            if current_label != new_alt:
                if not update_image_alt(sku_id, img, new_alt):
                    success = False
            else:
                log_message(f"      [SKIP] Already correct: {new_alt}")
        
        # Só marca como processado se teve sucesso na atualização
        if success:
            checkpoint.mark_processed(sku_id)
        
        return success
    
    elif response.status_code == 404:
        # 404 = SKU não existe ou sem imagens, considerar processado
        checkpoint.mark_processed(sku_id)
        return True
    else:
        log_message(f"[GET ERROR] SKU {sku_id} - Status: {response.status_code}", "ERROR")
        return False

def get_sku_details(sku_id: int) -> Tuple[Optional[str], Optional[str]]:
    """Retrieves SKU details."""
    url = f"{BASE_URL}/stockkeepingunit/{sku_id}"
    
    response = safe_request('GET', url)
    
    if response and response.status_code == 200:
        data = response.json()
        name = data.get('ProductName') or data.get('NameComplete') or data.get('Name')
        ref_id = data.get('RefId')
        return name, ref_id
    
    return None, None

def process_single_sku(sku_id: int, checkpoint: CheckpointManager) -> bool:
    """Processes a single SKU."""
    if checkpoint.is_processed(sku_id):
        log_message(f"SKU {sku_id} already processed (checkpoint)", "INFO")
        return True
    
    product_name, ref_id = get_sku_details(sku_id)
    
    if product_name:
        log_message(f"SKU ID: {sku_id} | RefId: {ref_id} | Product: {product_name}")
        # Passar checkpoint para process_sku_images
        success = process_sku_images(sku_id, product_name, checkpoint)
        return success
    else:
        log_message(f"SKU ID: {sku_id} | Ignored (no details)", "WARNING")
        # SKU sem detalhes = considerar processado para não tentar novamente
        checkpoint.mark_processed(sku_id)
        return True

def remove_processed_skus_from_file(processed_skus: List[int], filename: str = SKU_LIST_FILE):
    """Removes processed SKUs from the input file."""
    if not os.path.exists(filename):
        return

    try:
        # Create a set for faster lookup
        processed_set = set(processed_skus)
        
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        remaining_lines = []
        removed_count = 0
        
        for line in lines:
            stripped = line.strip()
            
            # Keep comments and empty lines
            if not stripped or stripped.startswith('#'):
                remaining_lines.append(line)
                continue
            
            try:
                sku_id = int(stripped)
                if sku_id in processed_set:
                    removed_count += 1
                else:
                    remaining_lines.append(line)
            except ValueError:
                remaining_lines.append(line)
        
        if removed_count > 0:
            with open(filename, 'w', encoding='utf-8') as f:
                f.writelines(remaining_lines)
            log_message(f"Removed {removed_count} processed SKUs from {filename}")
        else:
            log_message("No SKUs removed from file.")
            
    except Exception as e:
        log_message(f"Error updating SKU file: {e}", "ERROR")

# --- RUNNER ---
def run_bulk_update(resume: bool = True):
    """Executes bulk update from SKU list file."""
    checkpoint = CheckpointManager()
    
    if not resume:
        checkpoint.clear()
        log_message("Starting fresh (checkpoint cleared)")
    
    # Load SKU list from file
    sku_ids = load_sku_list(SKU_LIST_FILE)
    
    if not sku_ids:
        log_message("No SKUs to process. Check your sku_ids.txt file.", "CRITICAL")
        return
    
    log_message(f"--- STARTING BEMOL FARMA UPDATE (NATURAL ALT TEXT) ---")
    log_message(f"Total SKUs to process: {len(sku_ids)}")
    log_message(f"Max workers: {MAX_WORKERS}")
    
    processed_count = 0

    try:
        # Parallel processing with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_single_sku, sku_id, checkpoint): sku_id 
                for sku_id in sku_ids
            }
            
            for future in as_completed(futures):
                processed_count += 1
                
                # Save checkpoint periodically
                if processed_count % CHECKPOINT_INTERVAL == 0:
                    checkpoint.save()
                    # Remover SKUs processados periodicamente
                    remove_processed_skus_from_file(checkpoint.data["processed_skus"])
                    log_message(f"Checkpoint saved ({processed_count}/{len(sku_ids)} SKUs processed)")
        
        checkpoint.save()
    
    except KeyboardInterrupt:
        log_message("Process interrupted by user. Saving checkpoint...", "WARNING")
        checkpoint.save()
        remove_processed_skus_from_file(checkpoint.data["processed_skus"])
    except Exception as e:
        log_message(f"Fatal Error: {e}", "CRITICAL")
        checkpoint.save()
        remove_processed_skus_from_file(checkpoint.data["processed_skus"])
    
    # Always remove processed SKUs at the end
    remove_processed_skus_from_file(checkpoint.data["processed_skus"])
    
    log_message(f"--- PROCESS COMPLETED ({processed_count}/{len(sku_ids)} SKUs processed) ---")

# --- MAIN ---
if __name__ == "__main__":
    if "PASTE_YOUR_COOKIE_HERE" in VTEX_COOKIE:
        print("⚠️ ALERT: Paste your cookie in the VTEX_COOKIE variable.")
    else:
        print("=" * 60)
        print("VTEX IMAGE ALT TEXT UPDATER - NATURAL LANGUAGE SEO")
        print("=" * 60)
        print(f"SKU List File: {SKU_LIST_FILE}")
        print(f"Max Workers: {MAX_WORKERS}")
        print(f"Rate Limit Delay: {RATE_LIMIT_DELAY}s")
        print(f"Request Timeout: {REQUEST_TIMEOUT}s")
        print("=" * 60)
        
        if not os.path.exists(SKU_LIST_FILE):
            print(f"\n⚠️ ERROR: File '{SKU_LIST_FILE}' not found!")
            print(f"Create a file named '{SKU_LIST_FILE}' in the root directory")
            print("with one SKU ID per line.")
            print("\nExample:")
            print("12345")
            print("67890")
            print("# This is a comment")
            print("11111")
        else:
            resume = input("\nResume from checkpoint? (Y/n): ").strip().lower() != 'n'
            confirm = input("Type 'YES' to start: ")
            
            if confirm == "YES":
                run_bulk_update(resume=resume)