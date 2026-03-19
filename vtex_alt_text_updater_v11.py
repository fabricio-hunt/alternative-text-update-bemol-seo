#.\.venv\Scripts\Activate.ps1
import re
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
from dataclasses import dataclass
from enum import Enum, auto

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ImportError:
    pass


# =============================================================================
# VTEX IMAGE ALT TEXT UPDATER — LOCAL (v11)
# =============================================================================
#
# Correções aplicadas (v11):
#
#   [FIX 1 - _is_dirty_content: CRITÉRIO DE TAMANHO REMOVIDO]
#     O critério 3 da v10 (len < 5 e sem espaço) causava falsos positivos em
#     nomes legítimos como "led", "tv", "kit", disparando atualização em loop
#     quando o alt text gerado também era curto. Removido.
#     O critério 2 (blacklist) foi mantido mas restrito a correspondência
#     exata de palavra isolada — evita false positive em frases como
#     "foto da câmera principal" que contém "foto" como substring.
#
#   [FIX 2 - update_image_alt: MÁQUINA DE ESTADOS EXPLÍCITA]
#     A lógica de if/elif aninhados com sobrescrita da variável `response`
#     entre tentativas causava dois bugs:
#       (a) Um status_code não previsto em t2 não ativava t3 corretamente.
#       (b) Erros definitivos (ex: 422) não interrompiam as tentativas.
#     Solução: loop sobre lista de estratégias com `RETRY_ON = {400, 404, 405}`.
#     Erros fora desse conjunto interrompem as tentativas imediatamente.
#     AUTH_ERROR (401/403) sinaliza abort para o runner principal.
#
#   [FIX 3 - update_image_alt: AUTH_ERROR PROPAGADO ATÉ O RUNNER]
#     Na v10, 401/403 dentro de update_image_alt retornava False, que era
#     tratado como falha genérica. O runner nunca recebia o sinal de abort.
#     Solução: retorna a string "AUTH_ERROR". process_sku_images detecta e
#     retorna "AUTH_ERROR". process_single_sku detecta e sinaliza abort
#     (deve_abortar=True) para o runner principal.
#
#   [FIX 4 - process_sku_images: SKIP_405 NÃO ABANDONA IMAGENS RESTANTES]
#     Na v10, ao receber SKIP_405 na primeira imagem o loop fazia `break`,
#     deixando as imagens restantes sem atualização. O SKIP_405 é por SKU,
#     não por imagem — qualquer imagem com 405 em todas as estratégias
#     indica problema no SKU inteiro, então o break é correto, mas
#     _register_skipped_405 e checkpoint.mark_processed devem ser chamados
#     no nível do SKU (já estava assim) para evitar reprocessamento infinito.
#     NOVO: imagens anteriores ao SKIP_405 que já foram atualizadas com
#     sucesso são contabilizadas corretamente no log de razão.
#
#   [FIX 5 - CheckpointManager: GRAVAÇÃO ATÔMICA VIA ARQUIVO TEMPORÁRIO]
#     Na v10, uma falha/crash durante a escrita do checkpoint.json corrompía
#     o arquivo (JSON truncado). Na próxima execução o checkpoint era
#     descartado e SKUs já processados eram reprocessados.
#     Solução: escreve em checkpoint.json.tmp e usa os.replace() (atômico
#     no SO) para substituir o arquivo final apenas quando a escrita
#     for concluída com sucesso.
#
#   [FIX 6 - load_sku_list: ENCODING utf-8-sig PARA REMOVER BOM]
#     Arquivos gerados no Windows (Excel, Notepad) podem conter BOM
#     (Byte Order Mark: \ufeff) no início. Com encoding="utf-8" o BOM
#     ficava colado ao primeiro ID: "\ufeff123456" → int() falhava com
#     ValueError e o SKU era silenciosamente ignorado.
#     Solução: encoding="utf-8-sig" remove o BOM automaticamente.
#     O log agora também exibe o hex do valor inválido para diagnóstico.
#
#   [FIX 7 - safe_request: DETECÇÃO GLOBAL DE AUTH ERROR]
#     Na v10, 401/403 só era detectado dentro de update_image_alt (PUT).
#     Um 401 no GET de imagens retornava False genérico sem sinalizar abort.
#     Solução: safe_request seta um threading.Event global ao receber
#     401/403. O runner verifica o evento a cada iteração e aborta se setado.
#
#   [FIX 8 - safe_request: RECURSÃO EM 429 SUBSTITUÍDA POR LOOP]
#     A chamada recursiva `return safe_request(...)` em caso de 429 podia
#     causar RecursionError em rate limits prolongados (múltiplos 429
#     consecutivos). Substituída por loop iterativo com limite de 3 retries.
#
# Correções mantidas de versões anteriores:
#   v10: HTTP 405 → fallback POST, skip permanente, retry de arquivo bloqueado
#   v9:  URL normalizada para formato base (ArchiveId) no PUT
#   v8:  payload mínimo (MINIMAL_PUT_FIELDS), detecção de lixo ampliada
# =============================================================================


# ---------------------------------------------------------------------------- #
# CONFIGURAÇÕES CENTRALIZADAS
# ---------------------------------------------------------------------------- #

ACCOUNT_NAME      = "bemol"
BASE_URL          = f"https://{ACCOUNT_NAME}.vtexcommercestable.com.br/api/catalog/pvt"
VTEX_IMG_BASE_URL = f"https://{ACCOUNT_NAME}.vteximg.com.br/arquivos/ids"

VTEX_COOKIE = os.getenv("VTEX_COOKIE", "")

HEADERS = {
    "VtexIdclientAutCookie": VTEX_COOKIE,
    "Content-Type":          "application/json",
    "Accept":                "application/json",
}

LOG_FILE        = "execution_log.txt"
ERROR_LOG       = "error_log.txt"
CHECKPOINT_FILE = "checkpoint.json"
SKU_LIST_FILE   = "sku_ids.txt"

MAX_WORKERS         = 1
REQUEST_TIMEOUT     = 60
MAX_RETRIES         = 2
BACKOFF_FACTOR      = 2
RATE_LIMIT_DELAY    = 1.5
CHECKPOINT_INTERVAL = 10

PUT_MAX_ATTEMPTS = 3
PUT_RETRY_DELAY  = 5

# Retry para remoção de SKU do arquivo (Permission Denied em Windows)
FILE_REMOVE_MAX_ATTEMPTS = 5
FILE_REMOVE_RETRY_DELAY  = 2.0

# Arquivo de SKUs ignorados permanentemente (405 em todas as tentativas)
SKIPPED_405_FILE = "skipped_405.txt"

# Campos mínimos aceitos pelo PUT de imagem da VTEX
MINIMAL_PUT_FIELDS = {"Id", "SkuId", "ArchiveId", "IsMain", "Label", "Text", "Url", "Name"}

# Campos somente-leitura rejeitados pelo PUT
IMAGE_PUT_BLOCKED_FIELDS = {
    "ProductId",
    "FileLocation",
    "ImageCompany",
    "StoreUrl",
}

URL_BLOCKED_SCHEMES = ("s3://", "s3a://", "s3n://")

# [v11 FIX 1] Lista negra de placeholders — usada APENAS para correspondência
# exata de string isolada. Não detecta substrings dentro de frases legítimas.
DIRTY_VALUE_BLACKLIST = {
    "main", "imagem", "image", "foto", "photo", "picture",
    "img", "arquivo", "file", "thumbnail", "thumb",
    "product", "produto", "default", "sem título", "untitled",
}

# [v11 FIX 7] Event global para sinalizar auth error entre threads
_auth_error_event = threading.Event()

log_lock  = threading.Lock()
file_lock = threading.Lock()


# ---------------------------------------------------------------------------- #
# ESTRATÉGIAS DE PUT — v11 FIX 2
# ---------------------------------------------------------------------------- #

class PutStrategy(Enum):
    FULL_PRIMARY     = auto()   # t1: payload completo  + PUT /file/{id}
    MINIMAL_PRIMARY  = auto()   # t2: payload mínimo    + PUT /file/{id}
    MINIMAL_ALT_PUT  = auto()   # t3: payload mínimo    + PUT /file
    MINIMAL_ALT_POST = auto()   # t4: payload mínimo    + POST /file


@dataclass
class PutAttemptResult:
    strategy:     PutStrategy
    http_status:  int
    success:      bool
    error_detail: str = ""


# ---------------------------------------------------------------------------- #
# SESSÃO HTTP
# ---------------------------------------------------------------------------- #

def create_session() -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total            = MAX_RETRIES,
        backoff_factor   = BACKOFF_FACTOR,
        status_forcelist = [429, 500, 502, 503, 504],
        allowed_methods  = ["GET", "PUT", "POST"],
    )
    adapter = HTTPAdapter(
        max_retries      = retry_strategy,
        pool_connections = 10,
        pool_maxsize     = 20,
    )
    session.mount("https://", adapter)
    session.mount("http://",  adapter)
    return session


SESSION = create_session()


# ---------------------------------------------------------------------------- #
# LOG
# ---------------------------------------------------------------------------- #

def log_message(message: str, level: str = "INFO") -> None:
    timestamp     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_msg = f"[{timestamp}] [{level}] {message}"
    with log_lock:
        print(formatted_msg)
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(formatted_msg + "\n")
            if level in ("ERROR", "CRITICAL"):
                with open(ERROR_LOG, "a", encoding="utf-8") as f:
                    f.write(formatted_msg + "\n")
        except Exception as exc:
            print(f"Erro ao gravar log: {exc}")


# ---------------------------------------------------------------------------- #
# ALT TEXT
# ---------------------------------------------------------------------------- #

def normalize_product_name(name: str) -> str:
    if not name:
        return ""
    return " ".join(name.split()).lower().strip()


def generate_alt_text(product_name: str) -> str:
    normalized = normalize_product_name(product_name)
    return normalized if normalized else "produto"


# ---------------------------------------------------------------------------- #
# DETECÇÃO DE CONTEÚDO LIXO — v11 (critério de tamanho removido)
# ---------------------------------------------------------------------------- #

# Padrão: apenas dígitos, hífens, underscores e letras MAIÚSCULAS (sem espaço)
# Exemplos: "240270-0", "240270-0_A", "307783", "SKU001"
_DIRTY_CONTENT_PATTERN = re.compile(r"^[\d\-_A-Z]+$")


def _is_dirty_content(value: str) -> bool:
    """
    Detecta se um valor de campo é lixo/placeholder — não um alt text SEO válido.

    Critérios (qualquer um torna o valor "sujo"):
        1. Bate no padrão regex: só dígitos/hífens/underscores/MAIÚSCULAS
           Exemplos: "240270-0", "240270-0_A", "307783"

        2. Correspondência EXATA com a lista negra de placeholders (case-insensitive)
           Exemplos: "Main", "main", "Imagem", "image", "foto"
           NÃO afeta frases como "foto da câmera principal" (contém "foto" mas
           não é idêntica a ela).

    REMOVIDO (v11): critério de string curta (len < 5 sem espaço) — causava
    falsos positivos em alt texts legítimos como "led", "tv", "kit".

    Args:
        value: Conteúdo do campo Label ou Text.

    Returns:
        True se é lixo e deve ser substituído; False se parece alt text válido.
    """
    if not value:
        return False

    stripped = value.strip()

    # Critério 1: padrão numérico/código (ex: "240270-0", "240270-0_A")
    if _DIRTY_CONTENT_PATTERN.match(stripped):
        return True

    # Critério 2: correspondência exata com placeholder conhecido
    # (não usa substring para evitar false positive em frases legítimas)
    if stripped.lower() in DIRTY_VALUE_BLACKLIST:
        return True

    return False


def _is_real_image(img: Dict) -> bool:
    archive_id = img.get("ArchiveId")
    return archive_id is not None and archive_id != 0


def _build_update_reason(current_label: str, current_text: str, alt_text: str) -> str:
    reasons = []
    for field_name, field_value in [("Label", current_label), ("Text", current_text)]:
        if field_value == alt_text:
            continue
        if not field_value:
            reasons.append(f"{field_name}=[vazio]")
        elif _is_dirty_content(field_value):
            reasons.append(f"{field_name}=[SUJO:'{field_value}']")
        else:
            reasons.append(f"{field_name}=[desatualizado:'{field_value[:30]}']")
    return " | ".join(reasons) if reasons else "(motivo desconhecido)"


# ---------------------------------------------------------------------------- #
# RATE LIMITER
# ---------------------------------------------------------------------------- #

class RateLimiter:
    def __init__(self, delay: float = RATE_LIMIT_DELAY):
        self._delay        = delay
        self._last_request = 0.0
        self._lock         = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_request
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
            self._last_request = time.monotonic()


rate_limiter = RateLimiter()


# ---------------------------------------------------------------------------- #
# CHECKPOINT — v11 FIX 5 (gravação atômica)
# ---------------------------------------------------------------------------- #

class CheckpointManager:
    def __init__(self, filename: str = CHECKPOINT_FILE):
        self._filename = filename
        self._data     = self._load()

    def _load(self) -> Dict:
        if os.path.exists(self._filename):
            try:
                with open(self._filename, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                log_message("Checkpoint corrompido — iniciando do zero.", "WARNING")
        return {"processed_skus": []}

    def save(self) -> None:
        """
        [v11 FIX 5] Gravação atômica via arquivo temporário.
        Escreve em .tmp e usa os.replace() (operação atômica no SO) para
        substituir o arquivo final somente após conclusão bem-sucedida da escrita.
        Isso elimina o risco de JSON corrompido/truncado em caso de crash.
        """
        tmp_path = self._filename + ".tmp"
        with log_lock:
            try:
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(self._data, f, indent=2)
                os.replace(tmp_path, self._filename)
            except Exception as exc:
                log_message(f"Erro ao salvar checkpoint: {exc}", "ERROR")
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    def mark_processed(self, sku_id: int) -> None:
        if sku_id not in self._data["processed_skus"]:
            self._data["processed_skus"].append(sku_id)
            self.save()  # persiste imediatamente após cada marcação

    def is_processed(self, sku_id: int) -> bool:
        return sku_id in self._data["processed_skus"]

    def clear(self) -> None:
        self._data = {"processed_skus": []}
        self.save()


# ---------------------------------------------------------------------------- #
# SKU FILE MANAGER
# ---------------------------------------------------------------------------- #

class SKUFileManager:
    def __init__(self, filename: str = SKU_LIST_FILE):
        self._filename = filename

    def mark_for_removal(self, sku_id: int) -> None:
        """Remove o SKU do arquivo de pendentes com retry para Permission Denied."""
        if not os.path.exists(self._filename):
            return
        for attempt in range(1, FILE_REMOVE_MAX_ATTEMPTS + 1):
            try:
                with file_lock:
                    with open(self._filename, "r", encoding="utf-8-sig") as f:
                        lines = f.readlines()
                    new_lines = []
                    removed   = False
                    for line in lines:
                        stripped = line.strip()
                        if not stripped or stripped.startswith("#"):
                            new_lines.append(line)
                            continue
                        try:
                            if int(stripped) == sku_id:
                                removed = True
                                continue
                            new_lines.append(line)
                        except ValueError:
                            new_lines.append(line)
                    if removed:
                        with open(self._filename, "w", encoding="utf-8") as f:
                            f.writelines(new_lines)
                        log_message(f"✓ SKU {sku_id} removido do arquivo de pendentes")
                return  # sucesso
            except PermissionError as exc:
                if attempt < FILE_REMOVE_MAX_ATTEMPTS:
                    log_message(
                        f"[PERM ERROR] SKU {sku_id} — arquivo bloqueado "
                        f"(tentativa {attempt}/{FILE_REMOVE_MAX_ATTEMPTS}). "
                        f"Aguardando {FILE_REMOVE_RETRY_DELAY}s...",
                        "WARNING",
                    )
                    time.sleep(FILE_REMOVE_RETRY_DELAY)
                else:
                    log_message(
                        f"[PERM ERROR] Não foi possível remover SKU {sku_id} "
                        f"após {FILE_REMOVE_MAX_ATTEMPTS} tentativas: {exc}",
                        "ERROR",
                    )
            except Exception as exc:
                log_message(f"Erro ao remover SKU {sku_id} do arquivo: {exc}", "ERROR")
                return

    def get_remaining_count(self) -> int:
        if not os.path.exists(self._filename):
            return 0
        try:
            with open(self._filename, "r", encoding="utf-8-sig") as f:
                return sum(
                    1 for line in f
                    if line.strip() and not line.strip().startswith("#")
                    and line.strip().lstrip("-").isdigit()
                )
        except Exception:
            return 0


sku_file_manager = SKUFileManager()


# ---------------------------------------------------------------------------- #
# LOAD SKU LIST — v11 FIX 6 (encoding utf-8-sig para remover BOM)
# ---------------------------------------------------------------------------- #

def load_sku_list(filename: str = SKU_LIST_FILE) -> List[int]:
    """
    [v11 FIX 6] Usa encoding='utf-8-sig' para remover automaticamente o BOM
    (Byte Order Mark: \\ufeff) gerado por editores Windows (Notepad, Excel).
    Sem isso, o primeiro ID ficava como "\\ufeff123456" e int() falhava
    silenciosamente, descartando o SKU sem aviso claro.
    O log agora exibe o hex do valor inválido para facilitar diagnóstico.
    """
    if not os.path.exists(filename):
        log_message(f"Arquivo não encontrado: {filename}", "CRITICAL")
        return []
    sku_ids = []
    try:
        with open(filename, "r", encoding="utf-8-sig") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    sku_ids.append(int(line))
                except ValueError:
                    hex_repr = line.encode("utf-8").hex()[:20]
                    log_message(
                        f"ID inválido na linha {line_num}: '{line}' (hex: {hex_repr})",
                        "WARNING",
                    )
        log_message(f"Carregados {len(sku_ids)} SKU IDs de '{filename}'")
        return sku_ids
    except Exception as exc:
        log_message(f"Erro ao ler lista de SKUs: {exc}", "CRITICAL")
        return []


# ---------------------------------------------------------------------------- #
# HTTP — v11 FIX 7 (detecção global de auth) + FIX 8 (loop em vez de recursão)
# ---------------------------------------------------------------------------- #

def safe_request(method: str, url: str, **kwargs) -> Optional[requests.Response]:
    """
    [v11 FIX 7] Seta _auth_error_event global ao receber 401/403 em qualquer
    endpoint (GET ou PUT), permitindo que o runner aborte a execução sem
    depender do retorno de funções intermediárias.

    [v11 FIX 8] Tratamento de 429 substituído por loop iterativo (máximo 3
    vezes) para evitar RecursionError em rate limits prolongados.
    """
    rate_limiter.wait()
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    kwargs.setdefault("headers", HEADERS)

    max_429_retries = 3
    for attempt_429 in range(max_429_retries):
        try:
            response = SESSION.request(method, url, **kwargs)
        except requests.exceptions.Timeout:
            log_message(f"[TIMEOUT] {method} {url}", "ERROR")
            return None
        except requests.exceptions.ConnectionError as exc:
            log_message(f"[CONN ERROR] {method} {url} — {exc}", "ERROR")
            return None
        except Exception as exc:
            log_message(f"[UNEXPECTED] {method} {url} — {exc}", "ERROR")
            return None

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            log_message(
                f"[RATE LIMIT] Aguardando {retry_after}s... "
                f"(tentativa {attempt_429 + 1}/{max_429_retries})",
                "WARNING",
            )
            time.sleep(retry_after)
            rate_limiter.wait()
            continue  # tenta novamente no loop

        # [v11 FIX 7] Qualquer 401/403 em qualquer endpoint seta o evento global
        if response.status_code in (401, 403):
            log_message(
                f"[AUTH ERROR] HTTP {response.status_code} em {method} {url}. "
                "Cookie expirado ou inválido — sinalizando abort.",
                "CRITICAL",
            )
            _auth_error_event.set()

        return response

    # Todos os retries de 429 esgotados
    log_message(
        f"[RATE LIMIT ESGOTADO] {max_429_retries} tentativas após 429 — desistindo.",
        "ERROR",
    )
    return None


def get_sku_details(sku_id: int) -> Tuple[Optional[str], Optional[str], int]:
    url      = f"{BASE_URL}/stockkeepingunit/{sku_id}"
    response = safe_request("GET", url)
    if response is None:
        return None, None, 0
    if response.status_code == 200:
        data   = response.json()
        name   = data.get("ProductName") or data.get("NameComplete") or data.get("Name")
        ref_id = data.get("RefId")
        return name, ref_id, 200
    return None, None, response.status_code


# ---------------------------------------------------------------------------- #
# SANITIZAÇÃO DE PAYLOAD
# ---------------------------------------------------------------------------- #

def _build_vtex_url(archive_id) -> Optional[str]:
    """Monta a URL pública base da VTEX a partir do ArchiveId."""
    if archive_id:
        return f"{VTEX_IMG_BASE_URL}/{archive_id}"
    return None


_VTEX_URL_ARCHIVE_ID_RE = re.compile(r"/ids/(\d+)")


def _clean_vtex_url(url: str) -> str:
    """
    Normaliza uma URL de imagem VTEX para o formato base aceito no PUT.
    O GET retorna: .../ids/194802/arquivo.jpg?v=638537...
    O PUT aceita:  .../ids/194802
    """
    match = _VTEX_URL_ARCHIVE_ID_RE.search(url)
    if match:
        archive_id = match.group(1)
        return f"{VTEX_IMG_BASE_URL}/{archive_id}"
    return url.split("?")[0]


def _sanitize_url_field(payload: Dict, original_payload: Optional[Dict] = None) -> Dict:
    """
    Garante que o campo "Url" no payload seja válido para o PUT da VTEX.
    Sempre reconstrói a URL a partir do ArchiveId para evitar divergências
    entre o ID da URL raw e o ArchiveId correto do payload.
    """
    result     = payload.copy()
    url_value  = result.get("Url")
    url_is_s3  = isinstance(url_value, str) and url_value.lower().startswith(URL_BLOCKED_SCHEMES)

    archive_id = result.get("ArchiveId")
    if not archive_id and original_payload:
        archive_id = original_payload.get("ArchiveId")

    built_url = _build_vtex_url(archive_id)
    if built_url:
        result["Url"] = built_url
        if "ArchiveId" not in result and archive_id:
            result["ArchiveId"] = archive_id
    elif url_value and not url_is_s3:
        result["Url"] = _clean_vtex_url(url_value)
    else:
        result.pop("Url", None)

    return result


def _build_full_payload(image_data: Dict, alt_text: str) -> Dict:
    """Payload completo sanitizado (todas as chaves retornadas pelo GET)."""
    payload          = _sanitize_url_field(image_data.copy(), original_payload=image_data)
    payload["Label"] = alt_text
    payload["Text"]  = alt_text
    return payload


def _build_minimal_payload(image_data: Dict, alt_text: str) -> Dict:
    """
    Payload mínimo garantido para o PUT da VTEX.
    Usa apenas MINIMAL_PUT_FIELDS para eliminar campos problemáticos.
    """
    minimal          = {k: v for k, v in image_data.items() if k in MINIMAL_PUT_FIELDS}
    minimal["Label"] = alt_text
    minimal["Text"]  = alt_text
    minimal          = _sanitize_url_field(minimal, original_payload=image_data)
    return minimal


# ---------------------------------------------------------------------------- #
# PUT DE IMAGEM COM RETRY MANUAL
# ---------------------------------------------------------------------------- #

def _put_image(url: str, payload: Dict) -> Optional[requests.Response]:
    for attempt in range(1, PUT_MAX_ATTEMPTS + 1):
        response = safe_request("PUT", url, json=payload)
        if response is not None:
            return response
        is_last_attempt = (attempt == PUT_MAX_ATTEMPTS)
        if is_last_attempt:
            log_message(
                f"      [PUT TIMEOUT] {PUT_MAX_ATTEMPTS} tentativas sem resposta — desistindo.",
                "ERROR",
            )
        else:
            log_message(
                f"      [PUT TIMEOUT] Tentativa {attempt}/{PUT_MAX_ATTEMPTS} sem resposta. "
                f"Aguardando {PUT_RETRY_DELAY}s...",
                "WARNING",
            )
            time.sleep(PUT_RETRY_DELAY)
    return None


# ---------------------------------------------------------------------------- #
# REGISTRO DE SKUs IRRECUPERÁVEIS
# ---------------------------------------------------------------------------- #

def _register_skipped_405(sku_id: int) -> None:
    """Registra SKU irrecuperável por 405 em arquivo separado para revisão manual."""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(SKIPPED_405_FILE, "a", encoding="utf-8") as f:
            f.write(f"{sku_id}  # {timestamp} — HTTP 405 em todas tentativas (PUT+POST)\n")
    except Exception as exc:
        log_message(f"Erro ao registrar SKU {sku_id} em {SKIPPED_405_FILE}: {exc}", "WARNING")


# ---------------------------------------------------------------------------- #
# ATUALIZAÇÃO DE ALT TEXT — v11 FIX 2 + FIX 3 (máquina de estados explícita)
# ---------------------------------------------------------------------------- #

def update_image_alt(sku_id: int, image_data: Dict, new_alt_text: str):
    """
    Atualiza Label e Text de uma imagem na VTEX.

    [v11 FIX 2] Estratégia baseada em loop sobre lista de tentativas com
    conjunto RETRY_ON explícito. Erros fora de {400, 404, 405} são definitivos
    e interrompem as tentativas imediatamente. Elimina o problema de
    if/elif aninhados com sobrescrita de variável entre tentativas.

    [v11 FIX 3] Retorna "AUTH_ERROR" em 401/403 para propagar o sinal de
    abort até o runner principal, em vez de retornar False genérico.

    Estratégia de tentativas:
        t1: PUT /file/{id} + payload completo sanitizado
        t2: PUT /file/{id} + payload mínimo            (se t1 → 400)
        t3: PUT /file       + payload mínimo            (se t1/t2 → 404/405)
        t4: POST /file      + payload mínimo            (se t3 → 405)

    Returns:
        True        → atualizado com sucesso
        False       → falhou (será retentado na próxima execução)
        "AUTH_ERROR"→ cookie expirado — runner deve abortar
        "SKIP_405"  → 405 em todas as tentativas — SKU irrecuperável via API
    """
    file_id      = image_data.get("Id")
    archive_id   = image_data.get("ArchiveId")
    url_primary  = f"{BASE_URL}/stockkeepingunit/{sku_id}/file/{file_id}"
    url_alt      = f"{BASE_URL}/stockkeepingunit/{sku_id}/file"

    log_message(
        f"      [DIAG] img_id={file_id} | ArchiveId={archive_id} | "
        f"Url_raw={str(image_data.get('Url', 'AUSENTE'))[:80]}",
    )

    # Códigos HTTP que autorizam avançar para a próxima estratégia
    RETRY_ON = {400, 404, 405}

    # Lista de estratégias em ordem de prioridade:
    # (enum, método_http, url, função_que_constrói_payload)
    strategies = [
        (PutStrategy.FULL_PRIMARY,     "PUT",  url_primary, lambda: _build_full_payload(image_data, new_alt_text)),
        (PutStrategy.MINIMAL_PRIMARY,  "PUT",  url_primary, lambda: _build_minimal_payload(image_data, new_alt_text)),
        (PutStrategy.MINIMAL_ALT_PUT,  "PUT",  url_alt,     lambda: _build_minimal_payload(image_data, new_alt_text)),
        (PutStrategy.MINIMAL_ALT_POST, "POST", url_alt,     lambda: _build_minimal_payload(image_data, new_alt_text)),
    ]

    last_result: Optional[PutAttemptResult] = None

    for strategy, method, url, build_payload in strategies:
        payload = build_payload()
        log_message(
            f"      [{strategy.name}] {method} | Url: {payload.get('Url', 'AUSENTE')[:70]}"
        )

        if method == "PUT":
            response = _put_image(url, payload)
        else:
            response = safe_request(method, url, json=payload)

        if response is None:
            log_message(f"      [{strategy.name}] timeout — sem resposta.", "WARNING")
            last_result = PutAttemptResult(strategy, 0, False, "timeout")
            # Timeout não é definitivo — tenta a próxima estratégia
            continue

        log_message(f"      [{strategy.name}] HTTP {response.status_code}")

        # Sucesso
        if response.status_code == 200:
            log_message(f"      [OK] alt text atualizado via {strategy.name}: '{new_alt_text}'")
            return True

        # [v11 FIX 3] Auth error — propaga sinal de abort
        if response.status_code in (401, 403):
            return "AUTH_ERROR"

        error_body = response.text[:300].strip() or "(sem corpo)"
        last_result = PutAttemptResult(strategy, response.status_code, False, error_body)

        if response.status_code not in RETRY_ON:
            # Erro definitivo (ex: 422, 500 persistente) — não adianta continuar
            log_message(
                f"      [ABORT] HTTP {response.status_code} não recuperável — "
                f"interrompendo tentativas. Detalhe: {error_body}",
                "ERROR",
            )
            return False

        log_message(
            f"      [{strategy.name} → {response.status_code}] avançando para próxima estratégia...",
            "WARNING",
        )

    # Todas as estratégias esgotadas
    final_status = last_result.http_status if last_result else 0
    log_message(
        f"      [FAIL] SKU {sku_id} — todas as estratégias esgotadas. "
        f"Último HTTP: {final_status}",
        "ERROR",
    )

    # Se o último erro foi 405, o SKU é irrecuperável via API
    if final_status == 405:
        log_message(
            f"      [SKIP 405] SKU {sku_id} retornou 405 em todas as tentativas "
            "(PUT e POST). Marcando para skip permanente.",
            "WARNING",
        )
        return "SKIP_405"

    return False


# ---------------------------------------------------------------------------- #
# PROCESSAMENTO DE IMAGENS DE UM SKU — v11 FIX 3 + FIX 4
# ---------------------------------------------------------------------------- #

def process_sku_images(sku_id: int, product_name: str, checkpoint: CheckpointManager):
    """
    [v11 FIX 3] Propaga "AUTH_ERROR" para process_single_sku quando
    update_image_alt retornar esse valor, em vez de tratar como False genérico.

    [v11 FIX 4] SKIP_405 para no loop (break) corretamente — é uma condição
    de nível de SKU, não de imagem individual. As imagens processadas antes
    do SKIP_405 já foram atualizadas com sucesso (ou falharam por outros motivos)
    e são contabilizadas normalmente.

    Returns:
        True        → processamento concluído (com ou sem skip por 405)
        False       → pelo menos uma imagem falhou por erro não-405
        "AUTH_ERROR"→ cookie expirado — runner deve abortar
    """
    url      = f"{BASE_URL}/stockkeepingunit/{sku_id}/file"
    response = safe_request("GET", url)

    if not response:
        # Se o auth error event foi setado pelo safe_request, propaga
        if _auth_error_event.is_set():
            return "AUTH_ERROR"
        return False

    if response.status_code == 404:
        checkpoint.mark_processed(sku_id)
        return True

    if response.status_code != 200:
        log_message(f"[GET ERROR] SKU {sku_id} — HTTP {response.status_code}", "ERROR")
        return False

    images   = response.json()
    alt_text = generate_alt_text(product_name)

    if not images:
        checkpoint.mark_processed(sku_id)
        return True

    real_images = [img for img in images if _is_real_image(img)]
    empty_slots = len(images) - len(real_images)

    if empty_slots > 0:
        log_message(f"      [SLOT] {empty_slots} slot(s) vazio(s) ignorado(s)")

    if not real_images:
        checkpoint.mark_processed(sku_id)
        return True

    success  = True
    skip_405 = False

    for img in real_images:
        current_label = (img.get("Label") or "").strip()
        current_text  = (img.get("Text")  or "").strip()

        label_is_correct = (current_label == alt_text) and not _is_dirty_content(current_label)
        text_is_correct  = (current_text  == alt_text) and not _is_dirty_content(current_text)

        if label_is_correct and text_is_correct:
            log_message(f"      [SKIP] img_id={img.get('Id')} já está correto: '{alt_text}'")
            continue

        reason = _build_update_reason(current_label, current_text, alt_text)
        log_message(f"      [UPDATE] img_id={img.get('Id')} | {reason} → '{alt_text}'")

        result = update_image_alt(sku_id, img, alt_text)

        # [v11 FIX 3] AUTH_ERROR: propaga imediatamente para o runner
        if result == "AUTH_ERROR":
            return "AUTH_ERROR"

        # [v11 FIX 4] SKIP_405: é condição de nível de SKU — para o loop
        if result == "SKIP_405":
            skip_405 = True
            success  = False
            break

        if not result:
            success = False

    # SKU com 405 irrecuperável: registrar e marcar no checkpoint
    if skip_405:
        _register_skipped_405(sku_id)
        checkpoint.mark_processed(sku_id)
        log_message(
            f"      [SKIP 405 PERMANENTE] SKU {sku_id} registrado em '{SKIPPED_405_FILE}'.",
            "WARNING",
        )
        return True  # checkpoint avançado — não reprocessar

    if success:
        checkpoint.mark_processed(sku_id)

    return success


# ---------------------------------------------------------------------------- #
# PROCESSAMENTO DE UM ÚNICO SKU — v11 FIX 3 (AUTH_ERROR de process_sku_images)
# ---------------------------------------------------------------------------- #

def process_single_sku(sku_id: int, checkpoint: CheckpointManager) -> Tuple[bool, bool]:
    """
    Returns:
        (sucesso: bool, deve_abortar: bool)
        deve_abortar=True  → runner deve interromper a execução (auth error)
        deve_abortar=False → continua normalmente
    """
    if checkpoint.is_processed(sku_id):
        log_message(f"SKU {sku_id} já processado (checkpoint) — pulando.")
        sku_file_manager.mark_for_removal(sku_id)
        return True, False

    product_name, ref_id, http_status = get_sku_details(sku_id)

    # Verifica auth error tanto pelo status retornado quanto pelo event global
    if http_status in (401, 403) or _auth_error_event.is_set():
        log_message(
            f"[AUTH ERROR] HTTP {http_status} ao buscar SKU {sku_id}. "
            "Cookie expirado — interrompendo execução.",
            "CRITICAL",
        )
        return False, True

    if not product_name:
        log_message(
            f"SKU {sku_id} ignorado — sem detalhes na API VTEX "
            f"(HTTP {http_status or 'sem resposta'}).",
            "WARNING",
        )
        checkpoint.mark_processed(sku_id)
        sku_file_manager.mark_for_removal(sku_id)
        return True, False

    log_message(f"SKU ID: {sku_id} | RefId: {ref_id} | Produto: {product_name}")

    result = process_sku_images(sku_id, product_name, checkpoint)

    # [v11 FIX 3] AUTH_ERROR propagado de process_sku_images
    if result == "AUTH_ERROR" or _auth_error_event.is_set():
        log_message(
            f"[AUTH ERROR] Cookie expirou durante processamento do SKU {sku_id}. "
            "Interrompendo execução.",
            "CRITICAL",
        )
        return False, True

    success = bool(result)

    if success:
        remaining = sku_file_manager.get_remaining_count()
        sku_file_manager.mark_for_removal(sku_id)
        log_message(f"✓ SKU {sku_id} concluído | Restantes no arquivo: {remaining - 1}")
    else:
        log_message(f"✗ SKU {sku_id} falhou — será tentado novamente na próxima execução.", "WARNING")

    return success, False


# ---------------------------------------------------------------------------- #
# VALIDAÇÃO DE COOKIE ANTES DE INICIAR
# ---------------------------------------------------------------------------- #

def _validate_cookie_active() -> bool:
    """
    [v11] Verifica se o cookie ainda está válido antes de processar qualquer SKU.
    Executa um GET em /stockkeepingunit/1 (endpoint leve) e verifica o status.
    Retorna False se o cookie estiver expirado, True se estiver ativo.
    Não aborta em 404 (SKU inexistente) — apenas em 401/403.
    """
    log_message("Validando autenticação VTEX...", "INFO")
    url      = f"{BASE_URL}/stockkeepingunit/1"
    response = safe_request("GET", url)

    if response is None:
        log_message(
            "Não foi possível conectar à API VTEX para validar o cookie. "
            "Verifique a conectividade.",
            "ERROR",
        )
        return False

    if response.status_code in (401, 403):
        log_message(
            f"Cookie INVÁLIDO ou EXPIRADO (HTTP {response.status_code}). "
            "Atualize VTEX_COOKIE antes de continuar.",
            "CRITICAL",
        )
        return False

    # 404 = SKU não existe, mas a autenticação funcionou
    log_message(
        f"Cookie validado com sucesso (HTTP {response.status_code}).",
        "INFO",
    )
    # Limpa o event caso tenha sido setado na validação por algum outro motivo
    _auth_error_event.clear()
    return True


# ---------------------------------------------------------------------------- #
# RUNNER PRINCIPAL
# ---------------------------------------------------------------------------- #

def run_bulk_update(resume: bool = True) -> None:
    checkpoint = CheckpointManager()

    if not resume:
        checkpoint.clear()
        log_message("Checkpoint limpo — iniciando do zero.")

    # [v11] Valida o cookie antes de processar qualquer SKU
    if not _validate_cookie_active():
        log_message(
            "Execução abortada — cookie inválido. "
            "Configure VTEX_COOKIE e tente novamente.",
            "CRITICAL",
        )
        return

    sku_ids = load_sku_list(SKU_LIST_FILE)

    if not sku_ids:
        log_message("Nenhum SKU para processar. Verifique o arquivo sku_ids.txt.", "CRITICAL")
        return

    log_message("=" * 60)
    log_message("INICIANDO VTEX ALT TEXT UPDATER v11")
    log_message(f"Total de SKUs: {len(sku_ids)} | Workers: {MAX_WORKERS}")
    log_message("=" * 60)

    processed_count = 0
    auth_error      = False

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_single_sku, sku_id, checkpoint): sku_id
                for sku_id in sku_ids
            }

            for future in as_completed(futures):
                processed_count += 1
                _, deve_abortar = future.result()

                # [v11 FIX 7] Verifica também o event global (pode ser setado
                # por safe_request em qualquer thread antes do future completar)
                if deve_abortar or _auth_error_event.is_set():
                    auth_error = True
                    log_message(
                        "Execução interrompida por erro de autenticação. "
                        "Atualize VTEX_COOKIE e execute novamente.",
                        "CRITICAL",
                    )
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                if processed_count % CHECKPOINT_INTERVAL == 0:
                    # checkpoint.save() já é chamado em mark_processed —
                    # este save extra garante consistência no progresso
                    checkpoint.save()
                    remaining = sku_file_manager.get_remaining_count()
                    log_message(
                        f"📊 Progresso: {processed_count}/{len(sku_ids)} | "
                        f"Restantes: {remaining}"
                    )

        checkpoint.save()

    except KeyboardInterrupt:
        log_message("Interrompido pelo usuário — salvando checkpoint...", "WARNING")
        checkpoint.save()
    except Exception as exc:
        log_message(f"Erro fatal: {exc}", "CRITICAL")
        checkpoint.save()

    remaining = sku_file_manager.get_remaining_count()
    log_message("=" * 60)

    if auth_error:
        log_message(
            f"INTERROMPIDO POR AUTH ERROR | {processed_count}/{len(sku_ids)} SKUs processados"
        )
        log_message("→ Atualize VTEX_COOKIE e execute novamente para continuar.")
    else:
        log_message(f"CONCLUÍDO: {processed_count}/{len(sku_ids)} SKUs processados")

    log_message(f"Restantes no arquivo: {remaining}")
    log_message("=" * 60)


# ---------------------------------------------------------------------------- #
# PONTO DE ENTRADA
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
    if not VTEX_COOKIE:
        print("⚠️  ATENÇÃO: Configure o cookie VTEX na variável de ambiente VTEX_COOKIE.")
        print("    Opção 1 (recomendado): crie um arquivo .env com VTEX_COOKIE='seu_cookie'")
        print("    Opção 2: export VTEX_COOKIE='seu_cookie_aqui'  (Linux/macOS)")
        print("    Opção 3: set VTEX_COOKIE=seu_cookie_aqui       (Windows CMD)")
    else:
        print("=" * 60)
        print("  VTEX IMAGE ALT TEXT UPDATER — SEO NATURAL v11")
        print("=" * 60)
        print(f"  Arquivo de SKUs : {SKU_LIST_FILE}")
        print(f"  Max workers     : {MAX_WORKERS}")
        print(f"  Rate limit delay: {RATE_LIMIT_DELAY}s")
        print(f"  Request timeout : {REQUEST_TIMEOUT}s")
        print("=" * 60)

        if not os.path.exists(SKU_LIST_FILE):
            print(f"\n⚠️  Arquivo '{SKU_LIST_FILE}' não encontrado!")
            print("Crie o arquivo com um SKU ID por linha.")
        else:
            resume  = input("\nRetomar do checkpoint? (S/n): ").strip().lower() != "n"
            confirm = input("Digite 'SIM' para iniciar: ").strip()

            if confirm == "SIM":
                run_bulk_update(resume=resume)
            else:
                print("Execução cancelada.")