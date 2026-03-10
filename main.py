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

# Opcional: Carrega variáveis de ambiente a partir de um arquivo .env (útil em ambientes locais)
# Requer a dependência `python-dotenv` (pip install python-dotenv).
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except ImportError:
    pass


# =============================================================================
# VTEX IMAGE ALT TEXT UPDATER — LOCAL (v7)
# =============================================================================
# Responsabilidade:
#   Atualizar o alt text (Label e Text) das imagens de SKUs na VTEX a partir
#   de uma lista de IDs em arquivo texto, com checkpoint de progresso e
#   remoção automática do SKU do arquivo após processamento bem-sucedido.
#
# Correções aplicadas (v7):
#   [FIX - "Field Url is required": INJEÇÃO DE URL QUANDO AUSENTE NO PAYLOAD]
#     O GET de imagens da VTEX retorna alguns payloads sem o campo "Url"
#     (campo ausente, não nulo). O PUT exige "Url" obrigatoriamente.
#     A v6 removia Url nula/S3, mas não tratava o caso de Url completamente
#     ausente — o PUT continuava falhando com "Field Url is required".
#
#     Solução: _sanitize_image_payload() agora injeta a Url quando ausente,
#     construindo-a a partir do ArchiveId que sempre está presente:
#         https://{ACCOUNT_NAME}.vteximg.com.br/arquivos/ids/{ArchiveId}
#     Esse é o padrão público de URL de imagem da VTEX e é aceito no PUT.
#
# Correções aplicadas (v6):
#   [FIX - PAYLOAD DO PUT: CAMPOS PROBLEMÁTICOS IDENTIFICADOS]
#     Dois erros distintos foram observados ao enviar o payload completo no PUT:
#
#       (a) "Field Url is required" — o campo "Url" existe no payload do GET
#           com valor None em alguns SKUs. A VTEX rejeita o PUT quando "Url"
#           está ausente ou nulo. Solução: garantir que "Url" nunca seja None
#           — se estiver nulo, o campo é removido do payload para que a VTEX
#           use o valor existente no servidor.
#
#       (b) "The 's3' scheme is not supported." — o campo "Url" em alguns SKUs
#           contém uma URL interna S3 (s3://bucket/...) que a VTEX não aceita
#           no PUT. Solução: detectar e remover campos com URL de esquema
#           não-HTTP antes de enviar o payload.
#
#     Adicionada função _sanitize_image_payload() que aplica essas correções
#     antes de qualquer tentativa de PUT, eliminando a necessidade do fallback
#     400 na maioria dos casos.
#
# Correções aplicadas (v5):
#   [FIX - "Field Url is required" NO PUT DE IMAGEM]
#     A API VTEX exige o campo "Url" no payload do PUT de imagem. Esse campo
#     estava ausente no fallback porque IMAGE_PUT_ALLOWED_FIELDS não o incluía.
#     Além disso, o payload completo (tentativa 1) também estava sendo rejeitado,
#     indicando que a lista branca de campos estava incorreta como estratégia.
#
#     Nova estratégia para o fallback (tentativa 2):
#       - Em vez de lista branca (só campos conhecidos), usa lista negra:
#         remove apenas os campos que comprovadamente causam erros (campos de
#         metadados internos da VTEX que não devem ser reenviados no PUT).
#       - IMAGE_PUT_BLOCKED_FIELDS substitui IMAGE_PUT_ALLOWED_FIELDS
#       - Garante que "Url" e qualquer outro campo obrigatório nunca seja
#         descartado acidentalmente por omissão na lista.
#
# Correções aplicadas (v4):
#   [FIX - SUPORTE A RefId (SKU BEMOL) COMO ENTRADA]
#     O arquivo sku_ids.txt pode conter tanto VTEX IDs (numéricos) quanto
#     RefIds do sistema Bemol (ex: "12345-A", "SKU-99"). Quando o ID fornecido
#     retorna 404 na API /stockkeepingunit/{id}, o script tenta automaticamente
#     resolver via /stockkeepingunit/stockkeepingunitidbyrefid/{refId} para
#     obter o VTEX ID real. Todo o processamento subsequente usa o VTEX ID.
#     O log indica claramente quando a resolução por RefId foi necessária.
#
# Correções aplicadas (v7):
#   [FIX - "Field Url is required": INJEÇÃO DE URL QUANDO AUSENTE NO PAYLOAD]
#     O GET de imagens da VTEX retorna alguns payloads sem o campo "Url"
#     (campo ausente, não nulo). O PUT exige "Url" obrigatoriamente.
#     A v6 removia Url nula/S3, mas não tratava o caso de Url completamente
#     ausente — o PUT continuava falhando com "Field Url is required".
#
#     Solução: _sanitize_image_payload() agora injeta a Url quando ausente,
#     construindo-a a partir do ArchiveId que sempre está presente:
#         https://{ACCOUNT_NAME}.vteximg.com.br/arquivos/ids/{ArchiveId}
#     Esse é o padrão público de URL de imagem da VTEX e é aceito no PUT.
#
# Correções aplicadas (v6):
#   [FIX - PAYLOAD DO PUT: CAMPOS PROBLEMÁTICOS IDENTIFICADOS]
#     Dois erros distintos foram observados ao enviar o payload completo no PUT:
#
#       (a) "Field Url is required" — o campo "Url" existe no payload do GET
#           com valor None em alguns SKUs. A VTEX rejeita o PUT quando "Url"
#           está ausente ou nulo. Solução: garantir que "Url" nunca seja None
#           — se estiver nulo, o campo é removido do payload para que a VTEX
#           use o valor existente no servidor.
#
#       (b) "The 's3' scheme is not supported." — o campo "Url" em alguns SKUs
#           contém uma URL interna S3 (s3://bucket/...) que a VTEX não aceita
#           no PUT. Solução: detectar e remover campos com URL de esquema
#           não-HTTP antes de enviar o payload.
#
#     Adicionada função _sanitize_image_payload() que aplica essas correções
#     antes de qualquer tentativa de PUT, eliminando a necessidade do fallback
#     400 na maioria dos casos.
#
# Correções aplicadas (v5):
#   [FIX - "Field Url is required" NO PUT DE IMAGEM]
#     A API VTEX exige o campo "Url" no payload do PUT de imagem. Esse campo
#     estava ausente no fallback porque IMAGE_PUT_ALLOWED_FIELDS não o incluía.
#     Além disso, o payload completo (tentativa 1) também estava sendo rejeitado,
#     indicando que a lista branca de campos estava incorreta como estratégia.
#
#     Nova estratégia para o fallback (tentativa 2):
#       - Em vez de lista branca (só campos conhecidos), usa lista negra:
#         remove apenas os campos que comprovadamente causam erros (campos de
#         metadados internos da VTEX que não devem ser reenviados no PUT).
#       - IMAGE_PUT_BLOCKED_FIELDS substitui IMAGE_PUT_ALLOWED_FIELDS
#       - Garante que "Url" e qualquer outro campo obrigatório nunca seja
#         descartado acidentalmente por omissão na lista.
#
# Correções aplicadas (v4):
#   [FIX - RETRY MANUAL NO PUT DE IMAGEM]
#     O endpoint PUT de imagem da VTEX pode não responder em horário de pico,
#     retornando "sem resposta" (timeout silencioso). O retry automático da
#     urllib3 não cobre esse caso — só cobre erros HTTP explícitos (429/5xx).
#     Adicionado retry manual com backoff em update_image_alt():
#       - PUT_MAX_ATTEMPTS = 3  tentativas por imagem
#       - PUT_RETRY_DELAY  = 5s de espera entre tentativas
#     Também aumentados REQUEST_TIMEOUT (30→60s) e RATE_LIMIT_DELAY (1.0→1.5s)
#     para dar mais margem ao endpoint PUT, que é mais pesado que o GET.
#
# Correções aplicadas (v3):
#   [FIX - DETECÇÃO DE COOKIE EXPIRADO EM get_sku_details()]
#     Antes, a função retornava None tanto para SKU inexistente (404) quanto
#     para autenticação inválida (401/403). Isso mascarava cookies expirados:
#     o script interpretava todos os SKUs como "sem detalhes" e os ignorava
#     silenciosamente, removendo-os do arquivo sem processar nenhuma imagem.
#     Agora a função retorna um código de status separado, e o script aborta
#     imediatamente com mensagem clara quando detecta 401/403.
#
# Correções aplicadas (v2):
#   [FIX - DETECÇÃO DE CONTEÚDO NUMÉRICO/LIXO]
#     A VTEX preenche automaticamente os campos Label e Text com valores como
#     "240270-0" ou "240270-0_A" (ArchiveId/Name do arquivo). Esses valores
#     são lixo — não são alt texts válidos para SEO. O script agora:
#       1. Detecta esse padrão via regex no helper _is_dirty_content()
#       2. Trata conteúdo "sujo" como se estivesse vazio — força atualização
#       3. Exibe no log quando a atualização foi motivada por campo sujo
#   [FIX - VERIFICAÇÃO DUPLA DE CAMPOS]
#     Antes, o script verificava apenas o campo Label para decidir se pulava
#     a imagem. Agora verifica Label E Text — a imagem só é pulada quando
#     AMBOS os campos já contêm o alt text correto.
#   [FIX - SLOTS DE IMAGEM VAZIOS IGNORADOS]
#     A VTEX retorna slots sem foto (ArchiveId null/0) que causavam erro ao
#     tentar atualizar alt text em posição sem arquivo real. O script agora
#     filtra esses slots antes de processar.
#
# Pré-requisitos:
#   - Cookie de sessão VTEX na variável de ambiente VTEX_COOKIE
#   - Arquivo sku_ids.txt com um SKU ID por linha na raiz do projeto
# =============================================================================


# ---------------------------------------------------------------------------- #
# CONFIGURAÇÕES CENTRALIZADAS
# ---------------------------------------------------------------------------- #

ACCOUNT_NAME = "bemol"
BASE_URL          = f"https://{ACCOUNT_NAME}.vtexcommercestable.com.br/api/catalog/pvt"
VTEX_IMG_BASE_URL = f"https://{ACCOUNT_NAME}.vteximg.com.br/arquivos/ids"

# Cookie lido da variável de ambiente — nunca hardcode credenciais no código-fonte
VTEX_COOKIE = os.getenv("VTEX_COOKIE", "cookie_nao_definido")

HEADERS = {
    "VtexIdclientAutCookie": VTEX_COOKIE,
    "Content-Type":          "application/json",
    "Accept":                "application/json",
}

# Arquivos de saída
LOG_FILE        = "execution_log.txt"
ERROR_LOG       = "error_log.txt"
CHECKPOINT_FILE = "checkpoint.json"
SKU_LIST_FILE   = "sku_ids.txt"

# Performance — PUT de imagem é custoso, manter 1 worker evita sobrecarga
MAX_WORKERS          = 1
REQUEST_TIMEOUT      = 60   # Aumentado: PUT de imagem VTEX pode demorar em horário de pico
MAX_RETRIES          = 2    # Retries automáticos em erros 429/5xx (urllib3)
BACKOFF_FACTOR       = 2    # Espera exponencial entre retries urllib3: 2s, 4s
RATE_LIMIT_DELAY     = 1.5  # Aumentado: PUT é mais pesado que GET na VTEX
CHECKPOINT_INTERVAL  = 10   # Salva checkpoint a cada N SKUs processados

# Retry manual para o PUT de imagem (cobre "sem resposta"/timeout que urllib3 não retenta)
PUT_MAX_ATTEMPTS = 3   # Tentativas totais por imagem (1 original + 2 retries)
PUT_RETRY_DELAY  = 5   # Segundos de espera entre tentativas do PUT

# Campos somente-leitura que a VTEX rejeita quando reenviados no PUT.
# Usados no fallback de tentativa 2 em update_image_alt().
IMAGE_PUT_BLOCKED_FIELDS = {
    "ProductId",      # ID do produto — somente leitura, rejeitado no PUT
    "SkuId",          # ID do SKU — somente leitura, rejeitado no PUT
    "FileLocation",   # Caminho interno — somente leitura
    "ImageCompany",   # Campo interno — não aceito no PUT
    "StoreUrl",       # URL de loja — somente leitura
}

# Esquemas de URL que a VTEX não aceita no campo "Url" do payload do PUT.
# URLs S3 internas (s3://...) são geradas pela VTEX internamente e não podem
# ser reenviadas — causam "The 's3' scheme is not supported."
URL_BLOCKED_SCHEMES = ("s3://", "s3a://", "s3n://")

# Locks para operações thread-safe
log_lock  = threading.Lock()
file_lock = threading.Lock()


# ---------------------------------------------------------------------------- #
# SESSÃO HTTP COM RETRY AUTOMÁTICO
# ---------------------------------------------------------------------------- #

def create_session() -> requests.Session:
    """
    Cria uma sessão HTTP com retry automático para erros transitórios.

    Erros cobertos pelo retry:
        429 — Rate limit | 500/502/503/504 — Erros do servidor VTEX

    Returns:
        requests.Session configurada e pronta para uso.
    """
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
# UTILITÁRIOS DE LOG
# ---------------------------------------------------------------------------- #

def log_message(message: str, level: str = "INFO") -> None:
    """
    Registra uma mensagem no console e nos arquivos de log.

    Mensagens com level ERROR ou CRITICAL são gravadas também no error_log.txt,
    facilitando triagem de problemas sem precisar varrer o log completo.

    Args:
        message: Texto da mensagem.
        level:   Nível de severidade (INFO, WARNING, ERROR, CRITICAL).
    """
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
# GERAÇÃO E NORMALIZAÇÃO DO ALT TEXT
# ---------------------------------------------------------------------------- #

def normalize_product_name(name: str) -> str:
    """
    Normaliza o nome do produto para uso como alt text SEO-friendly.

    Transformações:
        - Remove espaços extras (tabs, quebras de linha, duplos espaços)
        - Converte para letras minúsculas

    Args:
        name: Nome bruto retornado pela API VTEX.

    Returns:
        Nome normalizado ou string vazia se a entrada for inválida.
    """
    if not name:
        return ""
    return " ".join(name.split()).lower().strip()


def generate_alt_text(product_name: str) -> str:
    """
    Gera o alt text final a partir do nome do produto.

    Args:
        product_name: Nome do produto (bruto, sem normalização).

    Returns:
        Alt text normalizado. Retorna "produto" se o nome estiver vazio.

    Exemplos:
        "Smart TV 4K  Samsung" → "smart tv 4k samsung"
        ""                     → "produto"
    """
    normalized = normalize_product_name(product_name)
    return normalized if normalized else "produto"


# ---------------------------------------------------------------------------- #
# DETECÇÃO DE CONTEÚDO NUMÉRICO/LIXO NOS CAMPOS DE ALT TEXT
# ---------------------------------------------------------------------------- #

# Padrão de "conteúdo sujo": valores gerados automaticamente pela VTEX ao fazer
# upload de imagem. Exemplos reais: "240270-0", "240270-0_A", "307783"
# O padrão é: apenas dígitos, hífens, underscores e letras maiúsculas, sem espaço.
# Alt texts legítimos sempre têm letras minúsculas e espaços (ex: "smart tv 4k").
_DIRTY_CONTENT_PATTERN = re.compile(r"^[\d\-_A-Z]+$")


def _is_dirty_content(value: str) -> bool:
    """
    Detecta se um valor de campo (Label ou Text) é lixo gerado automaticamente
    pela VTEX no momento do upload da imagem — e não um alt text SEO válido.

    Valores considerados "sujos" (exemplos reais):
        "240270-0"   → ArchiveId do arquivo
        "240270-0_A" → ArchiveId com sufixo de variação
        "307783"     → Código numérico interno

    Valores considerados limpos (não afetados):
        "smart tv 4k samsung" → alt text legítimo (tem espaço e minúsculas)
        ""                    → vazio (tratado separadamente como ausente)

    Args:
        value: Conteúdo do campo Label ou Text a ser avaliado.

    Returns:
        True se o valor é lixo automático e deve ser substituído.
        False se está vazio ou parece ser um alt text legítimo.
    """
    if not value:
        return False  # Campo vazio não é "sujo", é apenas ausente

    return bool(_DIRTY_CONTENT_PATTERN.match(value.strip()))


def _is_real_image(img: Dict) -> bool:
    """
    Verifica se um slot de imagem possui um arquivo real associado.

    A VTEX pode retornar slots vazios (sem foto cadastrada) com ArchiveId
    nulo ou zero. Tentar atualizar o alt text nesses slots causa erro na API.

    Args:
        img: Dict com os dados da imagem retornados pela API VTEX.

    Returns:
        True se o slot possui imagem real; False se está vazio.
    """
    archive_id = img.get("ArchiveId")
    return archive_id is not None and archive_id != 0


def _build_update_reason(current_label: str, current_text: str, alt_text: str) -> str:
    """
    Monta uma string descritiva explicando por que a imagem será atualizada.
    Usada para tornar o log mais legível e facilitar debugging.

    Exemplos de saída:
        "Label=[SUJO:'240270-0'] | Text=[SUJO:'240270-0_A']"
        "Label=[vazio] | Text=[desatualizado:'outra descrição']"
        "Text=[vazio]"

    Args:
        current_label: Valor atual do campo Label.
        current_text:  Valor atual do campo Text.
        alt_text:      Alt text que será aplicado.

    Returns:
        String com os motivos separados por " | ".
    """
    reasons = []

    for field_name, field_value in [("Label", current_label), ("Text", current_text)]:
        if field_value == alt_text:
            continue  # Este campo já está correto — não precisa mencionar

        if not field_value:
            reasons.append(f"{field_name}=[vazio]")
        elif _is_dirty_content(field_value):
            reasons.append(f"{field_name}=[SUJO:'{field_value}']")
        else:
            reasons.append(f"{field_name}=[desatualizado:'{field_value[:30]}']")

    return " | ".join(reasons) if reasons else "(motivo desconhecido)"


# ---------------------------------------------------------------------------- #
# RATE LIMITER (thread-safe)
# ---------------------------------------------------------------------------- #

class RateLimiter:
    """
    Controla o intervalo mínimo entre requisições à API da VTEX.
    Evita erros 429 (Too Many Requests) em execuções paralelas.

    Uso:
        rate_limiter.wait()  # chame antes de cada requisição HTTP
    """

    def __init__(self, delay: float = RATE_LIMIT_DELAY):
        self._delay        = delay
        self._last_request = 0.0
        self._lock         = threading.Lock()

    def wait(self) -> None:
        """Bloqueia a thread até que o intervalo mínimo entre chamadas seja respeitado."""
        with self._lock:
            elapsed = time.monotonic() - self._last_request
            if elapsed < self._delay:
                time.sleep(self._delay - elapsed)
            self._last_request = time.monotonic()


rate_limiter = RateLimiter()


# ---------------------------------------------------------------------------- #
# CHECKPOINT (controle de progresso entre execuções)
# ---------------------------------------------------------------------------- #

class CheckpointManager:
    """
    Persiste o progresso da execução em arquivo JSON.

    Permite retomar o processamento do ponto onde parou em caso de
    interrupção (Ctrl+C, queda de rede, erro fatal, etc.).

    Uso:
        checkpoint = CheckpointManager()
        checkpoint.is_processed(sku_id)   # verifica se já foi processado
        checkpoint.mark_processed(sku_id) # marca como concluído
        checkpoint.save()                 # persiste no arquivo
    """

    def __init__(self, filename: str = CHECKPOINT_FILE):
        self._filename = filename
        self._data     = self._load()

    def _load(self) -> Dict:
        """Carrega o checkpoint do arquivo. Retorna estrutura vazia se não existir."""
        if os.path.exists(self._filename):
            try:
                with open(self._filename, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                log_message("Checkpoint corrompido — iniciando do zero.", "WARNING")
        return {"processed_skus": []}

    def save(self) -> None:
        """Persiste o estado atual do checkpoint no arquivo JSON."""
        with log_lock:
            try:
                with open(self._filename, "w", encoding="utf-8") as f:
                    json.dump(self._data, f)
            except Exception as exc:
                log_message(f"Erro ao salvar checkpoint: {exc}", "ERROR")

    def mark_processed(self, sku_id: int) -> None:
        """Marca um SKU como processado. Idempotente — não duplica entradas."""
        if sku_id not in self._data["processed_skus"]:
            self._data["processed_skus"].append(sku_id)

    def is_processed(self, sku_id: int) -> bool:
        """Retorna True se o SKU já foi processado em uma execução anterior."""
        return sku_id in self._data["processed_skus"]

    def clear(self) -> None:
        """Apaga o histórico de progresso para forçar reprocessamento total."""
        self._data = {"processed_skus": []}
        self.save()


# ---------------------------------------------------------------------------- #
# GERENCIADOR DO ARQUIVO DE SKUs
# ---------------------------------------------------------------------------- #

class SKUFileManager:
    """
    Gerencia o arquivo de SKUs pendentes, removendo cada ID após processamento.

    Ao remover o SKU do arquivo imediatamente após o sucesso, o arquivo
    funciona como uma fila persistente — basta reabrir o arquivo para saber
    o que ainda falta processar.
    """

    def __init__(self, filename: str = SKU_LIST_FILE):
        self._filename = filename

    def mark_for_removal(self, sku_id: int) -> None:
        """
        Remove o SKU do arquivo de IDs pendentes.

        A remoção é feita de forma segura com file_lock para evitar
        condições de corrida em execuções com múltiplos workers.

        Args:
            sku_id: ID do SKU a remover.
        """
        if not os.path.exists(self._filename):
            return

        try:
            with file_lock:
                with open(self._filename, "r", encoding="utf-8") as f:
                    lines = f.readlines()

                new_lines = []
                removed   = False

                for line in lines:
                    stripped = line.strip()

                    # Preserva linhas vazias e comentários
                    if not stripped or stripped.startswith("#"):
                        new_lines.append(line)
                        continue

                    try:
                        if int(stripped) == sku_id:
                            removed = True
                            continue  # Remove este SKU
                        new_lines.append(line)
                    except ValueError:
                        new_lines.append(line)  # Linha inválida — preserva sem alterar

                if removed:
                    with open(self._filename, "w", encoding="utf-8") as f:
                        f.writelines(new_lines)
                    log_message(f"✓ SKU {sku_id} removido do arquivo de pendentes")

        except Exception as exc:
            log_message(f"Erro ao remover SKU {sku_id} do arquivo: {exc}", "ERROR")

    def get_remaining_count(self) -> int:
        """
        Retorna quantos SKUs ainda estão pendentes no arquivo.

        Returns:
            Contagem de linhas válidas (ignora vazias e comentários).
        """
        if not os.path.exists(self._filename):
            return 0

        try:
            with open(self._filename, "r", encoding="utf-8") as f:
                return sum(
                    1 for line in f
                    if line.strip() and not line.strip().startswith("#")
                    and line.strip().lstrip("-").isdigit()
                )
        except Exception:
            return 0


sku_file_manager = SKUFileManager()


# ---------------------------------------------------------------------------- #
# CARREGAMENTO DA LISTA DE SKUs
# ---------------------------------------------------------------------------- #

def load_sku_list(filename: str = SKU_LIST_FILE) -> List[int]:
    """
    Lê a lista de SKU IDs do arquivo texto.

    Formato esperado do arquivo:
        - Um ID inteiro por linha
        - Linhas iniciadas com # são tratadas como comentários e ignoradas
        - Linhas em branco são ignoradas

    Args:
        filename: Caminho do arquivo com os IDs.

    Returns:
        Lista de IDs inteiros. Retorna lista vazia em caso de erro.
    """
    if not os.path.exists(filename):
        log_message(f"Arquivo não encontrado: {filename}", "CRITICAL")
        return []

    sku_ids = []

    try:
        with open(filename, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()

                if not line or line.startswith("#"):
                    continue

                try:
                    sku_ids.append(int(line))
                except ValueError:
                    log_message(f"ID inválido na linha {line_num}: '{line}'", "WARNING")

        log_message(f"Carregados {len(sku_ids)} SKU IDs de '{filename}'")
        return sku_ids

    except Exception as exc:
        log_message(f"Erro ao ler lista de SKUs: {exc}", "CRITICAL")
        return []


# ---------------------------------------------------------------------------- #
# FUNÇÕES DE REQUISIÇÃO VTEX
# ---------------------------------------------------------------------------- #

def safe_request(method: str, url: str, **kwargs) -> Optional[requests.Response]:
    """
    Executa uma requisição HTTP com tratamento de erros e rate limit.

    Em caso de 429 (rate limit), aguarda o tempo indicado pelo header
    Retry-After e tenta novamente uma única vez.

    Args:
        method: Verbo HTTP ("GET", "PUT", etc.)
        url:    Endpoint completo da API VTEX.
        **kwargs: Argumentos extras repassados ao requests.

    Returns:
        Objeto Response ou None em caso de falha de rede/timeout.
    """
    rate_limiter.wait()

    try:
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
        kwargs.setdefault("headers", HEADERS)

        response = SESSION.request(method, url, **kwargs)

        # Rate limit explícito da VTEX — aguarda e tenta uma vez mais
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            log_message(f"[RATE LIMIT] Aguardando {retry_after}s antes de retry...", "WARNING")
            time.sleep(retry_after)
            return safe_request(method, url, **kwargs)

        return response

    except requests.exceptions.Timeout:
        log_message(f"[TIMEOUT] {method} {url}", "ERROR")
        return None
    except requests.exceptions.ConnectionError as exc:
        log_message(f"[CONN ERROR] {method} {url} — {exc}", "ERROR")
        return None
    except Exception as exc:
        log_message(f"[UNEXPECTED] {method} {url} — {exc}", "ERROR")
        return None


def get_sku_details(sku_id: int) -> Tuple[Optional[str], Optional[str], int]:
    """
    Busca o nome e o RefId de um SKU na API VTEX.

    O código HTTP é retornado junto para que o chamador possa distinguir
    entre SKU inexistente (404) e falha de autenticação (401/403).
    Essa distinção é crítica: um 401 indica cookie expirado e deve
    interromper toda a execução — não apenas pular o SKU atual.

    Args:
        sku_id: ID do SKU na VTEX.

    Returns:
        Tupla (nome_produto, ref_id, http_status):
            nome_produto — nome do produto ou None se não encontrado/erro
            ref_id       — RefId do SKU ou None
            http_status  — código HTTP da resposta (0 se sem resposta)
    """
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



def _sanitize_image_payload(payload: Dict) -> Dict:
    """
    Sanitiza o payload de imagem antes de enviá-lo no PUT para a VTEX.

    Problemas tratados:

        (a) Campo "Url" ausente ou nulo → injetado a partir do ArchiveId.
            O GET de imagens da VTEX às vezes retorna payloads sem o campo
            "Url" (completamente ausente, não apenas nulo). O PUT exige "Url"
            obrigatoriamente — sem ela retorna "Field Url is required".
            Solução: monta a URL padrão pública a partir do ArchiveId:
                https://{account}.vteximg.com.br/arquivos/ids/{ArchiveId}

        (b) Campo "Url" com esquema S3 (s3://...) → substituído pela URL pública.
            URLs de armazenamento interno S3 não são aceitas no PUT —
            causam "The 's3' scheme is not supported."
            Solução: mesma injeção da URL pública via ArchiveId.

    Args:
        payload: Cópia do dict de imagem retornado pelo GET da VTEX.

    Returns:
        Novo dict com "Url" garantida e válida. O payload original não é mutado.
    """
    sanitized = payload.copy()
    url_value  = sanitized.get("Url")
    archive_id = sanitized.get("ArchiveId")

    url_is_absent  = "Url" not in sanitized or url_value is None
    url_is_s3      = (
        isinstance(url_value, str)
        and url_value.lower().startswith(URL_BLOCKED_SCHEMES)
    )

    if url_is_absent or url_is_s3:
        if archive_id:
            # Monta a URL pública padrão da VTEX a partir do ArchiveId
            # Exemplo: https://bemol.vteximg.com.br/arquivos/ids/214578
            sanitized["Url"] = f"{VTEX_IMG_BASE_URL}/{archive_id}"
        else:
            # Sem ArchiveId não é possível construir a URL — remove o campo
            # para evitar enviar valor inválido (o fallback 400 tentará sem ele)
            sanitized.pop("Url", None)

    return sanitized


def _put_image(url: str, payload: Dict) -> Optional[requests.Response]:
    """
    Executa o PUT de imagem com retry manual e backoff.

    O retry automático da urllib3 cobre apenas erros HTTP explícitos (429/5xx).
    Timeouts silenciosos (sem resposta) retornam None e não são retentados
    automaticamente — por isso este retry manual é necessário.

    Cada tentativa aguarda PUT_RETRY_DELAY segundos antes de tentar novamente.
    A última tentativa não aguarda (não há próxima tentativa após ela).

    Args:
        url:     Endpoint completo do PUT de imagem.
        payload: Corpo da requisição (dict serializado como JSON).

    Returns:
        Objeto Response da primeira tentativa bem-sucedida, ou None se todas falharem.
    """
    for attempt in range(1, PUT_MAX_ATTEMPTS + 1):
        response = safe_request("PUT", url, json=payload)

        if response is not None:
            # Recebeu resposta HTTP (independente do status) — retorna para o chamador decidir
            return response

        # Sem resposta (timeout/queda de conexão) — tenta novamente se ainda há tentativas
        is_last_attempt = (attempt == PUT_MAX_ATTEMPTS)
        if is_last_attempt:
            log_message(
                f"      [PUT TIMEOUT] {PUT_MAX_ATTEMPTS} tentativas sem resposta — desistindo.",
                "ERROR",
            )
        else:
            log_message(
                f"      [PUT TIMEOUT] Tentativa {attempt}/{PUT_MAX_ATTEMPTS} sem resposta. "
                f"Aguardando {PUT_RETRY_DELAY}s antes de retry...",
                "WARNING",
            )
            time.sleep(PUT_RETRY_DELAY)

    return None


def update_image_alt(sku_id: int, image_data: Dict, new_alt_text: str) -> bool:
    """
    Atualiza o Label e o Text (alt text) de uma imagem específica na VTEX.

    Estratégia de payload e retry:
        Tentativa 1 — payload completo (todos os campos retornados pelo GET)
                       com retry automático em caso de timeout (via _put_image)
        Tentativa 2 — payload filtrado (apenas campos aceitos pelo PUT)
                       acionado se a tentativa 1 retornar HTTP 400
                       também com retry automático em caso de timeout

    Args:
        sku_id:       ID do SKU na VTEX.
        image_data:   Dict com os dados da imagem (retornado pelo GET de imagens).
        new_alt_text: Texto do alt text a ser aplicado.

    Returns:
        True se a atualização foi bem-sucedida; False caso contrário.
    """
    file_id = image_data.get("Id")
    url     = f"{BASE_URL}/stockkeepingunit/{sku_id}/file/{file_id}"

    # Tentativa 1: payload sanitizado (sem campos problemáticos) com retry em timeout
    # _sanitize_image_payload() remove campos Url nulos ou com esquema S3
    # que causam "Field Url is required" e "The 's3' scheme is not supported"
    payload_full          = _sanitize_image_payload(image_data)
    payload_full["Label"] = new_alt_text
    payload_full["Text"]  = new_alt_text

    response = _put_image(url, payload_full)

    if response is None:
        # Todas as tentativas retornaram timeout — erro já logado em _put_image
        return False

    if response.status_code == 200:
        log_message(f"      [OK] alt text atualizado: '{new_alt_text}'")
        return True

    if response.status_code == 401:
        log_message("      [AUTH ERROR] Cookie expirado ou inválido.", "CRITICAL")
        return False

    # Tentativa 2 (fallback 400): remove campos somente-leitura + sanitiza + retry
    # Aplicada apenas se a tentativa 1 ainda retornar 400 após a sanitização.
    if response.status_code == 400:
        payload_filtered          = {k: v for k, v in image_data.items() if k not in IMAGE_PUT_BLOCKED_FIELDS}
        payload_filtered          = _sanitize_image_payload(payload_filtered)
        payload_filtered["Label"] = new_alt_text
        payload_filtered["Text"]  = new_alt_text

        response2 = _put_image(url, payload_filtered)

        if response2 is None:
            return False

        if response2.status_code == 200:
            log_message(f"      [OK] alt text atualizado via payload filtrado: '{new_alt_text}'")
            return True

        error_msg = response2.text[:300].strip() or "sem corpo na resposta"
        log_message(f"      [UPDATE ERROR] SKU {sku_id} [fallback 400]: HTTP {response2.status_code} — {error_msg}", "ERROR")
        return False

    error_msg = response.text[:300].strip() or "sem corpo na resposta"
    log_message(f"      [UPDATE ERROR] SKU {sku_id}: HTTP {response.status_code} — {error_msg}", "ERROR")
    return False


# ---------------------------------------------------------------------------- #
# PROCESSAMENTO DE IMAGENS DE UM SKU
# ---------------------------------------------------------------------------- #

def process_sku_images(sku_id: int, product_name: str, checkpoint: CheckpointManager) -> bool:
    """
    Atualiza o alt text de todas as imagens de um SKU que precisam de correção.

    Regras de atualização por imagem:
        - Slots sem arquivo real (ArchiveId null/0) são ignorados silenciosamente
        - Atualiza se Label OU Text estiverem vazios
        - Atualiza se Label OU Text contiverem conteúdo "sujo" (ex: "240270-0")
        - Atualiza se Label OU Text forem diferentes do alt text gerado
        - Pula apenas se AMBOS os campos já tiverem o alt text correto

    Args:
        sku_id:       ID do SKU na VTEX.
        product_name: Nome do produto (usado para gerar o alt text).
        checkpoint:   Instância do CheckpointManager para marcar o SKU como concluído.

    Returns:
        True se todas as imagens foram processadas com sucesso; False se houve erro.
    """
    url      = f"{BASE_URL}/stockkeepingunit/{sku_id}/file"
    response = safe_request("GET", url)

    if not response:
        return False

    if response.status_code == 404:
        # SKU sem imagens cadastradas — considera concluído
        checkpoint.mark_processed(sku_id)
        return True

    if response.status_code != 200:
        log_message(f"[GET ERROR] SKU {sku_id} — HTTP {response.status_code}", "ERROR")
        return False

    images  = response.json()
    alt_text = generate_alt_text(product_name)

    if not images:
        checkpoint.mark_processed(sku_id)
        return True

    # Filtra slots vazios (ArchiveId null/0) — a VTEX rejeita PUT nesses slots
    real_images = [img for img in images if _is_real_image(img)]
    empty_slots = len(images) - len(real_images)

    if empty_slots > 0:
        log_message(f"      [SLOT] {empty_slots} slot(s) vazio(s) ignorado(s)")

    if not real_images:
        checkpoint.mark_processed(sku_id)
        return True

    success = True

    for img in real_images:
        current_label = (img.get("Label") or "").strip()
        current_text  = (img.get("Text")  or "").strip()

        label_is_correct = (current_label == alt_text)
        text_is_correct  = (current_text  == alt_text)

        # Pula apenas quando AMBOS os campos já estão corretos
        if label_is_correct and text_is_correct:
            log_message(f"      [SKIP] img_id={img.get('Id')} já está correto: '{alt_text}'")
            continue

        reason = _build_update_reason(current_label, current_text, alt_text)
        log_message(f"      [UPDATE] img_id={img.get('Id')} | {reason} → '{alt_text}'")

        if not update_image_alt(sku_id, img, alt_text):
            success = False

    if success:
        checkpoint.mark_processed(sku_id)

    return success


# ---------------------------------------------------------------------------- #
# PROCESSAMENTO DE UM ÚNICO SKU
# ---------------------------------------------------------------------------- #

def process_single_sku(sku_id: int, checkpoint: CheckpointManager) -> Tuple[bool, bool]:
    """
    Executa o fluxo completo para um SKU: busca detalhes, atualiza imagens,
    remove do arquivo de pendentes e marca no checkpoint.

    Args:
        sku_id:     ID do SKU na VTEX.
        checkpoint: Instância do CheckpointManager.

    Returns:
        Tupla (sucesso, deve_abortar):
            sucesso       — True se o SKU foi processado com sucesso
            deve_abortar  — True se foi detectado erro de autenticação (401/403),
                            sinalizando que toda a execução deve ser interrompida
    """
    # SKU já processado em execução anterior — apenas remove do arquivo
    if checkpoint.is_processed(sku_id):
        log_message(f"SKU {sku_id} já processado (checkpoint) — pulando.")
        sku_file_manager.mark_for_removal(sku_id)
        return True, False

    product_name, ref_id, http_status = get_sku_details(sku_id)

    # Cookie expirado ou credenciais inválidas — aborta tudo imediatamente
    # para não remover SKUs do arquivo sem ter processado nenhuma imagem
    if http_status in (401, 403):
        log_message(
            f"[AUTH ERROR] HTTP {http_status} ao buscar SKU {sku_id}. "
            "Cookie expirado ou inválido — interrompendo execução. "
            "Atualize o cookie em VTEX_COOKIE e execute novamente.",
            "CRITICAL",
        )
        return False, True  # sinaliza abort para o runner

    if not product_name:
        # SKU sem dados na VTEX (404 ou erro desconhecido) — marca como concluído
        # para não reprocessar, mas NÃO é falha de autenticação
        log_message(
            f"SKU {sku_id} ignorado — sem detalhes na API VTEX "
            f"(HTTP {http_status or 'sem resposta'}).",
            "WARNING",
        )
        checkpoint.mark_processed(sku_id)
        sku_file_manager.mark_for_removal(sku_id)
        return True, False

    log_message(f"SKU ID: {sku_id} | RefId: {ref_id} | Produto: {product_name}")

    success = process_sku_images(sku_id, product_name, checkpoint)

    if success:
        remaining = sku_file_manager.get_remaining_count()
        sku_file_manager.mark_for_removal(sku_id)
        log_message(f"✓ SKU {sku_id} concluído | Restantes no arquivo: {remaining - 1}")
    else:
        log_message(f"✗ SKU {sku_id} falhou — será tentado novamente na próxima execução.", "WARNING")

    return success, False


# ---------------------------------------------------------------------------- #
# RUNNER PRINCIPAL
# ---------------------------------------------------------------------------- #

def run_bulk_update(resume: bool = True) -> None:
    """
    Orquestra o processamento em lote de todos os SKUs do arquivo.

    Fluxo:
        1. Carrega (ou limpa) o checkpoint
        2. Lê a lista de SKUs do arquivo
        3. Processa em paralelo com ThreadPoolExecutor
        4. Salva checkpoint a cada CHECKPOINT_INTERVAL SKUs
        5. Exibe resumo final

    Args:
        resume: Se True, retoma do checkpoint. Se False, processa tudo do zero.
    """
    checkpoint = CheckpointManager()

    if not resume:
        checkpoint.clear()
        log_message("Checkpoint limpo — iniciando do zero.")

    sku_ids = load_sku_list(SKU_LIST_FILE)

    if not sku_ids:
        log_message("Nenhum SKU para processar. Verifique o arquivo sku_ids.txt.", "CRITICAL")
        return

    log_message("=" * 60)
    log_message("INICIANDO VTEX ALT TEXT UPDATER (NATURAL ALT TEXT)")
    log_message(f"Total de SKUs: {len(sku_ids)} | Workers: {MAX_WORKERS}")
    log_message("=" * 60)

    processed_count = 0

    auth_error = False

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_single_sku, sku_id, checkpoint): sku_id
                for sku_id in sku_ids
            }

            for future in as_completed(futures):
                processed_count += 1
                _, deve_abortar = future.result()

                # Cookie expirado — cancela futures pendentes e encerra imediatamente
                # para não remover SKUs do arquivo sem ter processado as imagens
                if deve_abortar:
                    auth_error = True
                    log_message(
                        "Execução interrompida por erro de autenticação. "
                        "Atualize o cookie VTEX_COOKIE e execute novamente. "
                        "O checkpoint foi salvo — os SKUs já processados não serão repetidos.",
                        "CRITICAL",
                    )
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                # Salva checkpoint periodicamente para minimizar retrabalho em caso de falha
                if processed_count % CHECKPOINT_INTERVAL == 0:
                    checkpoint.save()
                    remaining = sku_file_manager.get_remaining_count()
                    log_message(
                        f"📊 Progresso: {processed_count}/{len(sku_ids)} | "
                        f"Restantes no arquivo: {remaining}"
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
        log_message(f"INTERROMPIDO POR AUTH ERROR | {processed_count}/{len(sku_ids)} SKUs processados")
        log_message("→ Atualize VTEX_COOKIE e execute novamente para continuar.")
    else:
        log_message(f"CONCLUÍDO: {processed_count}/{len(sku_ids)} SKUs processados")

    log_message(f"Restantes no arquivo: {remaining}")
    log_message("=" * 60)


# ---------------------------------------------------------------------------- #
# PONTO DE ENTRADA
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
    if "PASTE_YOUR_COOKIE_HERE" in VTEX_COOKIE:
        print("⚠️  ATENÇÃO: Configure o cookie VTEX na variável de ambiente VTEX_COOKIE.")
        print("    Exemplo: export VTEX_COOKIE='seu_cookie_aqui'")
    else:
        print("=" * 60)
        print("  VTEX IMAGE ALT TEXT UPDATER — SEO NATURAL v7")
        print("=" * 60)
        print(f"  Arquivo de SKUs : {SKU_LIST_FILE}")
        print(f"  Max workers     : {MAX_WORKERS}")
        print(f"  Rate limit delay: {RATE_LIMIT_DELAY}s")
        print(f"  Request timeout : {REQUEST_TIMEOUT}s")
        print("=" * 60)

        if not os.path.exists(SKU_LIST_FILE):
            print(f"\n⚠️  Arquivo '{SKU_LIST_FILE}' não encontrado!")
            print("Crie o arquivo na raiz do projeto com um SKU ID por linha.")
            print("\nExemplo de conteúdo:")
            print("  12345")
            print("  67890")
            print("  # Isto é um comentário — linha ignorada")
            print("  11111")
        else:
            resume  = input("\nRetomar do checkpoint? (S/n): ").strip().lower() != "n"
            confirm = input("Digite 'SIM' para iniciar: ").strip()

            if confirm == "SIM":
                run_bulk_update(resume=resume)
            else:
                print("Execução cancelada.")