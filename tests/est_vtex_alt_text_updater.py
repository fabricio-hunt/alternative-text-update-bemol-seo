# =============================================================================
# tests/test_vtex_alt_text_updater.py
# =============================================================================
# Testes unitários para o VTEX Alt Text Updater v11.
#
# Princípio central: ZERO chamadas reais à API VTEX.
# Toda interação HTTP é interceptada via unittest.mock.patch.
#
# Organização dos testes:
#   1. generate_alt_text / normalize_product_name
#   2. _is_dirty_content
#   3. _is_real_image
#   4. _build_update_reason
#   5. _sanitize_url_field / _build_vtex_url / _clean_vtex_url
#   6. _build_minimal_payload / _build_full_payload
#   7. safe_request (rate limit, auth error event, 429)
#   8. get_sku_details
#   9. update_image_alt (máquina de estados — todas as estratégias)
#  10. process_sku_images (fluxos: skip, update, auth error, 405)
#  11. CheckpointManager (load, save atômica, mark_processed)
#  12. load_sku_list (BOM, linhas inválidas, comentários)
# =============================================================================

import json
import os
import threading
import pytest

from unittest.mock import MagicMock, patch, mock_open, call
from typing import Dict


# ---------------------------------------------------------------------------- #
# IMPORTAÇÕES DO MÓDULO ALVO
# Reseta o _auth_error_event antes de cada teste para evitar contaminação
# entre testes que setam o evento.
# ---------------------------------------------------------------------------- #

import vtex_alt_text_updater_v11 as sut


@pytest.fixture(autouse=True)
def reset_auth_event():
    """Garante que o threading.Event de auth error esteja limpo antes de cada teste."""
    sut._auth_error_event.clear()
    yield
    sut._auth_error_event.clear()


# ---------------------------------------------------------------------------- #
# HELPERS
# ---------------------------------------------------------------------------- #

def make_response(status_code: int, body=None, text: str = "") -> MagicMock:
    """Cria um mock de requests.Response com status_code e body configurados."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text or (json.dumps(body) if body else "")
    if body is not None:
        resp.json.return_value = body
    return resp


def make_image(
    img_id: int = 1,
    archive_id: int = 12345,
    label: str = "",
    text: str = "",
    url: str = "https://bemol.vteximg.com.br/arquivos/ids/12345/img.jpg?v=1",
) -> Dict:
    return {
        "Id":        img_id,
        "ArchiveId": archive_id,
        "Label":     label,
        "Text":      text,
        "Url":       url,
        "IsMain":    True,
        "Name":      "img.jpg",
        "SkuId":     999,
    }


# ============================================================================ #
# 1. GERAÇÃO DE ALT TEXT
# ============================================================================ #

class TestGenerateAltText:

    def test_normaliza_para_minusculas(self):
        assert sut.generate_alt_text("Smart TV 4K Samsung") == "smart tv 4k samsung"

    def test_remove_espacos_extras(self):
        assert sut.generate_alt_text("  Smart   TV  ") == "smart tv"

    def test_nome_vazio_retorna_produto(self):
        assert sut.generate_alt_text("") == "produto"

    def test_none_retorna_produto(self):
        assert sut.generate_alt_text(None) == "produto"  # type: ignore

    def test_apenas_espacos_retorna_produto(self):
        assert sut.generate_alt_text("   ") == "produto"

    def test_preserva_numeros(self):
        assert sut.generate_alt_text("iPhone 15 Pro Max 256GB") == "iphone 15 pro max 256gb"

    def test_nome_curto_legitimo(self):
        """Nomes curtos legítimos como 'led', 'tv', 'kit' devem ser preservados."""
        assert sut.generate_alt_text("LED") == "led"
        assert sut.generate_alt_text("TV") == "tv"
        assert sut.generate_alt_text("Kit") == "kit"


# ============================================================================ #
# 2. DETECÇÃO DE CONTEÚDO LIXO
# ============================================================================ #

class TestIsDirtyContent:

    # --- Deve detectar como lixo ---

    def test_codigo_numerico(self):
        assert sut._is_dirty_content("240270-0") is True

    def test_codigo_com_sufixo(self):
        assert sut._is_dirty_content("240270-0_A") is True

    def test_somente_numeros(self):
        assert sut._is_dirty_content("307783") is True

    def test_maiusculas_sem_espaco(self):
        assert sut._is_dirty_content("ABCDEF") is True

    def test_placeholder_main(self):
        assert sut._is_dirty_content("Main") is True

    def test_placeholder_main_minusculo(self):
        assert sut._is_dirty_content("main") is True

    def test_placeholder_imagem(self):
        assert sut._is_dirty_content("Imagem") is True

    def test_placeholder_foto(self):
        assert sut._is_dirty_content("foto") is True

    def test_placeholder_image_ingles(self):
        assert sut._is_dirty_content("image") is True

    # --- NÃO deve detectar como lixo (v11 FIX 1) ---

    def test_alt_text_valido(self):
        assert sut._is_dirty_content("smart tv 4k samsung") is False

    def test_nome_curto_legitimo_led(self):
        """'led' é um alt text legítimo — NÃO deve ser lixo (regressão v10)."""
        assert sut._is_dirty_content("led") is False

    def test_nome_curto_legitimo_tv(self):
        """'tv' é um alt text legítimo — NÃO deve ser lixo (regressão v10)."""
        assert sut._is_dirty_content("tv") is False

    def test_nome_curto_legitimo_kit(self):
        """'kit' é um alt text legítimo — NÃO deve ser lixo (regressão v10)."""
        assert sut._is_dirty_content("kit") is False

    def test_valor_vazio_nao_e_lixo(self):
        """Campo vazio é tratado separadamente — não é 'sujo'."""
        assert sut._is_dirty_content("") is False

    def test_frase_com_placeholder_como_substring(self):
        """'foto da câmera' contém 'foto' mas não é idêntica — não deve ser lixo."""
        assert sut._is_dirty_content("foto da câmera principal") is False

    def test_frase_com_image_como_substring(self):
        assert sut._is_dirty_content("image stabilization lens") is False


# ============================================================================ #
# 3. VERIFICAÇÃO DE IMAGEM REAL
# ============================================================================ #

class TestIsRealImage:

    def test_archive_id_valido(self):
        assert sut._is_real_image({"ArchiveId": 12345}) is True

    def test_archive_id_zero(self):
        assert sut._is_real_image({"ArchiveId": 0}) is False

    def test_archive_id_none(self):
        assert sut._is_real_image({"ArchiveId": None}) is False

    def test_sem_campo_archive_id(self):
        assert sut._is_real_image({}) is False


# ============================================================================ #
# 4. BUILD UPDATE REASON
# ============================================================================ #

class TestBuildUpdateReason:

    def test_ambos_vazios(self):
        reason = sut._build_update_reason("", "", "smart tv")
        assert "Label=[vazio]" in reason
        assert "Text=[vazio]" in reason

    def test_label_sujo(self):
        reason = sut._build_update_reason("240270-0", "smart tv", "smart tv")
        assert "SUJO" in reason
        assert "Label" in reason

    def test_label_desatualizado(self):
        reason = sut._build_update_reason("outro texto qualquer", "smart tv", "smart tv")
        assert "desatualizado" in reason

    def test_ambos_corretos_retorna_motivo_desconhecido(self):
        """Se ambos já estão corretos, nunca deve chegar aqui — mas não deve travar."""
        reason = sut._build_update_reason("smart tv", "smart tv", "smart tv")
        assert reason == "(motivo desconhecido)"


# ============================================================================ #
# 5. SANITIZAÇÃO DE URL
# ============================================================================ #

class TestSanitizeUrlField:

    def test_reconstroi_url_a_partir_do_archive_id(self):
        payload = {"ArchiveId": 12345, "Url": None}
        result  = sut._sanitize_url_field(payload)
        assert result["Url"] == "https://bemol.vteximg.com.br/arquivos/ids/12345"

    def test_substitui_url_s3(self):
        payload = {"ArchiveId": 12345, "Url": "s3://bucket/file.jpg"}
        result  = sut._sanitize_url_field(payload)
        assert result["Url"] == "https://bemol.vteximg.com.br/arquivos/ids/12345"

    def test_normaliza_url_com_path_e_querystring(self):
        payload = {
            "ArchiveId": 12345,
            "Url": "https://bemol.vteximg.com.br/arquivos/ids/12345/file.jpg?v=123",
        }
        result = sut._sanitize_url_field(payload)
        assert result["Url"] == "https://bemol.vteximg.com.br/arquivos/ids/12345"

    def test_busca_archive_id_do_payload_original(self):
        """Payload filtrado sem ArchiveId deve buscar no original."""
        payload_filtrado = {"Label": "teste"}
        original         = {"ArchiveId": 99999}
        result           = sut._sanitize_url_field(payload_filtrado, original_payload=original)
        assert result["Url"] == "https://bemol.vteximg.com.br/arquivos/ids/99999"

    def test_sem_archive_id_remove_url_invalida(self):
        payload = {"Url": "s3://bucket/file.jpg"}
        result  = sut._sanitize_url_field(payload)
        assert "Url" not in result

    def test_clean_vtex_url_remove_querystring(self):
        url    = "https://bemol.vteximg.com.br/arquivos/ids/194802/file.jpg?v=638537"
        result = sut._clean_vtex_url(url)
        assert result == "https://bemol.vteximg.com.br/arquivos/ids/194802"

    def test_clean_vtex_url_sem_path(self):
        url    = "https://bemol.vteximg.com.br/arquivos/ids/194802"
        result = sut._clean_vtex_url(url)
        assert result == "https://bemol.vteximg.com.br/arquivos/ids/194802"


# ============================================================================ #
# 6. BUILD MINIMAL PAYLOAD
# ============================================================================ #

class TestBuildMinimalPayload:

    def test_contem_apenas_campos_minimos(self):
        img    = make_image()
        result = sut._build_minimal_payload(img, "smart tv")
        # Campos fora de MINIMAL_PUT_FIELDS não devem estar presentes
        assert "ProductId" not in result
        assert "FileLocation" not in result

    def test_label_e_text_aplicados(self):
        img    = make_image()
        result = sut._build_minimal_payload(img, "notebook gamer")
        assert result["Label"] == "notebook gamer"
        assert result["Text"]  == "notebook gamer"

    def test_url_construida_pelo_archive_id(self):
        img    = make_image(archive_id=55555)
        result = sut._build_minimal_payload(img, "cadeira")
        assert result["Url"] == "https://bemol.vteximg.com.br/arquivos/ids/55555"

    def test_payload_original_nao_e_mutado(self):
        img        = make_image(label="original", text="original")
        img_backup = img.copy()
        sut._build_minimal_payload(img, "novo alt text")
        assert img["Label"] == img_backup["Label"]
        assert img["Text"]  == img_backup["Text"]


# ============================================================================ #
# 7. SAFE_REQUEST
# ============================================================================ #

class TestSafeRequest:

    @patch("vtex_alt_text_updater_v11.SESSION")
    def test_retorna_response_em_200(self, mock_session):
        mock_session.request.return_value = make_response(200)
        resp = sut.safe_request("GET", "https://example.com")
        assert resp.status_code == 200

    @patch("vtex_alt_text_updater_v11.SESSION")
    def test_seta_auth_event_em_401(self, mock_session):
        mock_session.request.return_value = make_response(401)
        sut.safe_request("GET", "https://example.com")
        assert sut._auth_error_event.is_set()

    @patch("vtex_alt_text_updater_v11.SESSION")
    def test_seta_auth_event_em_403(self, mock_session):
        mock_session.request.return_value = make_response(403)
        sut.safe_request("GET", "https://example.com")
        assert sut._auth_error_event.is_set()

    @patch("vtex_alt_text_updater_v11.time.sleep")
    @patch("vtex_alt_text_updater_v11.SESSION")
    def test_retry_em_429_loop_nao_recursao(self, mock_session, mock_sleep):
        """[v11 FIX 8] 429 deve ser tratado em loop, não em recursão."""
        # Simula 429 repetido e depois 200
        mock_session.request.side_effect = [
            make_response(429),
            make_response(429),
            make_response(200),
        ]
        resp = sut.safe_request("GET", "https://example.com")
        assert resp.status_code == 200
        assert mock_session.request.call_count == 3

    @patch("vtex_alt_text_updater_v11.SESSION")
    def test_retorna_none_em_timeout(self, mock_session):
        import requests as req_lib
        mock_session.request.side_effect = req_lib.exceptions.Timeout
        resp = sut.safe_request("GET", "https://example.com")
        assert resp is None

    @patch("vtex_alt_text_updater_v11.SESSION")
    def test_retorna_none_em_connection_error(self, mock_session):
        import requests as req_lib
        mock_session.request.side_effect = req_lib.exceptions.ConnectionError
        resp = sut.safe_request("GET", "https://example.com")
        assert resp is None


# ============================================================================ #
# 8. GET SKU DETAILS
# ============================================================================ #

class TestGetSkuDetails:

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_retorna_nome_e_ref_id(self, mock_req):
        mock_req.return_value = make_response(200, {
            "ProductName": "Smart TV 4K",
            "RefId": "TV001",
        })
        name, ref_id, status = sut.get_sku_details(123)
        assert name   == "Smart TV 4K"
        assert ref_id == "TV001"
        assert status == 200

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_fallback_para_name_complete(self, mock_req):
        mock_req.return_value = make_response(200, {
            "ProductName": None,
            "NameComplete": "Geladeira Frost Free",
            "RefId": "GEL01",
        })
        name, _, _ = sut.get_sku_details(456)
        assert name == "Geladeira Frost Free"

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_retorna_none_em_404(self, mock_req):
        mock_req.return_value = make_response(404)
        name, ref_id, status = sut.get_sku_details(999)
        assert name   is None
        assert ref_id is None
        assert status == 404

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_retorna_zeros_em_timeout(self, mock_req):
        mock_req.return_value = None
        name, ref_id, status = sut.get_sku_details(111)
        assert name   is None
        assert status == 0


# ============================================================================ #
# 9. UPDATE IMAGE ALT — máquina de estados (v11 FIX 2 + FIX 3)
# ============================================================================ #

class TestUpdateImageAlt:

    def _img(self, archive_id=12345):
        return make_image(archive_id=archive_id)

    @patch("vtex_alt_text_updater_v11._put_image")
    def test_t1_sucesso_em_200(self, mock_put):
        mock_put.return_value = make_response(200)
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result is True
        assert mock_put.call_count == 1  # não deve tentar t2, t3, t4

    @patch("vtex_alt_text_updater_v11._put_image")
    def test_t2_acionado_apos_t1_400(self, mock_put):
        mock_put.side_effect = [
            make_response(400, text="bad request"),
            make_response(200),
        ]
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result is True
        assert mock_put.call_count == 2

    @patch("vtex_alt_text_updater_v11.safe_request")
    @patch("vtex_alt_text_updater_v11._put_image")
    def test_t3_acionado_apos_t2_404(self, mock_put, mock_safe):
        mock_put.side_effect = [
            make_response(400, text="error"),
            make_response(404, text="not found"),
        ]
        mock_safe.return_value = make_response(200)  # t3 usa safe_request direto? não — _put_image
        # t3 também usa _put_image
        mock_put.side_effect = [
            make_response(400, text="error"),   # t1
            make_response(404, text="nf"),      # t2
            make_response(200),                 # t3
        ]
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result is True
        assert mock_put.call_count == 3

    @patch("vtex_alt_text_updater_v11.safe_request")
    @patch("vtex_alt_text_updater_v11._put_image")
    def test_t4_post_acionado_apos_t3_405(self, mock_put, mock_safe):
        """[v11 FIX 2] t4 (POST) deve ser tentado quando t3 retorna 405."""
        mock_put.side_effect = [
            make_response(400, text="err"),  # t1
            make_response(405, text="err"),  # t2
            make_response(405, text="err"),  # t3
        ]
        mock_safe.return_value = make_response(200)  # t4 POST
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result is True
        # safe_request chamado no t4 (POST)
        mock_safe.assert_called_once()
        _, call_kwargs = mock_safe.call_args
        # Verifica que foi um POST
        assert mock_safe.call_args[0][0] == "POST"

    @patch("vtex_alt_text_updater_v11.safe_request")
    @patch("vtex_alt_text_updater_v11._put_image")
    def test_skip_405_quando_t4_tambem_retorna_405(self, mock_put, mock_safe):
        """[v11 FIX 2] Se t4 POST também retorna 405, deve retornar 'SKIP_405'."""
        mock_put.side_effect = [
            make_response(405, text="err"),  # t1
            make_response(405, text="err"),  # t2
            make_response(405, text="err"),  # t3
        ]
        mock_safe.return_value = make_response(405, text="err")  # t4 POST
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result == "SKIP_405"

    @patch("vtex_alt_text_updater_v11._put_image")
    def test_auth_error_em_401(self, mock_put):
        """[v11 FIX 3] 401 deve retornar 'AUTH_ERROR', não False."""
        mock_put.return_value = make_response(401)
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result == "AUTH_ERROR"

    @patch("vtex_alt_text_updater_v11._put_image")
    def test_auth_error_em_403(self, mock_put):
        mock_put.return_value = make_response(403)
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result == "AUTH_ERROR"

    @patch("vtex_alt_text_updater_v11._put_image")
    def test_erro_definitivo_422_nao_avanca_estrategias(self, mock_put):
        """[v11 FIX 2] 422 não está em RETRY_ON — deve abortar sem tentar t2/t3/t4."""
        mock_put.return_value = make_response(422, text="unprocessable")
        result = sut.update_image_alt(1, self._img(), "smart tv")
        assert result is False
        assert mock_put.call_count == 1  # apenas t1

    @patch("vtex_alt_text_updater_v11._put_image")
    def test_retorna_false_em_timeout_total(self, mock_put):
        mock_put.return_value = None
        result = sut.update_image_alt(1, self._img(), "smart tv")
        # Todos os timeouts → False (não SKIP_405, pois status é 0)
        assert result is False


# ============================================================================ #
# 10. PROCESS SKU IMAGES
# ============================================================================ #

class TestProcessSkuImages:

    def _make_checkpoint(self):
        cp = MagicMock()
        cp.is_processed.return_value = False
        return cp

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_skip_imagem_ja_correta(self, mock_req):
        img = make_image(label="smart tv 4k samsung", text="smart tv 4k samsung")
        mock_req.return_value = make_response(200, [img])
        cp = self._make_checkpoint()

        with patch("vtex_alt_text_updater_v11.get_sku_details",
                   return_value=("Smart TV 4K Samsung", "TV001", 200)):
            result = sut.process_sku_images(1, "Smart TV 4K Samsung", cp)

        assert result is True
        cp.mark_processed.assert_called_once_with(1)

    @patch("vtex_alt_text_updater_v11.update_image_alt", return_value=True)
    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_atualiza_imagem_com_label_vazio(self, mock_req, mock_update):
        img = make_image(label="", text="")
        mock_req.return_value = make_response(200, [img])
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Smart TV 4K Samsung", cp)

        assert result is True
        mock_update.assert_called_once()
        cp.mark_processed.assert_called_once_with(1)

    @patch("vtex_alt_text_updater_v11.update_image_alt", return_value="AUTH_ERROR")
    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_propaga_auth_error(self, mock_req, mock_update):
        """[v11 FIX 3] AUTH_ERROR de update_image_alt deve ser propagado."""
        img = make_image(label="lixo", text="lixo")
        mock_req.return_value = make_response(200, [img])
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Smart TV 4K Samsung", cp)

        assert result == "AUTH_ERROR"
        cp.mark_processed.assert_not_called()

    @patch("vtex_alt_text_updater_v11._register_skipped_405")
    @patch("vtex_alt_text_updater_v11.update_image_alt", return_value="SKIP_405")
    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_skip_405_registra_e_marca_checkpoint(self, mock_req, mock_update, mock_reg):
        """[v11 FIX 4] SKIP_405 deve registrar o SKU e marcar o checkpoint."""
        img = make_image(label="lixo", text="lixo")
        mock_req.return_value = make_response(200, [img])
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Produto Qualquer", cp)

        assert result is True  # checkpoint avançado
        mock_reg.assert_called_once_with(1)
        cp.mark_processed.assert_called_once_with(1)

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_ignora_slots_vazios(self, mock_req):
        """Slots com ArchiveId=0 ou None devem ser ignorados."""
        imagens = [
            make_image(img_id=1, archive_id=12345, label="smart tv", text="smart tv"),
            {"Id": 2, "ArchiveId": 0, "Label": "", "Text": ""},   # slot vazio
            {"Id": 3, "ArchiveId": None, "Label": "", "Text": ""}, # slot vazio
        ]
        mock_req.return_value = make_response(200, imagens)
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Smart TV", cp)

        assert result is True
        cp.mark_processed.assert_called_once_with(1)

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_sku_sem_imagens_marca_checkpoint(self, mock_req):
        mock_req.return_value = make_response(200, [])
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Produto", cp)

        assert result is True
        cp.mark_processed.assert_called_once_with(1)

    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_404_marca_como_processado(self, mock_req):
        mock_req.return_value = make_response(404)
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Produto", cp)

        assert result is True
        cp.mark_processed.assert_called_once_with(1)

    @patch("vtex_alt_text_updater_v11.update_image_alt", return_value=False)
    @patch("vtex_alt_text_updater_v11.safe_request")
    def test_falha_nao_marca_checkpoint(self, mock_req, mock_update):
        img = make_image(label="lixo", text="lixo")
        mock_req.return_value = make_response(200, [img])
        cp = self._make_checkpoint()

        result = sut.process_sku_images(1, "Produto", cp)

        assert result is False
        cp.mark_processed.assert_not_called()


# ============================================================================ #
# 11. CHECKPOINT MANAGER
# ============================================================================ #

class TestCheckpointManager:

    def test_load_arquivo_inexistente(self, tmp_path):
        cp = sut.CheckpointManager(str(tmp_path / "checkpoint.json"))
        assert cp._data == {"processed_skus": []}

    def test_mark_processed_adiciona_sku(self, tmp_path):
        cp = sut.CheckpointManager(str(tmp_path / "checkpoint.json"))
        cp.mark_processed(42)
        assert 42 in cp._data["processed_skus"]

    def test_mark_processed_nao_duplica(self, tmp_path):
        cp = sut.CheckpointManager(str(tmp_path / "checkpoint.json"))
        cp.mark_processed(42)
        cp.mark_processed(42)
        assert cp._data["processed_skus"].count(42) == 1

    def test_is_processed_verdadeiro(self, tmp_path):
        cp = sut.CheckpointManager(str(tmp_path / "checkpoint.json"))
        cp.mark_processed(99)
        assert cp.is_processed(99) is True

    def test_is_processed_falso(self, tmp_path):
        cp = sut.CheckpointManager(str(tmp_path / "checkpoint.json"))
        assert cp.is_processed(999) is False

    def test_save_atomico_cria_arquivo(self, tmp_path):
        """[v11 FIX 5] Arquivo final deve existir e ser JSON válido após save."""
        filepath = str(tmp_path / "checkpoint.json")
        cp = sut.CheckpointManager(filepath)
        cp.mark_processed(1)
        cp.mark_processed(2)

        assert os.path.exists(filepath)
        with open(filepath, "r") as f:
            data = json.load(f)
        assert 1 in data["processed_skus"]
        assert 2 in data["processed_skus"]

    def test_save_atomico_nao_deixa_arquivo_tmp(self, tmp_path):
        """[v11 FIX 5] Arquivo .tmp não deve sobrar após save bem-sucedido."""
        filepath = str(tmp_path / "checkpoint.json")
        cp = sut.CheckpointManager(filepath)
        cp.mark_processed(1)

        assert not os.path.exists(filepath + ".tmp")

    def test_clear_reseta_estado(self, tmp_path):
        filepath = str(tmp_path / "checkpoint.json")
        cp = sut.CheckpointManager(filepath)
        cp.mark_processed(1)
        cp.mark_processed(2)
        cp.clear()
        assert cp._data["processed_skus"] == []

    def test_load_arquivo_corrompido_inicia_do_zero(self, tmp_path):
        filepath = str(tmp_path / "checkpoint.json")
        with open(filepath, "w") as f:
            f.write("{ INVALIDO JSON }")
        cp = sut.CheckpointManager(filepath)
        assert cp._data == {"processed_skus": []}


# ============================================================================ #
# 12. LOAD SKU LIST
# ============================================================================ #

class TestLoadSkuList:

    def test_carrega_ids_validos(self, tmp_path):
        f = tmp_path / "sku_ids.txt"
        f.write_text("111\n222\n333\n", encoding="utf-8")
        result = sut.load_sku_list(str(f))
        assert result == [111, 222, 333]

    def test_ignora_comentarios(self, tmp_path):
        f = tmp_path / "sku_ids.txt"
        f.write_text("# comentário\n111\n# outro\n222\n", encoding="utf-8")
        result = sut.load_sku_list(str(f))
        assert result == [111, 222]

    def test_ignora_linhas_em_branco(self, tmp_path):
        f = tmp_path / "sku_ids.txt"
        f.write_text("111\n\n\n222\n", encoding="utf-8")
        result = sut.load_sku_list(str(f))
        assert result == [111, 222]

    def test_remove_bom_utf8(self, tmp_path):
        """[v11 FIX 6] BOM \\ufeff no início do arquivo não deve corromper o primeiro ID."""
        f = tmp_path / "sku_ids.txt"
        # Escreve com BOM (como o Notepad do Windows gera)
        f.write_bytes("\ufeff123456\n789\n".encode("utf-8-sig"))
        result = sut.load_sku_list(str(f))
        assert result[0] == 123456  # sem BOM → int() deve funcionar

    def test_linha_invalida_e_ignorada(self, tmp_path):
        f = tmp_path / "sku_ids.txt"
        f.write_text("111\nabc\n222\n", encoding="utf-8")
        result = sut.load_sku_list(str(f))
        assert result == [111, 222]

    def test_arquivo_nao_encontrado(self, tmp_path):
        result = sut.load_sku_list(str(tmp_path / "inexistente.txt"))
        assert result == []