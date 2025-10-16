# app.py — Dashboard 3C+ com cache + coleta em background
# Rodar local:  pip install flask requests
#               python app.py

from flask import Flask, jsonify, send_file, request, Response
import requests, os, json
from datetime import datetime, timedelta, timezone
from threading import Thread, Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

# ===================== CONFIG =====================
TOKEN   = os.getenv("THREEC_TOKEN", "jKymeJxnAdcGIIZ7zxFjYJOp90j9QdHpMmBFSHlOwXpZrNkyFQ1ghNdE3L46")
BASEURL = os.getenv("THREEC_BASEURL", "https://barsixp.3c.plus/api/v1")
URL     = f"{BASEURL}/calls"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# Fuso horário local (America/Sao_Paulo)
TZ = timezone(timedelta(hours=-3))

# Quanto tempo um cache é considerado "fresco"
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))  # 15 min por padrão

# ===================== HTTP SESSION (pool + retry) =====================
session = requests.Session()
session.headers.update(HEADERS)
retry = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ===================== CACHE =====================
dados_cache = []           # registros filtrados (ou brutos, se raw=1)
cache_info  = {}           # meta da última coleta
prev_cache  = {"date": None, "resumo": None, "graficos": None}

cache_lock = Lock()
last_fetch_at = None       # datetime da última coleta concluída

# ===================== HELPERS =====================
def now_local():
    return datetime.now(TZ)

def to_utc_str(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def tempo_para_segundos(hms: str) -> int:
    try:
        h, m, s = map(int, (hms or "00:00:00").split(":"))
        return h * 3600 + m * 60 + s
    except Exception:
        return 0

def segundos_para_hms(seg: int) -> str:
    return str(timedelta(seconds=max(0, int(seg))))

def normalize_agent_name(lig) -> str:
    name = (lig.get("agent") or lig.get("agent_name") or lig.get("user") or "").strip()
    return name if name else "Desconhecido"

def parse_date(date_str: str):
    if not date_str:
        return None
    try:
        if "T" in date_str:
            return datetime.fromisoformat(date_str).astimezone(TZ)
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except Exception:
        try:
            return datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=TZ)
        except Exception:
            return None

def intervalos_anteriores(hoje_date):
    primeiro_deste_mes = hoje_date.replace(day=1)
    ultimo_mes_passado = primeiro_deste_mes - timedelta(days=1)
    primeiro_mes_passado = ultimo_mes_passado.replace(day=1)
    inicio_semana_atual = hoje_date - timedelta(days=hoje_date.weekday())  # segunda
    fim_semana_passada = inicio_semana_atual - timedelta(days=1)           # domingo anterior
    inicio_semana_passada = fim_semana_passada - timedelta(days=6)         # segunda anterior
    ontem = hoje_date - timedelta(days=1)
    return {
        "ontem": (ontem, ontem),
        "semana_passada": (inicio_semana_passada, fim_semana_passada),
        "mes_passado": (primeiro_mes_passado, ultimo_mes_passado),
    }

# ===================== COLETA DA API =====================
def fetch_api_data(start_dt_local: datetime, end_dt_local: datetime,
                   per_page=500, page_max=500, incluir_todas=False):
    """
    Busca paginada no endpoint /calls usando o campo 'call_date'.
    Tolerância de +5min no final para não perder registros na virada.

    Filtros padrão (quando incluir_todas=False):
      - speaking_time > 110s
      - agent não é '-' nem vazio
    """
    data_all = []
    page = 1
    total_pages = 0
    field = "call_date"

    end_dt_local = end_dt_local + timedelta(minutes=5)  # tolerância

    params_base = {
        f"filters[{field}][from]": to_utc_str(start_dt_local),
        f"filters[{field}][to]":   to_utc_str(end_dt_local),
        "per_page": per_page,
    }

    while True:
        if page > page_max:
            break
        params = list(params_base.items()) + [("page", page)]
        try:
            # usa sessão com pool + retry
            resp = session.get(URL, params=params, timeout=(10, 120))
        except Exception as e:
            print(f"[ERRO] Falha na página {page}: {e}")
            break

        if resp.status_code != 200:
            print(f"[{page}] HTTP {resp.status_code}")
            break

        payload = resp.json()
        page_data = payload.get("data", [])
        if not page_data:
            break

        mantidos = 0
        for lig in page_data:
            if incluir_todas:
                data_all.append(lig)
                mantidos += 1
                continue

            agente = str(lig.get("agent", "")).strip()
            tempo_falado = tempo_para_segundos(lig.get("speaking_time", "00:00:00"))
            if tempo_falado <= 110:
                continue
            if not agente or agente == "-":
                continue

            data_all.append(lig)
            mantidos += 1

        print(f"[{page}] registros={len(page_data)}  mantidos={mantidos}")
        total_pages += 1
        page += 1

    meta = {
        "base_url": URL,
        "field": field,
        "from_local": start_dt_local.isoformat(),
        "to_local": end_dt_local.isoformat(),
        "per_page": per_page,
        "pages_fetched": total_pages,
        "total_after_filters": len(data_all),
        "incluir_todas": incluir_todas,
    }
    return data_all, meta

# ===================== ROTAS WEB (DASHBOARD) =====================
@app.route("/")
def index():
    return send_file(os.path.join(os.path.dirname(__file__), "dashboard.html"))

@app.route("/previous")
def previous():
    return send_file(os.path.join(os.path.dirname(__file__), "dashboard_prev.html"))

# ---- status rápido do cache (debug)
@app.route("/api/status")
def status():
    global last_fetch_at
    with cache_lock:
        cached = len(dados_cache) > 0
        last = last_fetch_at.isoformat() if last_fetch_at else None
    age = None
    if last_fetch_at:
        age = int((now_local() - last_fetch_at).total_seconds())
    return jsonify({
        "cached": cached,
        "last_fetch_at": last,
        "age_seconds": age,
        "ttl_seconds": CACHE_TTL_SECONDS,
        "cache_info": cache_info
    })

# ---- coleta (agora não bloqueia se já houver cache)
@app.route("/api/pegar")
def pegar_dados():
    """
    Atualiza o cache com dados do 1º dia do mês passado até HOJE 23:59:59.
    - Só força nova coleta se: ?force=1 OU cache ausente/stale (> TTL)
    - Se existir cache (mesmo velho), dispara atualização em background e retorna rápido.
    - Para comparar com 3C, use ?raw=1 para incluir tudo (sem filtros).
    """
    global dados_cache, cache_info, last_fetch_at

    def do_fetch(incluir_todas):
        global dados_cache, cache_info, last_fetch_at
        hoje = now_local().date()
        primeiro_deste_mes = hoje.replace(day=1)
        ultimo_mes_passado = primeiro_deste_mes - timedelta(days=1)
        inicio = datetime(ultimo_mes_passado.year, ultimo_mes_passado.month, 1, 0, 0, 0, tzinfo=TZ)
        fim = datetime(hoje.year, hoje.month, hoje.day, 23, 59, 59, tzinfo=TZ)
        try:
            dados, meta = fetch_api_data(
                inicio, fim, per_page=500, page_max=500, incluir_todas=incluir_todas
            )
            with cache_lock:
                # sobrescreve cache de maneira atômica
                dados_cache[:] = dados
                cache_info.clear()
                cache_info.update(meta)
                last_fetch_at = now_local()
        except Exception as e:
            print(f"[ERRO] do_fetch: {e}")

    incluir_todas = "raw" in request.args
    force = request.args.get("force") == "1"

    with cache_lock:
        cached = len(dados_cache) > 0
        fresh = last_fetch_at and (now_local() - last_fetch_at).total_seconds() < CACHE_TTL_SECONDS

    if force or not cached or not fresh:
        if not cached and not fresh and not force:
            # primeira carga: bloqueia (para evitar UI vazia)
            do_fetch(incluir_todas)
            return jsonify({"status": "ok", "mensagem": "Dados coletados.", "meta": cache_info, "parcial": False})
        else:
            # já existe cache? atualiza ao fundo e responde rápido
            Thread(target=do_fetch, args=(incluir_todas,), daemon=True).start()
            return jsonify({"status":"stale","mensagem":"Atualização em background iniciada.","meta":cache_info,"parcial":True})

    # cache fresco
    return jsonify({"status":"ok","mensagem":"Cache fresco.","meta":cache_info,"parcial":False})

@app.route("/api/resumo")
def resumo_ligacoes():
    """
    Agrega o que está no cache para os 3 cartões:
      - Top conversão da semana
      - Top vendas no mês
      - Top ligações na semana
    """
    global dados_cache, prev_cache
    dados = dados_cache
    anterior = request.args.get("prev") is not None

    hoje = now_local().date()
    hoje_str = hoje.strftime("%Y-%m-%d")

    if anterior and prev_cache["date"] == hoje_str and prev_cache["resumo"]:
        return jsonify(prev_cache["resumo"])

    if anterior:
        (sem_ini, sem_fim) = intervalos_anteriores(hoje)["semana_passada"]
        (mes_ini, mes_fim) = intervalos_anteriores(hoje)["mes_passado"]
    else:
        sem_ini = hoje - timedelta(days=hoje.weekday())
        sem_fim = hoje
        mes_ini = hoje.replace(day=1)
        mes_fim = hoje

    vendas_ag_sem, vendas_ag_mes = {}, {}
    lig_sem_ag = {}

    for lig in dados:
        raw_date = lig.get("call_date_rfc3339") or lig.get("call_date")
        dt = parse_date(raw_date)
        if not dt:
            continue
        data_lig = dt.date()
        agente = normalize_agent_name(lig)
        qualificacao = (lig.get("qualification") or "")

        if sem_ini <= data_lig <= sem_fim:
            lig_sem_ag[agente] = lig_sem_ag.get(agente, 0) + 1
            if qualificacao == "Convertido":
                vendas_ag_sem[agente] = vendas_ag_sem.get(agente, 0) + 1

        if mes_ini <= data_lig <= mes_fim and qualificacao == "Convertido":
            vendas_ag_mes[agente] = vendas_ag_mes.get(agente, 0) + 1

    def max_key(d):
        return max(d, key=d.get) if d else "Nenhum agente"

    ag_v_sem   = max_key(vendas_ag_sem)
    ag_v_mes   = max_key(vendas_ag_mes)
    ag_lig_sem = max_key(lig_sem_ag)

    resposta = {
        "agente_venda_semana": ag_v_sem,
        "vendas_semana_agente": vendas_ag_sem.get(ag_v_sem, 0),
        "agente_venda_mes": ag_v_mes,
        "vendas_mes_agente": vendas_ag_mes.get(ag_v_mes, 0),
        "agente_ligacao_semana": ag_lig_sem,
        "ligacoes_semana_agente": lig_sem_ag.get(ag_lig_sem, 0),
    }

    if anterior:
        prev_cache["date"] = hoje_str
        prev_cache["resumo"] = resposta

    return jsonify(resposta)

@app.route("/api/graficos")
def dados_graficos():
    """
    Dados p/ gráficos "do dia":
      - top_vendas_hoje
      - top_ligacoes_hoje
    Se ?prev, considera "ontem".
    """
    global dados_cache, prev_cache
    dados = dados_cache
    anterior = request.args.get("prev") is not None

    hoje = now_local().date()
    hoje_str = hoje.strftime("%Y-%m-%d")

    if anterior and prev_cache["date"] == hoje_str and prev_cache["graficos"]:
        return jsonify(prev_cache["graficos"])

    if anterior:
        dia_base = intervalos_anteriores(hoje)["ontem"][0]
        dia_ini, dia_fim = dia_base, dia_base
    else:
        dia_ini, dia_fim = hoje, hoje

    vendas_ag, lig_ag = {}, {}

    for lig in dados:
        raw_date = lig.get("call_date_rfc3339") or lig.get("call_date")
        dt = parse_date(raw_date)
        if not dt:
            continue
        data_lig = dt.date()
        if not (dia_ini <= data_lig <= dia_fim):
            continue

        agente = normalize_agent_name(lig)
        qualificacao = (lig.get("qualification") or "")

        if qualificacao == "Convertido":
            vendas_ag[agente] = vendas_ag.get(agente, 0) + 1
        lig_ag[agente] = lig_ag.get(agente, 0) + 1

    resposta = {
        "top_vendas_hoje": sorted(vendas_ag.items(), key=lambda x: x[1], reverse=True)[:5],
        "top_ligacoes_hoje": sorted(lig_ag.items(), key=lambda x: x[1], reverse=True)[:5]
    }

    if anterior:
        prev_cache["date"] = hoje_str
        prev_cache["graficos"] = resposta

    return jsonify(resposta)

# ===================== ROTAS DE DIAGNÓSTICO =====================
@app.route("/api/debug_sample")
def debug_sample():
    return Response(json.dumps(dados_cache[:20], indent=2, ensure_ascii=False),
                    mimetype="application/json")

@app.route("/api/agents_all")
def agents_all():
    uniq, seen = [], set()
    for lig in dados_cache:
        name = normalize_agent_name(lig)
        if name and name not in seen and name != "-":
            seen.add(name); uniq.append(name)
    uniq.sort(key=lambda s: s.lower())
    body = {"unique_agents": len(uniq), "agents": uniq, "cache_meta": cache_info}
    return Response(json.dumps(body, indent=2, ensure_ascii=False), mimetype="application/json")

@app.route("/api/agents_overview")
def agents_overview():
    from collections import Counter
    c = Counter()
    for lig in dados_cache:
        name = normalize_agent_name(lig)
        if name and name != "-":
            c[name] += 1
    top = [{"agent": k, "calls": v} for k, v in c.most_common()]
    body = {"total_calls_in_cache": len(dados_cache), "by_agent": top, "cache_meta": cache_info}
    return Response(json.dumps(body, indent=2, ensure_ascii=False), mimetype="application/json")

# ===================== MAIN =====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
