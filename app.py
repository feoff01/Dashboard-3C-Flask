# app.py — Dashboard 3C+ (exclui agente e adiciona gráfico empilhado)
from flask import Flask, jsonify, send_file, request
import os, json, re, requests
from datetime import datetime, timedelta, timezone
from threading import Thread, Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict

app = Flask(__name__)

# ===================== CONFIG =====================
TOKEN   = os.getenv("THREEC_TOKEN", "jKymeJxnAdcGIIZ7zxFjYJOp90j9QdHpMmBFSHlOwXpZrNkyFQ1ghNdE3L46")
BASEURL = os.getenv("THREEC_BASEURL", "https://barsixp.3c.plus/api/v1")
URL     = f"{BASEURL}/calls"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
TZ = timezone(timedelta(hours=-3))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "900"))

# >>> Agentes a excluir de TODAS as contas <<<
EXCLUDED_AGENTS = { "Taina Jaremczuk" }

# ===================== HTTP SESSION =====================
session = requests.Session()
session.headers.update(HEADERS)
retry = Retry(total=3, backoff_factor=0.6,
              status_forcelist=[429, 500, 502, 503, 504],
              allowed_methods=False)
adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
session.mount("http://", adapter)
session.mount("https://", adapter)

# ===================== CACHE =====================
dados_cache = []
cache_info  = {}
prev_cache  = {"date": None, "resumo": None, "graficos": None}
cache_lock = Lock()
last_fetch_at = None
fetch_in_progress = False
generation = 0

# ===================== HELPERS =====================
def now_local(): return datetime.now(TZ)
def to_utc_str(dt): return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def tempo_para_segundos(hms):
    try: h, m, s = map(int, (hms or "00:00:00").split(":")); return h*3600+m*60+s
    except: return 0
def norm(s): return (s or "").strip()
def parse_date(date_str):
    if not date_str: return None
    try:
        if "T" in date_str: return datetime.fromisoformat(date_str).astimezone(TZ)
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
    except Exception:
        try: return datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=TZ)
        except: return None
def normalize_agent_name(lig):
    return (lig.get("agent") or lig.get("agent_name") or lig.get("user") or "Desconhecido").strip() or "Desconhecido"
def is_conversion_strict(lig): return (lig.get("qualification") or "").strip().lower() == "convertido"
def rank_stable(items): return sorted(items, key=lambda kv: (-kv[1], kv[0].lower()))
def is_excluded_agent(name: str) -> bool:
    return norm(name) in EXCLUDED_AGENTS

def intervalos_anteriores(hoje_date):
    primeiro_mes = hoje_date.replace(day=1)
    ultimo_mes_passado = primeiro_mes - timedelta(days=1)
    primeiro_mes_passado = ultimo_mes_passado.replace(day=1)
    inicio_semana_atual = hoje_date - timedelta(days=hoje_date.weekday())
    fim_semana_passada = inicio_semana_atual - timedelta(days=1)
    inicio_semana_passada = fim_semana_passada - timedelta(days=6)
    ontem = hoje_date - timedelta(days=1)
    return {"ontem": (ontem, ontem),
            "semana_passada": (inicio_semana_passada, fim_semana_passada),
            "mes_passado": (primeiro_mes_passado, ultimo_mes_passado)}

# ===================== COLETA DA API (com logs) =====================
def fetch_api_data(start_dt_local, end_dt_local, per_page=500, page_max=None, incluir_todas=False):
    data_all, page, total_pages = [], 1, 0
    end_dt_local += timedelta(minutes=5)
    params_base = {
        "filters[call_date][from]": to_utc_str(start_dt_local),
        "filters[call_date][to]": to_utc_str(end_dt_local),
        "per_page": per_page,
    }

    print("\n=== Iniciando coleta da API 3C ===")
    print(f"Período local: {start_dt_local.isoformat()} → {end_dt_local.isoformat()}")
    print("==================================\n")

    while True:
        if page_max is not None and page > page_max:
            print(f"[STOP] Limite de {page_max} páginas atingido.")
            break


        params = list(params_base.items()) + [("page", page)]
        print(f"[→] Requisitando página {page} ...")
        try:
            resp = session.get(URL, params=params, timeout=(10, 120))
        except Exception as e:
            print(f"[ERRO] Falha ao requisitar página {page}: {e}")
            break

        if resp.status_code != 200:
            print(f"[ERRO] página {page}: HTTP {resp.status_code}")
            break

        payload = resp.json()
        bloco = payload.get("data", [])
        if not bloco:
            print(f"[OK] Página {page} retornou 0 registros — fim da coleta.")
            break

        print(f"[✔] Página {page} carregada com {len(bloco)} registros.")
        for lig in bloco:
            # Se não incluir todas, pula registros sem agente
            if not incluir_todas:
                agente_raw = str(lig.get("agent") or lig.get("agent_name") or lig.get("user") or "").strip()
                if not agente_raw or agente_raw == "-":
                    continue
                # Exclui agentes bloqueados já na coleta
                if is_excluded_agent(agente_raw):
                    continue
            data_all.append(lig)

        total_pages += 1
        page += 1

    print(f"\n✅ Coleta concluída — {len(data_all)} registros válidos em {total_pages} páginas.")
    print("==================================\n")

    meta = {
        "base_url": URL,
        "from_local": start_dt_local.isoformat(),
        "to_local": end_dt_local.isoformat(),
        "pages_fetched": total_pages,
        "total_after_filters": len(data_all),
        "excluded_agents": sorted(EXCLUDED_AGENTS),
    }
    return data_all, meta

# ===================== ROTAS =====================
@app.route("/")
def index(): return send_file(os.path.join(os.path.dirname(__file__), "dashboard.html"))

@app.route("/previous")
def previous(): return send_file(os.path.join(os.path.dirname(__file__), "dashboard_prev.html"))

@app.route("/api/status")
def status():
    with cache_lock:
        meta = dict(cache_info)
        gen = generation
        cached = len(dados_cache) > 0
    last = last_fetch_at.isoformat() if last_fetch_at else None
    age = int((now_local() - last_fetch_at).total_seconds()) if last_fetch_at else None
    return jsonify({
        "cached": cached, "fetch_in_progress": fetch_in_progress, "generation": gen,
        "last_fetch_at": last, "age_seconds": age,
        "ttl_seconds": CACHE_TTL_SECONDS, "cache_info": meta
    })

@app.route("/api/pegar")
def pegar_dados():
    global last_fetch_at, fetch_in_progress, generation

    incluir_todas = "raw" in request.args
    force = request.args.get("force") == "1"

    def do_fetch(_incluir_todas):
        global last_fetch_at, fetch_in_progress, generation
        try:
            fetch_in_progress = True
            today = now_local().date()
            primeiro_mes = today.replace(day=1)
            ultimo_mes_passado = primeiro_mes - timedelta(days=1)
            inicio = datetime(ultimo_mes_passado.year, ultimo_mes_passado.month, 1, 0, 0, 0, tzinfo=TZ)
            fim = datetime(today.year, today.month, today.day, 23, 59, 59, tzinfo=TZ)

            dados, meta = fetch_api_data(inicio, fim, incluir_todas=_incluir_todas)
            with cache_lock:
                dados_cache[:] = dados
                cache_info.clear(); cache_info.update(meta)
                last_fetch_at = now_local()
                prev_cache["date"] = prev_cache["resumo"] = prev_cache["graficos"] = None
                generation += 1
            print("✅ Cache atualizado.")
        except Exception as e:
            print(f"[ERRO em thread de coleta] {e}")
        finally:
            fetch_in_progress = False

    with cache_lock:
        cached = len(dados_cache) > 0
        fresh = last_fetch_at and (now_local() - last_fetch_at).total_seconds() < CACHE_TTL_SECONDS

    if force or not cached or not fresh:
        Thread(target=do_fetch, args=(incluir_todas,), daemon=True).start()
        return jsonify({"status": ("stale" if cached else "fetching"),
                        "mensagem": "Coleta iniciada em background.",
                        "meta": cache_info})
    return jsonify({"status": "ok", "mensagem": "Cache fresco.", "meta": cache_info})

# ---- Resumo com filtro de duração para ligações semana
@app.route("/api/resumo")
def resumo_ligacoes():
    with cache_lock: dados = list(dados_cache)
    anterior = request.args.get("prev") is not None
    base = now_local().date()
    hoje_str = base.strftime("%Y-%m-%d")

    if anterior and prev_cache["date"] == hoje_str and prev_cache["resumo"]:
        return jsonify(prev_cache["resumo"])

    if anterior:
        (sem_ini, sem_fim) = intervalos_anteriores(base)["semana_passada"]
        (mes_ini, mes_fim) = intervalos_anteriores(base)["mes_passado"]
    else:
        sem_ini = base - timedelta(days=base.weekday())
        sem_fim = base
        mes_ini = base.replace(day=1)
        mes_fim = base

    conv_sem_por_ag = defaultdict(int)
    conv_mes_por_ag = defaultdict(int)
    lig_sem_por_ag = defaultdict(int)

    for lig in dados:
        dt = parse_date(lig.get("call_date_rfc3339") or lig.get("call_date"))
        if not dt: continue
        d = dt.date()
        ag = normalize_agent_name(lig)
        if is_excluded_agent(ag):  # <<< EXCLUIR
            continue
        dur = tempo_para_segundos(lig.get("speaking_time", "00:00:00"))

        # Ligações semana: apenas > 1s
        if sem_ini <= d <= sem_fim and dur > 1:
            lig_sem_por_ag[ag] += 1

        # Conversões (sem filtro de duração)
        if is_conversion_strict(lig):
            if sem_ini <= d <= sem_fim:
                conv_sem_por_ag[ag] += 1
            if mes_ini <= d <= mes_fim:
                conv_mes_por_ag[ag] += 1

    ag_v_sem = next(iter(rank_stable(conv_sem_por_ag.items())), ("Nenhum agente", 0))[0]
    ag_v_mes = next(iter(rank_stable(conv_mes_por_ag.items())), ("Nenhum agente", 0))[0]
    ag_lig_sem = next(iter(rank_stable(lig_sem_por_ag.items())), ("Nenhum agente", 0))[0]

    resposta = {
        "agente_venda_semana": ag_v_sem,
        "vendas_semana_agente": int(conv_sem_por_ag.get(ag_v_sem, 0)),
        "agente_venda_mes": ag_v_mes,
        "vendas_mes_agente": int(conv_mes_por_ag.get(ag_v_mes, 0)),
        "agente_ligacao_semana": ag_lig_sem,
        "ligacoes_semana_agente": int(lig_sem_por_ag.get(ag_lig_sem, 0)),
        "ranking_conversoes_semana": rank_stable(conv_sem_por_ag.items()),
        "ranking_conversoes_mes": rank_stable(conv_mes_por_ag.items()),
        "ranking_ligacoes_semana": rank_stable(lig_sem_por_ag.items()),
    }
    if anterior:
        prev_cache["date"] = hoje_str
        prev_cache["resumo"] = resposta
    return jsonify(resposta)

# ---- Gráficos do DIA (total >1s; subset >110s)
@app.route("/api/graficos")
def dados_graficos():
    with cache_lock: dados = list(dados_cache)
    anterior = request.args.get("prev") is not None
    base = now_local().date()
    hoje_str = base.strftime("%Y-%m-%d")

    if anterior and prev_cache["date"] == hoje_str and prev_cache["graficos"]:
        return jsonify(prev_cache["graficos"])

    dia = intervalos_anteriores(base)["ontem"][0] if anterior else base

    conv_por_ag = defaultdict(int)
    total_por_ag = defaultdict(int)   # > 1s
    long_por_ag  = defaultdict(int)   # > 110s (1:50)

    for lig in dados:
        dt = parse_date(lig.get("call_date_rfc3339") or lig.get("call_date"))
        if not dt or dt.date() != dia: 
            continue
        ag = normalize_agent_name(lig)
        if is_excluded_agent(ag):  # <<< EXCLUIR
            continue
        dur = tempo_para_segundos(lig.get("speaking_time", "00:00:00"))

        if dur > 1:        # total > 1s
            total_por_ag[ag] += 1
            if dur > 110:  # subset > 110s
                long_por_ag[ag] += 1

        if is_conversion_strict(lig):
            conv_por_ag[ag] += 1

    # Top 5 por TOTAL (>1s)
    top_agents = [ag for ag, _ in rank_stable(total_por_ag.items())[:10]]
    stack = [
        {
            "agente": ag,
            "total": int(total_por_ag.get(ag, 0)),
            "acima_110": int(long_por_ag.get(ag, 0))
        }
        for ag in top_agents
    ]

    resp = {
        "top_vendas_hoje": rank_stable(conv_por_ag.items())[:10],
        "top_ligacoes_hoje": rank_stable(total_por_ag.items())[:10],   # legado
        "top_ligacoes_hoje_stack": stack                              # novo
    }
    if anterior:
        prev_cache["date"] = hoje_str
        prev_cache["graficos"] = resp
    return jsonify(resp)

# ---- Auditoria de conversões (com exclusão)
@app.route("/api/audit/conversoes")
def audit_conversoes():
    with cache_lock: dados = list(dados_cache)
    base = now_local().date()
    if request.args.get("prev"):
        (sem_ini, sem_fim) = intervalos_anteriores(base)["semana_passada"]
        (mes_ini, mes_fim) = intervalos_anteriores(base)["mes_passado"]
    else:
        sem_ini = base - timedelta(days=base.weekday())
        sem_fim = base
        mes_ini = base.replace(day=1)
        mes_fim = base
    conv_sem, conv_mes = defaultdict(int), defaultdict(int)
    for lig in dados:
        dt = parse_date(lig.get("call_date_rfc3339") or lig.get("call_date"))
        if not dt or not is_conversion_strict(lig): 
            continue
        ag = normalize_agent_name(lig)
        if is_excluded_agent(ag):  # <<< EXCLUIR
            continue
        d = dt.date()
        if sem_ini <= d <= sem_fim: conv_sem[ag] += 1
        if mes_ini <= d <= mes_fim: conv_mes[ag] += 1
    return jsonify({"semana": rank_stable(conv_sem.items()), "mes": rank_stable(conv_mes.items())})

# ===================== MAIN =====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
