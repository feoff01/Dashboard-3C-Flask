# app.py — Dashboard 3C+ (layout simplificado + "top hoje" nos gráficos)
# Rodar:  pip install flask requests
#         python app.py

from flask import Flask, jsonify, send_file, request, Response
import requests, os, json
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

# ===================== CONFIG =====================
TOKEN   = os.getenv("THREEC_TOKEN", "jKymeJxnAdcGIIZ7zxFjYJOp90j9QdHpMmBFSHlOwXpZrNkyFQ1ghNdE3L46")
BASEURL = os.getenv("THREEC_BASEURL", "https://barsixp.3c.plus/api/v1")
URL     = f"{BASEURL}/calls"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# Fuso horário local (America/Sao_Paulo)
TZ = timezone(timedelta(hours=-3))

# ===================== CACHE =====================
dados_cache = []           # registros filtrados (ou brutos, se raw=1)
cache_info  = {}           # meta da última coleta
prev_cache  = {"date": None, "resumo": None, "graficos": None}

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
      - speaking_time > 0
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
            resp = requests.get(URL, headers=HEADERS, params=params, timeout=(5, 30))
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

@app.route("/api/pegar")
def pegar_dados():
    """
    Atualiza o cache com dados do 1º dia do mês passado até HOJE 23:59:59.
    - Modo padrão: aplica filtros (speaking_time>0, agent válido)
    - Modo bruto: adicionar ?raw=1 para pegar tudo (para comparação com 3C)
    """
    global dados_cache, cache_info

    hoje = now_local().date()
    primeiro_deste_mes = hoje.replace(day=1)
    ultimo_mes_passado = primeiro_deste_mes - timedelta(days=1)
    inicio = datetime(ultimo_mes_passado.year, ultimo_mes_passado.month, 1, 0, 0, 0, tzinfo=TZ)
    fim = datetime(hoje.year, hoje.month, hoje.day, 23, 59, 59, tzinfo=TZ)

    incluir_todas = "raw" in request.args  # ?raw=1 liga o modo bruto

    try:
        dados, meta = fetch_api_data(
            inicio, fim, per_page=500, page_max=500, incluir_todas=incluir_todas
        )
        dados_cache = dados
        cache_info = meta
        return jsonify({"status": "ok", "mensagem": "Dados prontos.", "meta": meta, "parcial": False})
    except Exception as e:
        return jsonify({"erro": f"Erro ao acessar a API: {e}"}), 500

@app.route("/api/resumo")
def resumo_ligacoes():
    """
    Agrega o que está no cache para os 3 cartões:
      - Top conversão da semana (agente com mais 'Convertido' na semana)
      - Top vendas no mês
      - Top ligações na semana (por contagem de ligações)

    Obs: gráficos foram movidos para "do dia" na rota /api/graficos.
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

        # Semana
        if sem_ini <= data_lig <= sem_fim:
            lig_sem_ag[agente] = lig_sem_ag.get(agente, 0) + 1
            if qualificacao == "Convertido":
                vendas_ag_sem[agente] = vendas_ag_sem.get(agente, 0) + 1

        # Mês
        if mes_ini <= data_lig <= mes_fim and qualificacao == "Convertido":
            vendas_ag_mes[agente] = vendas_ag_mes.get(agente, 0) + 1

    def max_key(d):
        return max(d, key=d.get) if d else "Nenhum agente"

    ag_v_sem   = max_key(vendas_ag_sem)
    ag_v_mes   = max_key(vendas_ag_mes)
    ag_lig_sem = max_key(lig_sem_ag)

    resposta = {
        # Top conversão semana
        "agente_venda_semana": ag_v_sem,
        "vendas_semana_agente": vendas_ag_sem.get(ag_v_sem, 0),

        # Top vendas no mês
        "agente_venda_mes": ag_v_mes,
        "vendas_mes_agente": vendas_ag_mes.get(ag_v_mes, 0),

        # Top ligações na semana
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
      - top_vendas_hoje        (contagem por agente no dia)
      - top_ligacoes_hoje      (contagem por agente no dia)

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

        # vendas do dia
        if qualificacao == "Convertido":
            vendas_ag[agente] = vendas_ag.get(agente, 0) + 1

        # ligações do dia (todas no cache já têm speaking_time>0 e agente válido)
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
