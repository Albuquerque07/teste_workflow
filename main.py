import requests
from pyairtable import Api
import time
import os
from dotenv import load_dotenv

# ==========================================
# CONFIGURAÇÕES E CREDENCIAIS
# ==========================================
PIPEFY_TOKEN = os.getenv("PIPEFY_TOKEN") #Coloquei essa linha

PIPE_ID_FUNIL = "306822038" 
PIPE_ID_REUNIAO = "301667528"

AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN") #Coloquei essa linha
AIRTABLE_BASE_ID = "appuIWX51Ek0tYynD" 

TABELA_LEADS = "Dados - Leads" 
TABELA_HISTORICO = "Histórico de fases"

api = Api(AIRTABLE_TOKEN)
tabela_leads = api.table(AIRTABLE_BASE_ID, TABELA_LEADS)
tabela_historico = api.table(AIRTABLE_BASE_ID, TABELA_HISTORICO)

url_pipefy = "https://api.pipefy.com/graphql"
headers = {
    "Authorization": f"Bearer {PIPEFY_TOKEN}",
    "Content-Type": "application/json"
}

def buscar_todos_cards_do_pipe(pipe_id):
    query = """
    query($pipeId: ID!, $cursor: String) {
      allCards(pipeId: $pipeId, first: 50, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id title
            current_phase { name }
            fields { name value }
            phases_history { phase { name } firstTimeIn lastTimeOut }
          }
        }
      }
    }
    """
    todos_cards = []
    tem_proxima_pagina = True
    cursor = None

    while tem_proxima_pagina:
        variaveis = {"pipeId": pipe_id, "cursor": cursor}
        resposta = requests.post(url_pipefy, json={'query': query, 'variables': variaveis}, headers=headers)
        dados = resposta.json()

        if 'errors' in dados:
            print(f"Erro ao puxar Pipe {pipe_id}:", dados['errors'])
            break

        cards = dados['data']['allCards']['edges']
        for edge in cards:
            todos_cards.append(edge['node'])

        page_info = dados['data']['allCards']['pageInfo']
        tem_proxima_pagina = page_info['hasNextPage']
        cursor = page_info['endCursor']
        time.sleep(0.2)
        
    return todos_cards

def sincronizar_dados():
    print("⏳ Passo 1: Puxando dados do Pipe de Reunião Diagnóstica (Consultores)...")
    cards_reuniao = buscar_todos_cards_do_pipe(PIPE_ID_REUNIAO)
    dict_consultores = {}
    fases_reuniao_permitidas = ["escopos pendentes", "26.1 digital", "26.1", "26.1 ganho"]
    
    for card in cards_reuniao:
        fase_atual = card['current_phase']['name'].lower() if card['current_phase'] else ""
        if fase_atual not in fases_reuniao_permitidas:
            continue
            
        nome_contato = card['title'].strip().lower() 
        front, back = "", ""
        for field in card.get('fields', []):
            fname = field['name'].lower().strip()
            val = field.get('value')
            if not val: continue
            if "nome do contato" in fname: nome_contato = str(val).strip().lower()
            elif fname == "consultor de front": front = str(val)
            elif fname == "consultor de back": back = str(val)
                
        if nome_contato:
            dict_consultores[nome_contato] = {"front": front, "back": back}

    print("⏳ Passo 2: Mapeando Leads já existentes no Airtable (O Radar)...")
    leads_existentes_brutos = tabela_leads.all(fields=["ID card pipefy", "Fase atual"])
    dicionario_airtable = {}
    for registro in leads_existentes_brutos:
        id_pipefy = str(registro['fields'].get('ID card pipefy', ''))
        if id_pipefy:
            dicionario_airtable[id_pipefy] = {
                "airtable_id": registro['id'],
                "fase_salva": registro['fields'].get('Fase atual', '')
            }

    print("⏳ Passo 3: Puxando dados do Pipe do Funil de Vendas (Principal)...")
    cards_funil = buscar_todos_cards_do_pipe(PIPE_ID_FUNIL)
    cards_processados = 0

    print("🚀 Passo 4: Cruzando dados e executando o Upsert no Airtable...")
    for card in cards_funil:
        nome_contato = card['title'] 
        email, phone, prospec, keyword = "", "", "", ""
        motivo_desq, valor_final, motivo_perda, motivo_desc = "", None, "", ""
        
        fase_atual = card['current_phase']['name'].strip() if card['current_phase'] else ""
        pipefy_id = str(card['id'])

        for field in card.get('fields', []):
            fname = field['name'].lower().strip()
            val = field.get('value')
            if not val: continue

            if fname == "nome do contato": nome_contato = str(val)
            elif fname == "e-mail do contato": email = str(val)
            elif fname == "telefone do contato": phone = str(val)
            
            # TRATAMENTO DA PROSPECÇÃO (Indicação -> Ativa)
            elif fname == "prospecção": 
                prospec = str(val).strip()
                if prospec.lower() in ["indicação", "indicacao"]:
                    prospec = "Ativa"
                    
            elif fname == "palavra chave": keyword = str(val)
            elif fname == "o que fez o lead ser desqualificado?": motivo_desq = str(val)
            elif fname == "motivação da perda": motivo_perda = str(val)
            elif fname == "o que fez o lead ser descartado?": motivo_desc = str(val)
            elif fname == "valor final negociado": valor_final = val

        registro_lead = {
            "ID card pipefy": pipefy_id,
            "Cliente": card['title'],
            "Fase atual": fase_atual,
        }
        
        if nome_contato: registro_lead["Nome do contato"] = nome_contato
        if email: registro_lead["Email"] = email
        if phone: registro_lead["Phone"] = phone
        if prospec: registro_lead["Prospecção"] = prospec
        if keyword: registro_lead["Palavra Chave"] = keyword
        if motivo_desq: registro_lead["Motivo Desqualificação"] = motivo_desq
        if motivo_perda: registro_lead["Motivação da perda"] = motivo_perda
        if motivo_desc: registro_lead["Motivo Descarte"] = motivo_desc

        if valor_final:
            try:
                v_limpo = str(valor_final).replace("R$", "").replace(".", "").replace(",", ".").strip()
                registro_lead["Valor final"] = float(v_limpo)
            except ValueError:
                pass 

        chave_busca = str(nome_contato).strip().lower()
        if chave_busca in dict_consultores:
            if dict_consultores[chave_busca]["front"]: registro_lead["Consultor de Front"] = dict_consultores[chave_busca]["front"]
            if dict_consultores[chave_busca]["back"]: registro_lead["Consultor de Back"] = dict_consultores[chave_busca]["back"]

        # ==========================================
        # LÓGICA DE UPSERT E VARREDURA DE HISTÓRICO
        # ==========================================
        fases_para_inserir = []
        historico_pipefy = card.get('phases_history', [])
        
        if pipefy_id in dicionario_airtable:
            # LEAD JÁ EXISTE NO AIRTABLE
            airtable_lead_id = dicionario_airtable[pipefy_id]['airtable_id']
            fase_antiga = dicionario_airtable[pipefy_id]['fase_salva']
            
            try:
                tabela_leads.update(airtable_lead_id, registro_lead)
                print(f"🔄 Lead Atualizado: {card['title']}")
                
                # Se a fase mudou, o lead andou. Vamos descobrir as fases intermediárias.
                if fase_atual != fase_antiga:
                    index_corte = -1
                    # Procura em que momento do histórico ele estava na fase_antiga
                    for i, h in enumerate(historico_pipefy):
                        if h['phase']['name'].strip().lower() == fase_antiga.strip().lower():
                            index_corte = i 
                            
                    if index_corte != -1:
                        # Pega todas as fases que aconteceram DEPOIS da fase_antiga
                        fases_para_inserir = historico_pipefy[index_corte + 1:]
                    else:
                        # Fallback de segurança: Se renomearam a fase no Pipefy e ele não achou, insere a última
                        fases_para_inserir = [historico_pipefy[-1]] if historico_pipefy else []
                        
            except Exception as e:
                print(f"❌ Erro ao atualizar Lead {card['title']}: {e}")
                continue
        else:
            # LEAD NOVO NO AIRTABLE
            try:
                novo_lead = tabela_leads.create(registro_lead)
                airtable_lead_id = novo_lead['id']
                print(f"✅ Lead Novo Inserido: {card['title']}")
                
                # Como é um lead inédito, inserimos todo o histórico de vida dele
                fases_para_inserir = historico_pipefy 
            except Exception as e:
                print(f"❌ Erro ao criar Lead {card['title']}: {e}")
                continue

        # ==========================================
        # CRIAÇÃO DAS LINHAS DE HISTÓRICO
        # ==========================================
        for fase in fases_para_inserir:
            registro_historico = {
                "Dados - Leads": [airtable_lead_id],
                "Nome da fase": fase['phase']['name'].strip()
            }
            
            data_entrada = fase.get('firstTimeIn')
            data_saida = fase.get('lastTimeOut')
            
            if data_entrada:
                registro_historico["Data de entrada"] = str(data_entrada)[:10]
            if data_saida:
                registro_historico["Data de saída"] = str(data_saida)[:10]
            
            try:
                tabela_historico.create(registro_historico)
            except Exception as e:
                print(f"❌ Erro no histórico do lead {card['title']}: {e}")
            
        cards_processados += 1
        time.sleep(0.25)

    print(f"\n🎉 Tudo finalizado! {cards_processados} cards do Funil foram cruzados e sincronizados com histórico detalhado.")

if _name_ == "_main_":
    sincronizar_dados()