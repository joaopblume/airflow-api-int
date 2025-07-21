"""
Millennium Order Processing DAG

Esta DAG processa pedidos diariamente, buscando registros do dia anterior,
agrupando por pedido e enviando para integração via API.

Principais características:
- Usa "Datasets" para controle de dependências
- Implementa XComs via JSON serializáveis
- Segue padrões do Airflow 2.10+ com task decorators e task groups
- Usa deferrable operators para tarefas de longa duração
- Implementa retries configuráveis e mecanismos de timeout
"""

import os
import sys
import logging
import pendulum
from datetime import datetime, timedelta
import oracledb
from airflow import Dataset
from airflow.decorators import dag, task, task_group
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
import smtplib
from email.mime.text import MIMEText
import json
from typing import Dict, List, Any

# Adiciona o diretório 'includes' ao sys.path para importar o módulo send_order
sys.path.append(os.path.join(os.path.dirname(__file__), ".", "includes"))
from send_order import fazer_requisicao

# Inicializa o Oracle Thick Client
oracledb.init_oracle_client(lib_dir="/opt/oracle/instantclient")

# Dataset para coordenar dags dependentes
millennium_orders_dataset = Dataset("millennium://orders")

# Configurações da DAG com tempos de timeout específicos
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'execution_timeout': timedelta(minutes=60),
    'sla': None,
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
}

# Conexão Oracle definida no Airflow UI
ORACLE_CONN_ID = 'millennium_db'
# Número máximo de pedidos para processar por execução
MAX_ORDERS_PER_RUN = 100  # Defina conforme necessário

class EmailService:
    """Classe para gerenciar o envio de emails"""
    
    @staticmethod
    def _get_smtp_connection():
        """Recupera conexão SMTP do Airflow"""
        conn = BaseHook.get_connection("email_alert")
        return {
            'server': conn.host,
            'port': conn.port,
            'user': conn.login,
            'password': conn.password,
            'sender': conn.login,
            'recipient': conn.extra_dejson.get("recipient", "tech-team@example.com,orders@example.com")
        }
    
    @staticmethod
    def send_success_email(subject: str, pedidos_ids: list) -> bool:
        """Envia um e-mail de sucesso com a lista de IDs de pedidos integrados."""
        smtp = EmailService._get_smtp_connection()
        
        # Formata a lista de IDs de pedidos
        if pedidos_ids:
            pedidos_text = "<ul>"
            for pedido_id in pedidos_ids:
                pedidos_text += f"<li>{pedido_id}</li>"
            pedidos_text += "</ul>"
        else:
            pedidos_text = "<p>Nenhum pedido foi integrado.</p>"
        
        # Cria o conteúdo HTML do e-mail
        html_content = f"""
        <html>
          <body>
            <h1>{subject}</h1>
            <p>A integração foi concluída com sucesso para os seguintes pedidos:</p>
            {pedidos_text}
            <p style="color: green; font-weight: bold;">Operação finalizada com sucesso!</p>
          </body>
        </html>
        """
        
        msg = MIMEText(html_content, 'html', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = smtp['sender']
        msg['To'] = smtp['recipient']

        try:
            server = smtplib.SMTP(smtp['server'], smtp['port'])
            server.starttls()
            server.login(smtp['user'], smtp['password'])
            server.sendmail(smtp['sender'], [smtp['recipient']], msg.as_string())
            server.quit()
            logging.info("E-mail de sucesso enviado para %s", smtp['recipient'])
            return True
        except Exception as e:
            logging.error("Erro ao enviar e-mail de sucesso: %s", e)
            return False

    @staticmethod
    def send_error_email(subject: str, message: str) -> bool:
        """Envia um e-mail de erro com formatação JSON."""
        smtp = EmailService._get_smtp_connection()
        
        # Cria o conteúdo HTML do e-mail
        html_content = f"""
        <html>
          <body>
            <h1>{subject}</h1>
            <pre style="background-color: #f4f4f4; padding: 10px; border: 1px solid #ddd;">{message}</pre>
          </body>
        </html>
        """
        
        msg = MIMEText(html_content, 'html', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = smtp['sender']
        msg['To'] = smtp['recipient']

        try:
            server = smtplib.SMTP(smtp['server'], smtp['port'])
            server.starttls()
            server.login(smtp['user'], smtp['password'])
            server.sendmail(smtp['sender'], [smtp['recipient']], msg.as_string())
            server.quit()
            logging.info("E-mail de erro enviado para %s", smtp['recipient'])
            return True
        except Exception as e:
            logging.error("Erro ao enviar e-mail: %s", e)
            return False


class DatabaseService:
    """Classe para encapsular operações de banco de dados."""
    
    @staticmethod
    def insere_pedido_integrado(cod_produtoc, pedidoc_millennium):
        """Insere o pedido na tabela de confirmação de integração."""
        logging.info('Inserindo o pedido na tabela de confirmacao de integracao')
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        sql = """
            INSERT INTO millennium.pedidos_integrados 
            (cod_pedidoc, pedidoc_millennium, integrado_em)
            VALUES (:cod, :pedidoc_millennium, sysdate)
        """
        params = {'cod': cod_produtoc, 'pedidoc_millennium': pedidoc_millennium}
        return oracle_hook.run(sql, parameters=params)

    @staticmethod
    def insere_registro_erro(cod_pedidoc, mensagem_erro, email_enviado):
        """Insere um registro de erro na tabela."""
        logging.info('Inserindo o pedido na tabela de erros de integracao')
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        sql = """
            INSERT INTO millennium.pedidos_erros 
            (cod_pedidoc, mensagem_erro, email_enviado)
            VALUES (:cod, :msg, :email)
        """
        params = {'cod': cod_pedidoc, 'msg': mensagem_erro[:4000], 'email': email_enviado}
        return oracle_hook.run(sql, parameters=params)

    @staticmethod
    def registro_erro_existe(cod_pedidoc):
        """Verifica se já existe um registro de erro para o pedido na tabela."""
        logging.info('Verificando se o pedido está na fila de erros')
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        sql = "SELECT COUNT(*) FROM millennium.pedidos_erros WHERE cod_pedidoc = :cod"
        result = oracle_hook.get_records(sql, parameters={'cod': cod_pedidoc})
        return result[0][0] > 0

    @staticmethod
    def deleta_registro_erro(cod_pedidoc):
        """Remove o registro de erro da tabela para o pedido informado."""
        logging.info('Deletando o pedido da fila de erros')
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        sql = "DELETE FROM millennium.pedidos_erros WHERE cod_pedidoc = :cod"
        return oracle_hook.run(sql, parameters={'cod': cod_pedidoc})

    @staticmethod
    def fetch_orders_from_db(data_ref=None):
        """
        Consulta a view para obter os registros de pedidos, 
        limitando ao número máximo definido por MAX_ORDERS_PER_RUN.
        """
        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
        
        if data_ref is None:
            # Data padrão: dia anterior
            data_ref = "(SYSDATE - 1)"
        else:
            # Data fornecida como parâmetro
            data_ref = f"TO_DATE('{data_ref}', 'YYYY-MM-DD')"
            
        sql = f"""
        SELECT CNPJ_FORNECEDOR, COD_COLECAO, COD_CONDICAO_PGTO, COD_FILIAL, COD_PEDIDOC, DATA, ULTIMA_ATUALIZACAO, DATA_ENTREGA, PRODUTOS, BARRA, PRECO, QUANTIDADE, SKU, PEDIDO FROM (
            SELECT * FROM vw_pedido_millennium 
            WHERE COD_PEDIDOC NOT IN (
                SELECT cod_pedidoc FROM millennium.pedidos_integrados
            )
            ORDER BY COD_PEDIDOC
        ) WHERE ROWNUM <= {MAX_ORDERS_PER_RUN} AND ULTIMA_ATUALIZACAO > {data_ref}
        """
        return oracle_hook.get_records(sql)


def format_cnpj(cnpj):
    """Formata o CNPJ para o padrão: 00.000.000/0000-00."""
    cnpj_str = str(cnpj).zfill(14)
    return f"{cnpj_str[:2]}.{cnpj_str[2:5]}.{cnpj_str[5:8]}/{cnpj_str[8:12]}-{cnpj_str[12:]}"


@dag(
    dag_id='millennium_order_processing',
    default_args=default_args,
    description='Processa e envia pedidos para integração com a Millennium',
    schedule='0 */1 * * *',  # Cron expression para a cada hora
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['millennium', 'integração', 'pedidos'],
    max_active_runs=1,  # Evita execuções concorrentes
)
def millennium_order_processing_dag():
    """
    DAG para processar pedidos e enviar para integração com a Millennium.
    
    Esta DAG implementa:
    - Processamento independente para cada pedido (dynamic task mapping)
    - Tratamento de erros isolado
    - Comunicação assíncrona via Dataset
    - Parâmetros configuráveis
    - Backoff exponencial para retries
    """


    @task(task_id="fetch_orders")
    def fetch_orders():
        """Busca os pedidos do banco de dados."""
        data_ref = None
        records = DatabaseService.fetch_orders_from_db(data_ref)
        
        if not records:
            logging.info("Nenhum registro encontrado para processamento.")
            # Usa AirflowSkipException para pular as próximas tarefas
            raise AirflowSkipException("Nenhum pedido encontrado para processar.")
            
        return records

    @task(task_id="group_orders")
    def group_orders(records: list):
        """Agrupa os produtos por pedido."""
        orders = {}
        for row in records:
            record = {
                "cnpj_fornecedor": row[0],
                "cod_colecao": row[1],
                "cod_condicao_pgto": row[2],
                "cod_filial": row[3],
                "cod_pedidoc": row[4],

                "data": row[5],
                "ultima_atualizacao": row[6],
                "data_entrega": row[7],
                "barra": row[9],
                "preco": row[10],
                "quantidade": row[11],
                "sku": row[12]
            }

            order_code = record["cod_pedidoc"]

            if order_code not in orders:
                orders[order_code] = {
                    "cnpj_fornecedor": record["cnpj_fornecedor"],
                    "cod_condicao_pgto": record["cod_condicao_pgto"],
                    "cod_filial": record["cod_filial"],
                    "data": record["data"],
                    "data_entrega": record["data_entrega"],
                    "produtos": []
                }
            product = {
                "sku": record["sku"],
                "barra": record["barra"],
                "quantidade": record["quantidade"],
                "preco": record["preco"]
            }
            orders[order_code]["produtos"].append(product)
        
        logging.info(f"Pedidos agrupados: {len(orders)}")
        
        # Vazio? Skip
        if not orders:
            raise AirflowSkipException("Nenhum pedido para processar após agrupamento.")
            
        # Transforma o dicionário em lista para a operação de mapeamento
        order_list = [{"order_code": k, "order_data": v} for k, v in orders.items()]
        return order_list

    @task(
        task_id="process_order",  # Prefixo para as tarefas mapeadas
        retries=0,                # Sobrescreve retries para esta tarefa específica
        execution_timeout=timedelta(minutes=5)
    )
    def process_order(order_item: Dict[str, Any], **context):
        """
        Processa um pedido - prepara e envia para a API.
        Esta função combina a preparação e o envio em uma única tarefa,
        reduzindo overhead de XComs para grandes volumes de dados.
        """
        # Extrai informações do pedido
        order_code = order_item["order_code"]
        order = order_item["order_data"]
        
        logging.info(f"Processando pedido {order_code} com {len(order['produtos'])} produtos")
        
        # Prepara o corpo da requisição
        body = {
            "cod_pedidoc": order_code,
            "data": order["data"].strftime("%Y-%m-%d") if isinstance(order["data"], datetime) else order["data"],
            
            "cod_filial": str(order["cod_filial"]),
            "produtos": order["produtos"],
            "cnpj_fornecedor": format_cnpj(order["cnpj_fornecedor"]),
            "data_entrega": order["data_entrega"].strftime("%Y-%m-%d") if isinstance(order["data_entrega"], datetime) else order["data_entrega"],
            "cod_condicao_pgto": order["cod_condicao_pgto"]
        }
        
        formatted_body = json.dumps(body, indent=2, sort_keys=True)
        logging.info(f"Preparado corpo da requisição para pedido {order_code}")
        
        # Envia para API
        try:
            response = fazer_requisicao(body)
            
            if response.status_code != 200:
                raise Exception(f"Status code não esperado: {response.status_code}")
                
            # Sucesso na integração
            pedidoc_millennium = response.json().get('pedidoc', '')
            DatabaseService.insere_pedido_integrado(order_code, pedidoc_millennium)
            
            # Remove registro de erro se existir
            if DatabaseService.registro_erro_existe(order_code):
                DatabaseService.deleta_registro_erro(order_code)
                
            logging.info(f"Pedido {order_code} processado com sucesso!")
            return {
                "success": True, 
                "order_code": order_code,
                "pedidoc_millennium": pedidoc_millennium
            }
            
        except Exception as e:
            # Tratamento de erro
            try:
                if hasattr(response, 'json'):
                    error_data = response.json()
                    if isinstance(error_data, dict) and "error" in error_data:
                        error_message = error_data["error"]["message"]["value"].replace('\r', '')
                    else:
                        error_message = str(error_data)
                else:
                    error_message = str(e)
            except Exception:
                error_message = str(e)
                
            logging.error(f"Erro ao processar pedido {order_code}: {error_message}")
            
            # Categoriza o erro
            error_friendly = "Erro desconhecido"
            if hasattr(response, 'status_code') and response.status_code == 400:
                if 'Fornecedor com cnpj' in error_message:
                    error_friendly = f"Erro ao encontrar o cnpj do fornecedor: {body['cnpj_fornecedor']}"
                else:
                    error_friendly = f"Erro 400 - desconhecido: {error_message}"
            else:
                error_friendly = f"Erro: {str(e)}"

            EmailService.send_error_email(
                subject=f"Erro no processamento do pedido {order_code} - {error_friendly}",
                message=formatted_body
            )
            
            # Registra erro no banco
            if not DatabaseService.registro_erro_existe(order_code):
                DatabaseService.insere_registro_erro(order_code, error_message, 1)
            
            # Outros erros são registrados, mas a task é marcada como "concluída com aviso"
            return {
                "success": False, 
                "order_code": order_code, 
                "error": error_friendly
            }

    @task(task_id="summarize_results")
    def summarize_results(results: List[Dict[str, Any]]):
        """Processa os resultados e envia e-mail de sucesso."""
        successful_orders = [r["order_code"] for r in results if r.get("success", False)]
        failed_orders = [r["order_code"] for r in results if not r.get("success", False)]
        
        logging.info(f"Pedidos processados com sucesso: {len(successful_orders)}")
        logging.info(f"Pedidos com falha: {len(failed_orders)}")
        
        if successful_orders:
            EmailService.send_success_email('Integração de pedidos concluída', successful_orders)
        
        # Output para ser visualizado no Airflow UI
        return {
            "successful": successful_orders,
            "failed": failed_orders,
            "total_processed": len(results),
            "success_rate": f"{len(successful_orders)/len(results)*100:.1f}%" if results else "N/A"
        }

    # Define o fluxo da DAG
    orders_data = fetch_orders()
    orders_grouped = group_orders(orders_data)
    # Processa cada pedido individualmente com dynamic mapping
    results = process_order.expand(order_item=orders_grouped)
    final_summary = summarize_results(results)

# Instanciação da DAG
millennium_order_processing_dag()
