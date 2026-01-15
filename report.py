import json
from datetime import datetime
from collections import Counter
import os

class LogReportGenerator:
    def __init__(self):
        self.execution_log = []
        self.error_log = []
        self.checkpoint_data = {}
        self.sku_ids = []
        
    def read_execution_log(self, filepath='execution_log.txt'):
        """Lê o log de execução"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.execution_log = f.readlines()
            print(f"✓ {filepath} lido com sucesso")
        except FileNotFoundError:
            print(f"⚠ Arquivo {filepath} não encontrado")
            
    def read_error_log(self, filepath='error_log.txt'):
        """Lê o log de erros"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.error_log = f.readlines()
            print(f"✓ {filepath} lido com sucesso")
        except FileNotFoundError:
            print(f"⚠ Arquivo {filepath} não encontrado")
            
    def read_checkpoint(self, filepath='checkpoint.json'):
        """Lê o arquivo de checkpoint"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.checkpoint_data = json.load(f)
            print(f"✓ {filepath} lido com sucesso")
        except FileNotFoundError:
            print(f"⚠ Arquivo {filepath} não encontrado")
        except json.JSONDecodeError:
            print(f"⚠ Erro ao decodificar {filepath}")
            
    def read_sku_ids(self, filepath='sku_ids.txt'):
        """Lê a lista de SKU IDs"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                self.sku_ids = [line.strip() for line in f if line.strip()]
            print(f"✓ {filepath} lido com sucesso")
        except FileNotFoundError:
            print(f"⚠ Arquivo {filepath} não encontrado")
    
    def analyze_execution_log(self):
        """Analisa o log de execução"""
        stats = {
            'total_linhas': len(self.execution_log),
            'processamentos_sucesso': 0,
            'processamentos_falha': 0,
            'warnings': 0,
            'info': 0,
            'updates_ok': 0,
            'checkpoint_saves': 0
        }
        
        for line in self.execution_log:
            line_lower = line.lower()
            
            # Conta updates com [OK] ou Image updated como sucesso
            if '[ok]' in line_lower or 'image updated:' in line_lower:
                stats['processamentos_sucesso'] += 1
                stats['updates_ok'] += 1
            # Conta checkpoint saves
            elif 'checkpoint saved' in line_lower:
                stats['checkpoint_saves'] += 1
                stats['processamentos_sucesso'] += 1
            # Outros sucessos
            elif 'success' in line_lower or 'sucesso' in line_lower:
                stats['processamentos_sucesso'] += 1
            # Erros e falhas
            elif 'error' in line_lower or 'erro' in line_lower or 'fail' in line_lower or '[error]' in line_lower:
                stats['processamentos_falha'] += 1
            # Warnings
            elif 'warning' in line_lower or 'aviso' in line_lower or '[warning]' in line_lower:
                stats['warnings'] += 1
            # Info
            elif '[info]' in line_lower:
                stats['info'] += 1
                
        return stats
    
    def analyze_errors(self):
        """Analisa os erros do log"""
        if not self.error_log:
            return {'total_erros': 0, 'tipos_erros': {}}
            
        error_types = []
        for line in self.error_log:
            if 'timeout' in line.lower():
                error_types.append('Timeout')
            elif 'connection' in line.lower() or 'conexão' in line.lower():
                error_types.append('Erro de Conexão')
            elif 'not found' in line.lower() or '404' in line:
                error_types.append('Não Encontrado')
            elif 'permission' in line.lower() or 'permissão' in line.lower():
                error_types.append('Erro de Permissão')
            else:
                error_types.append('Outros')
                
        error_counter = Counter(error_types)
        
        return {
            'total_erros': len(self.error_log),
            'tipos_erros': dict(error_counter)
        }
    
    def generate_html_chart(self, exec_stats, error_stats):
        """Gera gráficos em HTML usando Chart.js"""
        
        # Calcula taxa de sucesso
        total_proc = exec_stats['processamentos_sucesso'] + exec_stats['processamentos_falha']
        taxa_sucesso = (exec_stats['processamentos_sucesso'] / total_proc * 100) if total_proc > 0 else 0
        taxa_falha = 100 - taxa_sucesso
        
        # Prepara dados de erros
        error_labels = list(error_stats['tipos_erros'].keys()) if error_stats['tipos_erros'] else []
        error_values = list(error_stats['tipos_erros'].values()) if error_stats['tipos_erros'] else []
        
        html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Relatório de Processamento - Dashboard</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        .header {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 30px;
            text-align: center;
        }}
        h1 {{
            color: #333;
            margin-bottom: 10px;
        }}
        .timestamp {{
            color: #666;
            font-size: 14px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            text-align: center;
            transition: transform 0.3s;
        }}
        .stat-card:hover {{
            transform: translateY(-5px);
        }}
        .stat-number {{
            font-size: 36px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .stat-label {{
            color: #666;
            font-size: 14px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        .success {{ color: #10b981; }}
        .error {{ color: #ef4444; }}
        .warning {{ color: #f59e0b; }}
        .info {{ color: #3b82f6; }}
        .chart-container {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 30px;
        }}
        .chart-wrapper {{
            position: relative;
            height: 400px;
        }}
        h2 {{
            color: #333;
            margin-bottom: 20px;
            text-align: center;
        }}
        .success-rate {{
            font-size: 48px;
            font-weight: bold;
            text-align: center;
            margin: 20px 0;
        }}
        .rate-good {{ color: #10b981; }}
        .rate-medium {{ color: #f59e0b; }}
        .rate-bad {{ color: #ef4444; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Dashboard de Processamento</h1>
            <p class="timestamp">Gerado em: {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total de SKUs</div>
                <div class="stat-number info">{len(self.sku_ids)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Sucessos</div>
                <div class="stat-number success">{exec_stats['processamentos_sucesso']}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Falhas</div>
                <div class="stat-number error">{exec_stats['processamentos_falha']}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Avisos</div>
                <div class="stat-number warning">{exec_stats['warnings']}</div>
            </div>
        </div>

        <div class="chart-container">
            <h2>Taxa de Sucesso do Processamento</h2>
            <div class="success-rate {'rate-good' if taxa_sucesso >= 80 else 'rate-medium' if taxa_sucesso >= 50 else 'rate-bad'}">
                {taxa_sucesso:.1f}%
            </div>
            <div class="chart-wrapper">
                <canvas id="successChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2>Distribuição de Erros por Tipo</h2>
            <div class="chart-wrapper">
                <canvas id="errorChart"></canvas>
            </div>
        </div>

        <div class="chart-container">
            <h2>Resumo Geral do Processamento</h2>
            <div class="chart-wrapper">
                <canvas id="summaryChart"></canvas>
            </div>
        </div>
    </div>

    <script>
        // Gráfico de Taxa de Sucesso (Doughnut)
        const successCtx = document.getElementById('successChart').getContext('2d');
        new Chart(successCtx, {{
            type: 'doughnut',
            data: {{
                labels: ['Sucesso', 'Falha'],
                datasets: [{{
                    data: [{taxa_sucesso:.2f}, {taxa_falha:.2f}],
                    backgroundColor: ['#10b981', '#ef4444'],
                    borderWidth: 0
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        position: 'bottom',
                        labels: {{
                            font: {{
                                size: 14
                            }},
                            padding: 20
                        }}
                    }}
                }}
            }}
        }});

        // Gráfico de Erros por Tipo
        const errorCtx = document.getElementById('errorChart').getContext('2d');
        new Chart(errorCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(error_labels)},
                datasets: [{{
                    label: 'Quantidade de Erros',
                    data: {json.dumps(error_values)},
                    backgroundColor: '#ef4444',
                    borderRadius: 8
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        display: false
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            stepSize: 1
                        }}
                    }}
                }}
            }}
        }});

        // Gráfico de Resumo Geral
        const summaryCtx = document.getElementById('summaryChart').getContext('2d');
        new Chart(summaryCtx, {{
            type: 'bar',
            data: {{
                labels: ['Sucessos', 'Falhas', 'Avisos', 'Informações'],
                datasets: [{{
                    label: 'Quantidade',
                    data: [
                        {exec_stats['processamentos_sucesso']},
                        {exec_stats['processamentos_falha']},
                        {exec_stats['warnings']},
                        {exec_stats['info']}
                    ],
                    backgroundColor: ['#10b981', '#ef4444', '#f59e0b', '#3b82f6'],
                    borderRadius: 8
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{
                        display: false
                    }}
                }},
                scales: {{
                    y: {{
                        beginAtZero: true
                    }}
                }}
            }}
        }});
    </script>
</body>
</html>"""
        return html
    
    def generate_report(self, output_file='relatorio_gerencia.txt'):
        """Gera o relatório completo em texto"""
        exec_stats = self.analyze_execution_log()
        error_stats = self.analyze_errors()
        
        report = []
        report.append("=" * 70)
        report.append("RELATÓRIO EXECUTIVO - PROCESSAMENTO DE LOGS")
        report.append("=" * 70)
        report.append(f"\nData de Geração: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        report.append("\n" + "-" * 70)
        
        # Resumo Geral
        report.append("\n📊 RESUMO GERAL")
        report.append("-" * 70)
        report.append(f"Total de SKUs Processados: {len(self.sku_ids)}")
        report.append(f"Total de Registros de Execução: {exec_stats['total_linhas']}")
        report.append(f"Processamentos com Sucesso: {exec_stats['processamentos_sucesso']}")
        report.append(f"  • Imagens Atualizadas [OK]: {exec_stats['updates_ok']}")
        report.append(f"  • Checkpoints Salvos: {exec_stats['checkpoint_saves']}")
        report.append(f"Processamentos com Falha: {exec_stats['processamentos_falha']}")
        report.append(f"Avisos (Warnings): {exec_stats['warnings']}")
        report.append(f"Mensagens Informativas: {exec_stats['info']}")
        
        # Taxa de Sucesso
        if exec_stats['processamentos_sucesso'] + exec_stats['processamentos_falha'] > 0:
            total_proc = exec_stats['processamentos_sucesso'] + exec_stats['processamentos_falha']
            taxa_sucesso = (exec_stats['processamentos_sucesso'] / total_proc) * 100
            report.append(f"\n✅ Taxa de Sucesso: {taxa_sucesso:.2f}%")
            report.append(f"❌ Taxa de Falha: {100 - taxa_sucesso:.2f}%")
        
        # Análise de Erros
        report.append("\n" + "-" * 70)
        report.append("\n❌ ANÁLISE DE ERROS")
        report.append("-" * 70)
        report.append(f"Total de Erros Registrados: {error_stats['total_erros']}")
        
        if error_stats['tipos_erros']:
            report.append("\nDistribuição por Tipo de Erro:")
            for tipo, qtd in error_stats['tipos_erros'].items():
                porcentagem = (qtd / error_stats['total_erros']) * 100
                report.append(f"  • {tipo}: {qtd} ({porcentagem:.1f}%)")
        
        # Informações do Checkpoint
        if self.checkpoint_data:
            report.append("\n" + "-" * 70)
            report.append("\n💾 INFORMAÇÕES DE CHECKPOINT")
            report.append("-" * 70)
            for key, value in self.checkpoint_data.items():
                report.append(f"{key}: {value}")
        
        # Recomendações
        report.append("\n" + "-" * 70)
        report.append("\n💡 RECOMENDAÇÕES")
        report.append("-" * 70)
        
        if exec_stats['processamentos_falha'] > exec_stats['processamentos_sucesso']:
            report.append("⚠ CRÍTICO: Taxa de falha superior a 50%. Investigação urgente necessária.")
        
        if error_stats['tipos_erros'].get('Timeout', 0) > 5:
            report.append("⚠ Múltiplos timeouts detectados. Considere ajustar configurações de rede.")
        
        if error_stats['tipos_erros'].get('Erro de Conexão', 0) > 5:
            report.append("⚠ Problemas de conexão frequentes. Verificar estabilidade da rede.")
        
        if not error_stats['total_erros'] and exec_stats['processamentos_sucesso'] > 0:
            report.append("✅ Execução perfeita! Nenhum erro detectado.")
        
        report.append("\n" + "=" * 70)
        report.append("Fim do Relatório")
        report.append("=" * 70)
        
        # Salva o relatório
        report_text = "\n".join(report)
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        return report_text

def main():
    print("🚀 Iniciando geração de relatório...\n")
    
    generator = LogReportGenerator()
    
    # Lê todos os arquivos
    generator.read_execution_log()
    generator.read_error_log()
    generator.read_checkpoint()
    generator.read_sku_ids()
    
    print("\n📝 Gerando relatório em texto...")
    exec_stats = generator.analyze_execution_log()
    error_stats = generator.analyze_errors()
    
    # Gera o relatório em texto
    report = generator.generate_report()
    print(report)
    print(f"\n✅ Relatório em texto salvo em: relatorio_gerencia.txt")
    
    # Gera o relatório HTML com gráficos
    print("\n📊 Gerando dashboard com gráficos...")
    html_report = generator.generate_html_chart(exec_stats, error_stats)
    
    with open('dashboard_relatorio.html', 'w', encoding='utf-8') as f:
        f.write(html_report)
    
    print("✅ Dashboard HTML salvo em: dashboard_relatorio.html")
    print("\n🌐 Abra o arquivo 'dashboard_relatorio.html' no navegador para visualizar os gráficos!")

if __name__ == "__main__":
    main()