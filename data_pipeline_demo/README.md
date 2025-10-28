# Demo: Testes em Pipeline de Dados (pytest + pandas)

## Estrutura
```
data_pipeline_demo/
├─ data/
│  ├─ raw/
│  │  └─ customers.csv
│  └─ processed/
├─ src/
│  └─ pipeline.py
├─ tests/
│  └─ test_pipeline.py
└─ requirements.txt
```

## Como rodar
1) Crie o ambiente e instale dependências
```bash
python -m venv .venv
# Linux/macOS
source .venv/bin/activate
# Windows (PowerShell)
.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

2) Rode os testes
```bash
pytest -q
```

3) Execute o pipeline end-to-end
```bash
python -m src.pipeline
```

Os artefatos de saída serão gravados em `data/processed/`.

## O que os testes cobrem?
- **Contrato do dado bruto** (colunas e tamanho esperado)
- **Transformação**: tipos, domínios válidos (UFs), unicidade e regras (spending ≥ 0)
- **Agregação**: consistência entre média, total e contagem
- **End-to-end (fumaça)**: arquivos gerados e não vazios

## Observações
- Os dados de exemplo incluem casos problemáticos (nulos, negativo, UF inválida, duplicidade) para ilustrar os testes.
- Ajuste `ALLOWED_STATES` e regras conforme o seu domínio.
