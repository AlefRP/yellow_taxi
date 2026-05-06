# yellow_taxi (dbt)

Projeto dbt que modela as viagens de táxi amarelo de NYC.

## Camadas

- **`models/src/`** — sources e modelos `src_*` (ephemeral, espelham a tabela raw).
- **`models/staging/`** — `stg_*` com renomeações, tipagem e chaves substitutas (view).
- **`models/marts/`** — fatos/dimensões prontos para consumo (table).

## Convenções

- Modelos em `snake_case`.
- Documentação e testes em `_<camada>.yml` (um YAML por camada).
- Particionamento por data (`pickup_date`) e clustering por `vendor_id` no staging.

## Comandos

```bash
dbt deps        # instala packages
dbt build       # roda models, snapshots, seeds e testes em ordem
dbt test        # apenas testes
dbt docs generate && dbt docs serve
```

## Dependências

Ver `packages.yml` — `dbt_utils` e `dbt_expectations`.
