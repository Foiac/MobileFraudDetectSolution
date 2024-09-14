# MobileFraudDetectSolution - Identificando Tentativas de Fraudes em Aplicativos Mobile
## Case Data Master 2024

#### I. Objetivo do Case

Este trabalho tem como objetivo implementar um case prático de coleta de logs de acesso a um aplicativo de transações financeiras, onde a arquitetura contém ingestão e transformação e visualização de dados em um data lake na cloud (Azure Data Lake Gen2).

#### II. Arquitetura de Solução e Arquitetura Técnica

#### III. Explicação sobre o case desenvolvido

#### IV. Melhorias e Considerações finais

#### Provisionando a infraestrutura
1 - Abrir terminal Bash da Azure e Clonar o repositório executando o comando a seguir

```bash
git clone https://github.com/Foiac/MobileFraudDetectSolution.git
```

2 - Dar permissão e executar o arquivo script.sh no diretório MobileFraudDetectSolution/Infraestrutura

```bash
# Give permission to execute .sh and run script.sh to create resource group and resources
chmod +x MobileFraudDetectSolution/Infraestrutura/script.sh
MobileFraudDetectSolution/Infraestrutura/script.sh
```

```bash
# Give permission to execute .sh # Run dbscript.sh to create databricks databricks cluster
chmod +x MobileFraudDetectSolution/Infraestrutura/databricks/cluster/dbscript.sh
MobileFraudDetectSolution/Infraestrutura/databricks/cluster/dbscript.sh
```
