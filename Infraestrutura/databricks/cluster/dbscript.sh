SCOPE_NAME="secret-fraud"

echo "Para continuar digite seu host e token de acesso do databricks"
databricks configure --token

databricks clusters create --json-file MobileFraudDetectSolution/Infraestrutura/databricks/cluster/cluster_config.json
databricks secrets create-scope --scope $SCOPE_NAME
databricks secrets put --scope $SCOPE_NAME --key SECRET_CONNECTION_STRING
databricks secrets put --scope $SCOPE_NAME --key SECRET_AZURE_KEY
