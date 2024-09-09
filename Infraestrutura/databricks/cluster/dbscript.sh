echo "Para continuar digite seu host e token de acesso do databricks"
databricks configure --token

databricks clusters create --json-file cluster_config.json
