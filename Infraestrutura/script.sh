# Variables

SUBSCRIPTION_ID="d194704a-1cc1-45e7-8cf5-2b4f7bb9e3c3"
RESOURCE_GROUP="rgFraud"
REGION="eastus"
EVENTHUB_NAMESPACE_NAME="fraudEhNameSpace"
SKU_TYPE="Basic"
EVENTHUB_NAME="frauddetect"
POLICIE_NAME="listenerEhPolicie"
KV_NAME="kvfraud"
ASSIGNE_ID="kaio_cfs_hotmail.com#EXT#@kaiocfshotmail.onmicrosoft.com"
SECRET_NAME="ehcssecret"
STORAGE_ACCOUNT_NAME="stacfraud"
CONTAINER_NAME="contfraud"
WORKSPACE_NAME="wksdtbsfraud"

# Create Resouce Group 
echo "Create Resorce Group"
az group create --name $RESOURCE_GROUP --location $REGION

# Create Eventhub Namespace
echo "Create Eventhub Namespace: " $EVENTHUB_NAMESPACE_NAME
az eventhubs namespace create --name $EVENTHUB_NAMESPACE_NAME --resource-group $RESOURCE_GROUP --sku $SKU_TYPE -l $REGION

# Create Topic
echo "Create Topic: " $EVENTHUB_NAME
az eventhubs eventhub create --resource-group $RESOURCE_GROUP --namespace-name $EVENTHUB_NAMESPACE_NAME --name $EVENTHUB_NAME --cleanup-policy Delete --partition-count 1

# Create Access Policies to Eventhub
echo "Create Listen Access Policies to Eventhub"
az eventhubs namespace authorization-rule create --resource-group $RESOURCE_GROUP --namespace-name $EVENTHUB_NAMESPACE_NAME --name $POLICIE_NAME --rights Listen

# Create Azure Key Vault
echo "Create Azure Key Vault: " $KV_NAME
az keyvault create --name $KV_NAME --resource-group $RESOURCE_GROUP --location $REGION
az role assignment create --assignee $ASSIGNE_ID --role "Key Vault Administrator" --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.KeyVault/vaults/$KV_NAME

# Storage Connection String on Key Vault
echo "Storage Connection String on Key Vault"
CONNECTION_STRING=$(az eventhubs namespace authorization-rule keys list --resource-group $RESOURCE_GROUP --namespace-name $EVENTHUB_NAMESPACE_NAME --name $POLICIE_NAME --query primaryConnectionString --output tsv)
az keyvault secret set --vault-name $KV_NAME --name $SECRET_NAME --value $CONNECTION_STRING

# Create Storage Account on Azure and create container on ADLS
echo "Storage Connection String on Key Vault"
az storage account create --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --location $REGION --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true
az storage container create --name $CONTAINER_NAME --account-name $STORAGE_ACCOUNT_NAME --auth-mode login

# Create Paths to Delta Tables
echo "Create bronze, silver and gold paths to delta tables"
az storage fs directory create --account-name $STORAGE_ACCOUNT_NAME --file-system $CONTAINER_NAME --name bronze
az storage fs directory create --account-name $STORAGE_ACCOUNT_NAME --file-system $CONTAINER_NAME --name silver
az storage fs directory create --account-name $STORAGE_ACCOUNT_NAME --file-system $CONTAINER_NAME --name gold

# Create databricks workspace
echo "Create databricks workspace"
az config set extension.dynamic_install_allow_preview=true
az databricks workspace create --resource-group $RESOURCE_GROUP --name $WORKSPACE_NAME --location $REGION --sku standard

# Install databricks CLI
echo "Install databricks CLI"
pip install databricks-cli