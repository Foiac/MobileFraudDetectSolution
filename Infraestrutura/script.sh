# Variables

SUBSCRIPTION_ID=""
RESOURCE_GROUP="rg-fraud"
REGION="westus"
EVENTHUB_NAMESPACE_NAME="fraud-eh-namespace"
SKU_TYPE="Basic"
EVENTHUB_NAME="fraud-detect"
POLICIE_NAME="listener-eh-policie"
KV_NAME="kv-fraud" # Change this name to replicate this solution
ASSIGNE_ID=""
STORAGE_ACCOUNT_NAME="stacmfraud" # Change this name to replicate this solution
CONTAINER_NAME="cont-fraud" 
WORKSPACE_NAME="wks-dtbs-fraud"

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
#az keyvault create --name $KV_NAME --resource-group $RESOURCE_GROUP --location $REGION
az keyvault recover --name $KV_NAME
az role assignment create --assignee $ASSIGNE_ID --role "Key Vault Administrator" --scope /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.KeyVault/vaults/$KV_NAME

# Create Storage Account on Azure and create container on ADLS
echo "Create Storage Account on Azure and create container on ADLS"
az storage account create --name $STORAGE_ACCOUNT_NAME --resource-group $RESOURCE_GROUP --location $REGION --sku Standard_LRS --kind StorageV2 --enable-hierarchical-namespace true --default-action Allow
az storage container create --name $CONTAINER_NAME --account-name $STORAGE_ACCOUNT_NAME --auth-mode login

# Create Paths to Delta Tables
echo "Create bronze, silver and gold paths to delta tables"
az storage fs directory create --account-name $STORAGE_ACCOUNT_NAME --file-system $CONTAINER_NAME --name bronze
az storage fs directory create --account-name $STORAGE_ACCOUNT_NAME --file-system $CONTAINER_NAME --name silver
az storage fs directory create --account-name $STORAGE_ACCOUNT_NAME --file-system $CONTAINER_NAME --name gold

# Create databricks workspace
echo "Create databricks workspace"
az databricks workspace create --resource-group $RESOURCE_GROUP --name $WORKSPACE_NAME --location $REGION --sku premium 