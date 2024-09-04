# Variables

subsId="d194704a-1cc1-45e7-8cf5-2b4f7bb9e3c3"
rgName="rgfraud"
region="eastus"
namespaceName="fraudEhNameSpace"
skuType="Basic"
eventhubName="frauddetect"
policieName="listenerEhPolicie"
kvName="kvfraud"
assigneeId="kaio_cfs_hotmail.com#EXT#@kaiocfshotmail.onmicrosoft.com"
secretName="ehcssecret"
storageAcName="stacfraud"
containerName="contfraud"

# Create Resouce Group 
az group create --name $rgName --location $region

# Create Eventhub Namespace
az eventhubs namespace create --name $namespaceName --resource-group $rgName --sku $skuType -l $region

# Create Tópico
az eventhubs eventhub create --resource-group $rgName --namespace-name $namespaceName --name $eventhubName --cleanup-policy Delete --partition-count 1

# Create Acces Policies to Eventhub
az eventhubs namespace authorization-rule create --resource-group $rgName --namespace-name $namespaceName --name $policieName --rights Listen

# Create Azure Key Vault
az keyvault create --name $kvName --resource-group $rgName --location $region
az role assignment create --assignee $assigneeId --role "Key Vault Administrator" --scope /subscriptions/$subsId/resourceGroups/$rgName/providers/Microsoft.KeyVault/vaults/$kvName

# # Storage Connection String on Key Vault
connectionString=$(az eventhubs namespace authorization-rule keys list --resource-group $rgName --namespace-name $namespaceName --name $policieName --query primaryConnectionString --output tsv)
az keyvault secret set --vault-name $kvName --name $secretName --value $connectionString

# Create Storage Account on Azure and create container on ADLS
az storage account create --name $storageAcName --resource-group $rgName --location $region --sku Standard_LRS --kind StorageV2 --hierarchical-namespace true
az storage container create --name $containerName --account-name $storageAcName --auth-mode login

# # Create Paths to Delta Tables
az storage fs directory create --account-name $storageAcName --file-system $containerName --name Bronze
az storage fs directory create --account-name $storageAcName --file-system $containerName --name Silver
az storage fs directory create --account-name $storageAcName --file-system $containerName --name Gold