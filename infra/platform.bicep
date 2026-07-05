// Platform layer: everything that outlives a single app deployment - registry,
// managed identity, Event Hubs (Kafka-compatible), Cosmos DB (Mongo API),
// Key Vault, and the Container Apps environment (incl. file share for Spark
// checkpoints). Deploy this once per environment, then deploy apps.bicep
// against its outputs for each app rollout.
targetScope = 'resourceGroup'

@description('Short name used as a prefix for all resources.')
param namePrefix string = 'frauddet'

param location string = resourceGroup().location

@description('Event Hubs throughput unit capacity.')
@minValue(1)
@maxValue(20)
param eventHubCapacity int = 1

@description('Cosmos DB Mongo API database name; matches MONGO_DB in .env.example.')
param cosmosDatabaseName string = 'fraud_platform'

@description('Event Hub (Kafka topic) name; matches KAFKA_TRANSACTIONS_TOPIC in .env.example.')
param eventHubName string = 'transactions'

@description('Object ID of the CI service principal (AZURE_CLIENT_ID app) so it can push images to ACR. Leave empty to grant AcrPush manually instead.')
param ciPrincipalId string = ''

var uniqueSuffix = uniqueString(resourceGroup().id)
var acrName = toLower('${namePrefix}acr${uniqueSuffix}')
var storageAccountName = toLower(take('${namePrefix}st${uniqueSuffix}', 24))
var eventHubsNamespaceName = toLower('${namePrefix}-ehns-${uniqueSuffix}')
var cosmosAccountName = toLower('${namePrefix}-cosmos-${uniqueSuffix}')
var keyVaultName = toLower(take('${namePrefix}-kv-${uniqueSuffix}', 24))
var logAnalyticsName = '${namePrefix}-logs'
var containerAppsEnvName = '${namePrefix}-env'
var identityName = '${namePrefix}-identity'
var checkpointShareName = 'spark-checkpoints'
var envStorageName = 'spark-checkpoints'

// --- Identity used by every Container App for ACR pull + Key Vault secret refs ---
resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: identityName
  location: location
}

// --- Container registry, app images get pushed here by CI ---
resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  sku: { name: 'Basic' }
  properties: {
    adminUserEnabled: false
  }
}

resource acrPullRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, identity.id, 'AcrPull')
  scope: acr
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
  }
}

// Lets the CI service principal `docker push` to this registry - AcrPull above
// is only for the Container Apps' runtime identity to pull, which is a
// separate concern.
resource acrPushRoleForCI 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(ciPrincipalId)) {
  name: guid(acr.id, ciPrincipalId, 'AcrPush')
  scope: acr
  properties: {
    principalId: ciPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '8311e382-0749-4cb8-b61a-304f252e45ec')
  }
}

// --- Log Analytics + Container Apps environment ---
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: logAnalyticsName
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// Storage account backing an Azure Files share for the Spark job's streaming
// checkpoint directory - without durable checkpoint storage a container
// restart would lose Kafka offsets and replay/duplicate data.
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-01-01' = {
  parent: storageAccount
  name: 'default'
}

resource checkpointShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-01-01' = {
  parent: fileService
  name: checkpointShareName
  properties: {
    shareQuota: 20
  }
}

resource containerAppsEnv 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: containerAppsEnvName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalytics.properties.customerId
        sharedKey: logAnalytics.listKeys().primarySharedKey
      }
    }
  }
}

resource envStorage 'Microsoft.App/managedEnvironments/storages@2024-03-01' = {
  parent: containerAppsEnv
  name: envStorageName
  properties: {
    azureFile: {
      accountName: storageAccount.name
      accountKey: storageAccount.listKeys().keys[0].value
      shareName: checkpointShareName
      accessMode: 'ReadWrite'
    }
  }
}

// --- Event Hubs, standing in for Kafka via its Kafka-compatible endpoint ---
resource eventHubsNamespace 'Microsoft.EventHub/namespaces@2023-01-01-preview' = {
  name: eventHubsNamespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: eventHubCapacity
  }
  properties: {
    kafkaEnabled: true
  }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2023-01-01-preview' = {
  parent: eventHubsNamespace
  name: eventHubName
  properties: {
    partitionCount: 4
    messageRetentionInDays: 1
  }
}

resource eventHubAuthRule 'Microsoft.EventHub/namespaces/AuthorizationRules@2023-01-01-preview' = {
  parent: eventHubsNamespace
  name: 'fraud-platform-app'
  properties: {
    rights: ['Send', 'Listen']
  }
}

// --- Cosmos DB, standing in for MongoDB via its Mongo API ---
resource cosmosAccount 'Microsoft.DocumentDB/databaseAccounts@2023-11-15' = {
  name: cosmosAccountName
  location: location
  kind: 'MongoDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    locations: [
      { locationName: location, failoverPriority: 0 }
    ]
    capabilities: [
      { name: 'EnableMongo' }
      { name: 'EnableServerless' }
    ]
    apiProperties: {
      serverVersion: '7.0'
    }
  }
}

resource cosmosDatabase 'Microsoft.DocumentDB/databaseAccounts/mongodbDatabases@2023-11-15' = {
  parent: cosmosAccount
  name: cosmosDatabaseName
  properties: {
    resource: { id: cosmosDatabaseName }
  }
}

// --- Key Vault holding the two connection secrets Container Apps reference directly ---
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: subscription().tenantId
    sku: { family: 'A', name: 'standard' }
    enableRbacAuthorization: true
  }
}

resource keyVaultSecretsUserRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, identity.id, 'KeyVaultSecretsUser')
  scope: keyVault
  properties: {
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
  }
}

resource kafkaSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'kafka-sasl-password'
  properties: {
    value: eventHubAuthRule.listKeys().primaryConnectionString
  }
}

resource mongoSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'mongo-uri'
  properties: {
    value: cosmosAccount.listConnectionStrings().connectionStrings[0].connectionString
  }
}

output acrName string = acr.name
output acrLoginServer string = acr.properties.loginServer
output identityId string = identity.id
output identityClientId string = identity.properties.clientId
output containerAppsEnvironmentId string = containerAppsEnv.id
output envStorageName string = envStorage.name
output keyVaultUri string = keyVault.properties.vaultUri
output kafkaBootstrapServers string = '${eventHubsNamespaceName}.servicebus.windows.net:9093'
output eventHubName string = eventHubName
output cosmosDatabaseName string = cosmosDatabaseName
