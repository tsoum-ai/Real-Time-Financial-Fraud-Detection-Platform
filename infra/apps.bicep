// App layer: the three Container Apps (api, spark, producer), deployed
// against the outputs of platform.bicep. Re-run this on every image push -
// it's the fast, frequent deployment; platform.bicep is the slow, rare one.
targetScope = 'resourceGroup'

param namePrefix string = 'frauddet'
param location string = resourceGroup().location

@description('Image tag to deploy for all three apps, e.g. the git SHA CI just pushed.')
param imageTag string = 'latest'

param acrLoginServer string
param containerAppsEnvironmentId string
param identityId string
param envStorageName string
param keyVaultUri string
param kafkaBootstrapServers string
param eventHubName string
param cosmosDatabaseName string

@description('Name of the Key Vault secret holding the Cosmos DB Mongo API connection string.')
param mongoSecretName string = 'mongo-uri'

@description('Name of the Key Vault secret holding the Event Hubs SASL connection string.')
param kafkaSecretName string = 'kafka-sasl-password'

// Fraud rule thresholds - same defaults as .env.example, override per environment if needed.
param fraudAmountThreshold string = '10000'
param fraudRapidTxnWindowSeconds string = '60'
param fraudRapidTxnCount string = '5'
param fraudHighRiskCountries string = 'NG,RU,KP,IR,SY'
param genIntervalSeconds string = '1.0'
param genFraudRatio string = '0.15'

var identityRef = {
  '${identityId}': {}
}

var sharedSecrets = [
  {
    name: kafkaSecretName
    keyVaultUrl: '${keyVaultUri}secrets/${kafkaSecretName}'
    identity: identityId
  }
  {
    name: mongoSecretName
    keyVaultUrl: '${keyVaultUri}secrets/${mongoSecretName}'
    identity: identityId
  }
]

var sharedRegistries = [
  {
    server: acrLoginServer
    identity: identityId
  }
]

var kafkaEnvCommon = [
  { name: 'KAFKA_BOOTSTRAP_SERVERS', value: kafkaBootstrapServers }
  { name: 'KAFKA_TRANSACTIONS_TOPIC', value: eventHubName }
  { name: 'KAFKA_SECURITY_PROTOCOL', value: 'SASL_SSL' }
  { name: 'KAFKA_SASL_MECHANISM', value: 'PLAIN' }
  { name: 'KAFKA_SASL_USERNAME', value: '$ConnectionString' }
  { name: 'KAFKA_SASL_PASSWORD', secretRef: kafkaSecretName }
  { name: 'MONGO_URI', secretRef: mongoSecretName }
  { name: 'MONGO_DB', value: cosmosDatabaseName }
  { name: 'FRAUD_AMOUNT_THRESHOLD', value: fraudAmountThreshold }
  { name: 'FRAUD_RAPID_TXN_WINDOW_SECONDS', value: fraudRapidTxnWindowSeconds }
  { name: 'FRAUD_RAPID_TXN_COUNT', value: fraudRapidTxnCount }
  { name: 'FRAUD_HIGH_RISK_COUNTRIES', value: fraudHighRiskCountries }
]

resource api 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-api'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: identityRef
  }
  properties: {
    environmentId: containerAppsEnvironmentId
    configuration: {
      registries: sharedRegistries
      secrets: sharedSecrets
      ingress: {
        external: true
        targetPort: 8000
        transport: 'auto'
      }
    }
    template: {
      containers: [
        {
          name: 'api'
          image: '${acrLoginServer}/fraud-api:${imageTag}'
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: kafkaEnvCommon
          probes: [
            {
              type: 'Liveness'
              httpGet: { path: '/health', port: 8000 }
              initialDelaySeconds: 15
            }
          ]
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 3 }
    }
  }
}

resource spark 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-spark'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: identityRef
  }
  properties: {
    environmentId: containerAppsEnvironmentId
    configuration: {
      registries: sharedRegistries
      secrets: sharedSecrets
    }
    template: {
      containers: [
        {
          name: 'spark'
          image: '${acrLoginServer}/fraud-spark:${imageTag}'
          resources: { cpu: json('2.0'), memory: '4Gi' }
          env: concat(kafkaEnvCommon, [
            { name: 'SPARK_CHECKPOINT_DIR', value: '/mnt/checkpoints' }
          ])
          volumeMounts: [
            { volumeName: 'checkpoints', mountPath: '/mnt/checkpoints' }
          ]
        }
      ]
      volumes: [
        { name: 'checkpoints', storageType: 'AzureFile', storageName: envStorageName }
      ]
      // exactly one replica: it's a single stateful streaming query, not a
      // horizontally scalable request handler
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

resource producer 'Microsoft.App/containerApps@2024-03-01' = {
  name: '${namePrefix}-producer'
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: identityRef
  }
  properties: {
    environmentId: containerAppsEnvironmentId
    configuration: {
      registries: sharedRegistries
      secrets: sharedSecrets
    }
    template: {
      containers: [
        {
          name: 'producer'
          image: '${acrLoginServer}/fraud-producer:${imageTag}'
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: concat(kafkaEnvCommon, [
            { name: 'GEN_INTERVAL_SECONDS', value: genIntervalSeconds }
            { name: 'GEN_FRAUD_RATIO', value: genFraudRatio }
          ])
        }
      ]
      scale: { minReplicas: 1, maxReplicas: 1 }
    }
  }
}

output apiUrl string = 'https://${api.properties.configuration.ingress.fqdn}'
