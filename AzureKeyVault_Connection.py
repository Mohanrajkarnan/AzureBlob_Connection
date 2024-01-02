import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv


def azureAccessKey(secret_key):

    load_dotenv()

    client_id  = os.environ['AZURE_CLIENT_ID']
    tenant_id  = os.environ['AZURE_TENANT_ID']
    client_secret  = os.environ['AZURE_CLIENT_SECRET']
    valut_url  = os.environ['AZURE_VAULT_URL']

    blobSecretKey = secret_key

    credientials = ClientSecretCredential(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id
    )

    secretClient = SecretClient(credential=credientials,vault_url=valut_url)
    secretValue = secretClient.get_secret(blobSecretKey)
    return secretValue.value


