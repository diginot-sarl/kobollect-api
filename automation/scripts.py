import requests
import json
import logging
from app.schemas import UserCreate
from tqdm import tqdm

logger = logging.getLogger(__name__)

async def create_kobo_account(user_data: UserCreate):
    """
    Route de test pour créer un compte KoboToolbox via l'API
    et vérifier les utilisateurs après 1 minute
    """
    KOBOTOOLBOX_URL = "http://kf.hidscollect.com"
    KOBOTOOLBOX_ADMIN_USER = "super_admin"
    KOBOTOOLBOX_ADMIN_PASSWORD = "LIRV8Bfq5bkjJ3tspxhL"

    user_info = {
        "username": user_data.login,
        "password": 123456,
        "email": user_data.mail,
        "first_name": user_data.prenom,
        "last_name": user_data.nom
    }

    kobotoolbox_api_url = f"{KOBOTOOLBOX_URL}/api/v2/users/"
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(
            kobotoolbox_api_url,
            headers=headers,
            data=json.dumps(user_info),
            auth=(KOBOTOOLBOX_ADMIN_USER, KOBOTOOLBOX_ADMIN_PASSWORD)
        )
        
        if response.status_code == 201:
            return logger.info(f"Compte créé avec succès: {response.json()}")
        else:
            return logger.warning(f"Échec de la création du compte: {response.text}")
            
    except requests.exceptions.RequestException as e:
        return logger.error(f"Erreur de requête {str(e)}")

async def main():
    # Load users from JSON file
    with open('assets/import_users.json', 'r') as f:
        users = json.load(f)
    
    # Process each user with progress bar
    for user in tqdm(users, desc="Creating Kobo accounts", unit="user"):
        user_data = UserCreate(
            login=user['username'],
            prenom=user['metadata'].get('name', '').split(' ')[0] if user['metadata'].get('name') else '',
            nom=user['metadata'].get('name', '').split(' ')[-1] if user['metadata'].get('name') else '',
            mail=user['email']
        )
        # Create Kobo account with await
        await create_kobo_account(user_data)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())