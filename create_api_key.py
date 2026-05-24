"""
create_api_key.py
Luo Polymarket CLOB API -avaimet MetaMask private keyllä.
Aja kerran — tallentaa avaimet .env-tiedostoon automaattisesti.
"""

from py_clob_client_v2 import ClobClient

# ----------------------------------------------------------------
# TÄYTÄ TÄHÄN oma private key (0x-alkuinen)
# Älä jaa tätä tiedostoa kenellekään
# ----------------------------------------------------------------
PRIVATE_KEY = "LIITÄ_TÄHÄN_PRIVATE_KEY"
# ----------------------------------------------------------------

def main():
    print("Yhdistetään Polymarket CLOB API:iin...")

    # Luo client
    client = ClobClient(
        host="https://clob.polymarket.com",
        key=PRIVATE_KEY,
        chain_id=137,
        signature_type=2,
    )

    print("Luodaan API-avaimet...")
    try:
        api_creds = client.create_or_derive_api_creds()

        key        = api_creds.api_key
        secret     = api_creds.api_secret
        passphrase = api_creds.api_passphrase

        print("\n✅ API-avaimet luotu onnistuneesti!")
        print(f"CLOB_API_KEY={key}")
        print(f"CLOB_API_SECRET={secret}")
        print(f"CLOB_PASSPHRASE={passphrase}")

        # Tallenna .env-tiedostoon
        env_path = ".env"
        with open(env_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Korvaa tyhjät arvot
        content = content.replace("CLOB_API_KEY=", f"CLOB_API_KEY={key}")
        content = content.replace("CLOB_API_SECRET=", f"CLOB_API_SECRET={secret}")
        content = content.replace("CLOB_PASSPHRASE=", f"CLOB_PASSPHRASE={passphrase}")

        with open(env_path, "w", encoding="utf-8") as f:
            f.write(content)

        print("\n✅ Avaimet tallennettu .env-tiedostoon automaattisesti.")
        print("Voit nyt poistaa private keyn tästä tiedostosta.")

    except Exception as e:
        print(f"\n❌ Virhe: {e}")
        print("Tarkista että private key on oikein ja MetaMask-tili on aktiivinen.")

if __name__ == "__main__":
    main()
