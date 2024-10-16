import pandas as pd
import aiohttp
import asyncio
import random

# Google Translate URL
TRANSLATE_URL = "https://googleapis.com/"

async def translate_text(session, texts, target_language, cert_file, key_file, retries=3):
    params = {
        'q': texts,
        'target': target_language,
    }

    for attempt in range(retries):
        try:
            async with session.post(TRANSLATE_URL, json=params, ssl=aiohttp.ClientSslContext(cert_file, key_file)) as response:
                if response.status == 200:
                    data = await response.json()
                    return [translation['translatedText'] for translation in data['data']['translations']]
                else:
                    print(f"Error {response.status}: {await response.text()}")
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt + random.uniform(0, 1)  # Exponential backoff
                        print(f"Retrying in {wait_time:.2f} seconds...")
                        await asyncio.sleep(wait_time)

        except Exception as e:
            print(f"Exception occurred: {e}")
            if attempt < retries - 1:
                wait_time = 2 ** attempt + random.uniform(0, 1)
                print(f"Retrying in {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time)

    print("Max retries reached. Returning original texts.")
    return texts  # Return original texts if all retries failed

async def translate_batch(df, target_language, cert_file, key_file, max_chars, max_concurrent_requests):
    semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def sem_translated_text(session, texts):
        async with semaphore:
            return await translate_text(session, texts, target_language, cert_file, key_file)

    async with aiohttp.ClientSession() as session:
        tasks = []
        current_batch = []
        current_length = 0

        for text in df['text']:
            if current_length + len(text) + len(current_batch) <= max_chars:
                current_batch.append(text)
                current_length += len(text)
            else:
                if current_batch:
                    tasks.append(sem_translated_text(session, current_batch))
                current_batch = [text]
                current_length = len(text)

        if current_batch:
            tasks.append(sem_translated_text(session, current_batch))

        translations = await asyncio.gather(*tasks)
        return [item for sublist in translations for item in sublist]

def translate_dataframe(df, target_language, cert_file, key_file, max_chars, max_concurrent_requests):
    loop = asyncio.get_event_loop()
    translations = loop.run_until_complete(translate_batch(df, target_language, cert_file, key_file, max_chars, max_concurrent_requests))
    df['translated_text'] = translations
    return df

# Example usage
if __name__ == "__main__":
    data = {'text': ['Hello, world!', 'How are you?', 'Goodbye!', 'This is a test sentence.']}
    df = pd.DataFrame(data)

    target_language = 'es'  # Spanish
    cert_file = 'path/to/your/cert.pem'  # Path to your client certificate
    key_file = 'path/to/your/key.pem'     # Path to your private key
    max_chars = 5000  # Adjust based on API limits
    max_concurrent_requests = 5  # Max concurrent requests

    translated_df = translate_dataframe(df, target_language, cert_file, key_file, max_chars, max_concurrent_requests)
    print(translated_df)
