import nltk
import json
from nltk.sentiment import SentimentIntensityAnalyzer
from googletrans import Translator
nltk.download('vader_lexicon')

class SentimentAnalysis:
    def get_sa():
        # Define SentimentIntensityAnalyzer
        sid = SentimentIntensityAnalyzer()
        # Define the translators
        translator = Translator()
        with open('result.json','r') as jd:
            data = json.load(jd)

        for item in data:
            link =item['Link']
            txt = item['Content']
            # Set text baseon indonesian language
            text_id = txt.strip()

            #Translate the text to English
            try:
                print(len(text_id))
                if len(text_id)>=5000:
                    text_en = trans_text(text_id)
                else:
                    text_en = translator.translate(text_id, dest='en').text
            except Exception as e:
                # Handle the error or retry translation
                print("Translation Error:", str(e))
            
            # Perform sentiment analysis on the translated text
            sentiment_scores = sid.polarity_scores(text_en)

            # Extract the sentiment polarity score
            compound_score = sentiment_scores["compound"]

            # Determine the sentiment label based on the compound score
            if compound_score >= 0.05:
                item['Sentiment'] = "positive"
            elif compound_score <= -0.05:
                item['Sentiment'] = "negative"
            else:
                item['Sentiment'] = "neutral"

            #Print the results
            #print("Text (Indonesian):", text_id)
            print("Text (English):", text_en)
            print("Sentiment Score:", compound_score)
            #print("Sentiment Label:", sentiment_label)
            with open('result.json','w+') as out:
                json.dump(data, out,indent=4)
            print("Update Complete!")

    def trans_text(text, dest='en'):
        translator = Translator(service_urls=['translate.google.com'])

        # Split the text into chunks of 5000 characters
        chunks = [text[i:i+5000] for i in range(0, len(text), 5000)]

        trans_chunks = []
        for chunk in chunks:
            translation = translator.translate(chunk, dest=dest)
            trans_chunks.append(translation.text)

        trans_text = ' '.join(trans_chunks)
        return trans_text
