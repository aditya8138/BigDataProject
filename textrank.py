import spacy
nlp = spacy.load('es_core_news_sm')
from textRankKeyword import *


content ='El giro de 180 grados de Pablo Casado tras su dura derrota electoral se completó este lunes en La Moncloa, donde inauguró un tono muy diferente con Pedro Sánchez, al que hasta hace poco llamaba traidor, felón, okupa o sucedáneo de presidente. Las elecciones han marcado un cambio de rumbo de tal calibre que ambos pactaron abrir un canal de comunicación permanente sobre Cataluña, algo impensable hace unas semanas. Casado explicó que el PP no facilitará con su abstención la investidura de Sánchez, pero invitó a Ciudadanos a hacerlo para evitar que el Gobierno de España dependa de los independentistas.'

doc = nlp(content)

candidate_pos = ['NOUN', 'PROPN', 'VERB']
sentences = []

# for sent in doc.sents:
#     selected_words = []
#     for token in sent:
#         if token.pos_ in candidate_pos and token.is_stop is False:
#             selected_words.append(token)
#     sentences.append(selected_words)
#
# print(sentences)


tr4w = TextRank4Keyword()
tr4w.analyze(content, candidate_pos = ['NOUN', 'PROPN','VERB'], window_size=4, lower=False)
tr4w.get_keywords(15)