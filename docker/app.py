

class CustomEmbeddingFunction:
    def __init__(self, ):
        self.model = model

    def __call__(self, input):
        # Assicurati che 'input' sia una lista di stringhe
        if isinstance(input, list):
            return [self.generate_embeddings(text) for text in input]
        else:
            return [self.generate_embeddings(input)]

    def generate_embeddings(self, text):
        # Assicurati che il testo non sia vuoto
        if text:
            embeddings = self.model.encode([text], convert_to_tensor=False)
            return embeddings.tolist()[0]
        else:
            return []
# Creazione dell'istanza della classe di funzione di embedding
embedding_function = CustomEmbeddingFunction()
!rm -rf home/jovyan/chroma
# Utilizzo dell'istanza per aggiungere testi
client = chromadb.PersistentClient(path="your_storage_path")
collection = client.get_or_create_collection(name="test", embedding_function=embedding_function)

# Preparazione dei dati di testo
sentences = ["Who is Laurens van der Maaten?", "What is machine learning?"]

# Aggiunta dei testi e degli embeddings calcolati alla collezione
collection.add(documents=sentences, ids = ["id4","id5"])
