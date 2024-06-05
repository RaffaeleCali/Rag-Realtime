
import streamlit as st  
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import ElasticsearchStore
from elasticsearch import Elasticsearch

# Inizializzazione del modello di embedding e del client Elasticsearch
embedding_model = HuggingFaceEmbeddings(
    model_name="all-MiniLM-L6-v2",
    model_kwargs={"device": "cpu"}
)

es_cli = Elasticsearch("http://elasticsearch:9200")
es_index = "spark-index"
es_store = ElasticsearchStore(
    index_name=es_index,
    es_url="http://elasticsearch:9200",
    embedding=embedding_model,
    distance_strategy="COSINE"
)

# Funzione di recupero documenti
def retrieve_documents(query):
    try:
        results = es_store.similarity_search(query=query, k=6)
        return results
    except Exception as e:
        print(f"Error retrieving documents: {e}")
        return []

# Funzione per visualizzare i risultati del retriever
def print_retriever_results(results):
    for doc in results:
        print(doc.page_content)

# Funzione per creare la catena conversazionale
def get_conversational_chain():
    prompt_template = """
    Answer the question as detailed as possible from the provided context. If the answer is not in the context, just say, "answer is not available in the context", don't provide the wrong answer.\n\n
    Context:\n {context}?\n
    Question: \n{question}\n
    Answer:
    """
    model = ChatOpenAI(
        base_url="http://ollama:11434/v1",
        temperature=0,
        api_key="not needed",
        model_name="gemma:2b",
    )
    prompt = PromptTemplate(template=prompt_template, input_variables=["context", "question"])
    chain = load_qa_chain(model, chain_type="stuff", prompt=prompt)
    return chain

# Funzione principale per gestire il flusso della chat
def handle_user_input(prompt):
    retriever_results = retrieve_documents(prompt)
    print_retriever_results(retriever_results)

    retrieved_docs = "\n\n".join([str(doc.page_content) for doc in retriever_results])
    
    chat_history = "".join([f"{msg['role']}: {msg['content']}\n" for msg in st.session_state.messages])
    
    agent_prompt = f"""
    
    Question: {prompt}
    Context: {retrieved_docs}
    Chat History: {chat_history}
    """
    #Chat History: {chat_history}
    
    chain = get_conversational_chain()
    response = chain({"input_documents": retriever_results, "question": prompt}, return_only_outputs=True)
    return response["output_text"]

# Configurazione della pagina Streamlit
st.set_page_config(page_title="Document Genie", layout="wide")

st.markdown("""
This chatbot is built using the Retrieval-Augmented Generation (RAG) framework
""")

# Inizializza la cronologia della chat nello stato della sessione
if "messages" not in st.session_state:
    st.session_state.messages = []

# Mostra la cronologia della chat
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Gestisci l'input dell'utente
if prompt := st.chat_input("What is up?", key="first_question"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # Recupera documenti e genera risposta
        response = handle_user_input(prompt)

    # Aggiungi la risposta alla cronologia della chat
    st.session_state.messages.append({"role": "assistant", "content": response})
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])