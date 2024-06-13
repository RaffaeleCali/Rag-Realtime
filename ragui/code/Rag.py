import streamlit as st
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import ElasticsearchStore
from elasticsearch import Elasticsearch
import os
import time


# Set environment variable for Hugging Face API token
os.environ["HUGGINGFACEHUB_API_TOKEN"] = "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"
print("chiave impostata")
# Initialize embedding model
embedding_model = HuggingFaceEmbeddings(
    model_name= "thenlper/gte-small",
    model_kwargs={"device": "cpu"}
)
print(embedding_model)
# Initialize Elasticsearch connection
es_connection = Elasticsearch("http://elasticsearch:9200")
es_indexd = "tes"

# Initialize Elasticsearch store
es_store = ElasticsearchStore(
    es_connection=es_connection,
    index_name=es_indexd,
    embedding=embedding_model,
    vector_query_field='vector',
    distance_strategy='COSINE'
)

print("connesso ad elastic")
# Initialize language model
#modell =

# Function to retrieve documents from Elasticsearch
def retrieve_documents(query):
    try:
        results = es_store.similarity_search(query=query, k=3)
        return results
    except Exception as e:
        print.error(f"Error retrieving documents: {e}")
        return []

# Function to print retriever results for debugging
def print_retriever_results(results):
    for doc in results:
        print(doc.page_content)

q = retrieve_documents("parlami di battiato")
print(q)
# Function to create the conversational chain
def get_conversational_chain():
    prompt_template = """
    Answer the question as detailed as possible from the provided context. If the answer is not in the context, just say, "answer is not available in the context", don't provide the wrong answer.\n\n
    Context:\n {context}?\n
    Question: \n{question}\n
    Answer:
    """
    model =  ChatOpenAI(
        base_url="http://ollama:11434/v1",
        temperature=0,
        api_key="not needed",
        model_name="gemma:2b",
    )
    prompt = PromptTemplate(template=prompt_template, input_variables=["context", "question"])
    chain = load_qa_chain(model, chain_type="stuff", prompt=prompt)
    return chain

# Function to handle user input and generate response
def handle_user_input(prompt):
    print(prompt)
    retriever_results = retrieve_documents(prompt)
    print_retriever_results(retriever_results)

    retrieved_docs = "\n\n".join([str(doc.page_content) for doc in retriever_results])
    chat_history = "".join([f"{msg['role']}: {msg['content']}\n" for msg in st.session_state.messages])

    agent_prompt = f"""
    Question: {prompt}
    Context: {retrieved_docs}
    Chat History: {chat_history}
    """
    
    chain = get_conversational_chain()
    response = chain({"input_documents": retriever_results, "question": prompt}, return_only_outputs=True)
    return response["output_text"]

# Streamlit page configuration
st.set_page_config(page_title="Document Genie", layout="wide")

st.markdown("""
This chatbot is built using the Real time Retrieval-Augmented Generation (RAG) framework
""")

# Initialize chat history in session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Handle user input
if prompt := st.chat_input("What is up?", key="first_question"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        # Retrieve documents and generate response
        response = handle_user_input(prompt)
    st.session_state.messages.append({"role": "assistant", "content": response})
    #for message in st.session_state.messages:
    #    with st.chat_message(message["role"]):
    #        st.markdown(message["content"])
