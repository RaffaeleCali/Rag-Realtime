import streamlit as st
from langchain.chains.question_answering import load_qa_chain
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import ElasticsearchStore
from elasticsearch import Elasticsearch
import os
import time

# Streamlit page configuration
st.set_page_config(page_title="Document Genie", layout="wide")

# Set environment variable for Hugging Face API token
os.environ["HUGGINGFACEHUB_API_TOKEN"] = "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"
print("API key set", flush=True)

os.environ["TOKENIZERS_PARALLELISM"] = "false"

# Initialize embedding model
@st.cache_resource
def load_embedding_model():
    model = HuggingFaceEmbeddings(
        model_name="thenlper/gte-small",
        model_kwargs={"device": "cpu"}
    )
    return model


# Initialize Elasticsearch connection
@st.cache_resource
def load_elasticsearch():
    return Elasticsearch("http://elasticsearch:9200")

# Initialize Elasticsearch store
@st.cache_resource
def load_elasticsearch_store(_es_connection, _embedding_model):
    return ElasticsearchStore(
        es_connection=_es_connection,
        index_name="def",
        embedding=_embedding_model,
        vector_query_field='vector',
        distance_strategy='COSINE'
    )

# Function to create the conversational chain
@st.cache_resource
def get_conversational_chain():
    prompt_template = """
    Try answering the question with context, if you don't use context you only say by putting dashes - - not by context \n\n
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
    return load_qa_chain(model, chain_type="stuff", prompt=prompt)

# Function to retrieve documents from Elasticsearch
def retrieve_documents(query):
    results = es_store.similarity_search(query=query, k=3)
    return results

# Function to print retriever results for debugging
def print_retriever_results(results):
    print("Documents retrieved from the database:", flush=True)
    for doc in results:
        print(doc.page_content, flush=True)

# Function to handle user input and generate response
def handle_user_input(prompt, chain):
    retriever_results = retrieve_documents(prompt)
    print("User question:", prompt, flush=True)
    print_retriever_results(retriever_results)

    retrieved_docs = "\n\n".join([str(doc.page_content) for doc in retriever_results])
    chat_history = "".join([f"{msg['role']}: {msg['content']}\n" for msg in st.session_state.messages])

    agent_prompt = f"""
    Question: {prompt}
    Context: {retrieved_docs}
    Chat History: {chat_history}
    """
    
    response = chain({"input_documents": retriever_results, "question": prompt}, return_only_outputs=True)
    return response["output_text"]

if __name__ == "__main__":
    embedding_model = load_embedding_model()
    es_connection = load_elasticsearch()
    es_store = load_elasticsearch_store(es_connection, embedding_model)
    chain = get_conversational_chain()
    print("All models and connections loaded", flush=True)

    st.markdown("""
    This chatbot is built using the Real-time Retrieval-Augmented Generation (RAG) framework
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
            response = handle_user_input(prompt, chain)
            st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})
