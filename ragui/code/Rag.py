from dotenv import load_dotenv, find_dotenv
from langchain_community.vectorstores import ElasticsearchStore
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain import hub
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.tools.retriever import create_retriever_tool
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain.agents import create_tool_calling_agent, AgentExecutor
import streamlit as st
from elasticsearch import Elasticsearch
from langchain.callbacks.base import BaseCallbackHandler
import os

# Streaming Handler
class StreamHandler(BaseCallbackHandler):
    def __init__(self, container: st.delta_generator.DeltaGenerator, initial_text: str = ""):
        self.container = container
        self.text = initial_text
        self.run_id_ignore_token = None

    def on_llm_start(self, serialized: dict, prompts: list, **kwargs):
        # Workaround to prevent showing the rephrased question as output
        if prompts[0].startswith("Human"):
            self.run_id_ignore_token = kwargs.get("run_id")

    def on_llm_new_token(self, token: str, **kwargs) -> None:
        if self.run_id_ignore_token == kwargs.get("run_id", False):
            return
        self.text += token
        self.container.markdown(self.text)

# Load environment variables
load_dotenv(find_dotenv())

# Initialize embeddings model
embedding_model = HuggingFaceEmbeddings(
    model_name="all-MiniLM-L6-v2",
    model_kwargs={"device": "cpu"}
)

# Initialize text splitter
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)

# Initialize chat model
chat_model = ChatOpenAI(
    base_url="http://ollama:11434/v1",
    temperature=0,
    api_key="not needed",
    model_name="gemma:2b",
)

# Initialize Elasticsearch client
es_cli = Elasticsearch("http://elasticsearch:9200")
es_index = "spark-index"
es_store = ElasticsearchStore(
    index_name=es_index,
    es_url="http://elasticsearch:9200",
    embedding=embedding_model,
)

# Create retriever tool
retriever = es_store.as_retriever(search_type="mmr", search_kwargs={"k": 6})
retriever_tool = create_retriever_tool(
    retriever,
    "Helper realtime",
    "Help students and Search for information about University of Catania courses. For any questions about uni courses and their careers, you must use this tool for helping students!",
)

# Tavily Search Results (disabled API key input for simplicity)
# os.environ["TAVILY_API_KEY"] = getpass.getpass()
#search = TavilySearchResults(max_results=3)
tools = [retriever_tool]

# Define chat prompt template
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", "You are a helpful assistant. Answer always in the language of the question"),
        ("placeholder", "{chat_history}"),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)

# Create the agent
agent = create_tool_calling_agent(chat_model, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Streamlit setup
st.set_page_config(page_title="RealtimeRag", page_icon="🌐")
st.header("Your personal assistant, real-time news 🤖")
st.write(
    """Hi. I am an agent powered by Raffaele.
I will be your virtual assistant to help you with news or personal data. 
Ask me anything about news recently"""
)

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
        stream_handler = StreamHandler(st.empty())
        # Execute the agent with chat history
        result = agent_executor(
            {
                "input": prompt,
                "chat_history": [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages
                ],
            },
            callbacks=[stream_handler],
        )
        response = result.get("output")

    st.session_state.messages.append({"role": "assistant", "content": response})
