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
from langchain.agents import create_tool_calling_agent
from langchain.agents import AgentExecutor
import streamlit as st
from elasticsearch import Elasticsearch
from langchain.callbacks.base import BaseCallbackHandler

# Streaming Handler
class StreamHandler(BaseCallbackHandler):
    def __init__(
        self, container: st.delta_generator.DeltaGenerator, initial_text: str = ""
    ):
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

load_dotenv(find_dotenv())





model = HuggingFaceEmbeddings(
    model_name="all-MiniLM-L6-v2", model_kwargs={"device": "cpu"}
)

text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200)

from langchain_openai import ChatOpenAI

model = ChatOpenAI(
    base_url = "http://localhost:11434",
    temperature = 0,
    api_key = "not needed",
    model_name = "gemma:2b",
    
)
 
es_cli = Elasticsearch("http://elasticsearch:9200")

es = ElasticsearchStore(
    index_name= es_index,
    es_url="http://elasticsearch:9200",
    embedding= HuggingFaceEmbeddings(
        model_name="all-MiniLM-L6-v2", model_kwargs={"device": "cpu"}
    ),
        )



retriever = es.as_retriever(search_type="mmr", search_kwargs={"k": 6})
retriever_tool = create_retriever_tool(
    retriever,
    "Helper realtime",
    "Help students and Search for information about University of Catania courses. For any questions about uni courses and their careers, you must use this tool for helping students!",
)
search = TavilySearchResults(max_results=3)
tools = [search, retriever_tool]

prompt = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            "You are a helpful assistant. Answer always in the language of the question",
        ),
        ("placeholder", "{chat_history}"),
        ("human", "{input}"),
        ("placeholder", "{agent_scratchpad}"),
    ]
)

# Create the agent
agent = create_tool_calling_agent(model, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

st.set_page_config(page_title="RealttimeRag", page_icon="🌐")
st.header("Your personal assistant , real time news 🤖")
st.write(
    """Hi. I am an agent powered by Raffaele.
I will be your virtual assistant to help you with new o personal news. 
Ask me anything about news recently"""
)
#st.write(
#    "[![view source code ](https://img.shields.io/badge/view_source_code-gray?logo=github)](https://github.com/neodatagroup/hackathon_RAG/tree/main)"
#)


if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

#with st.sidebar:
#    st.header("I tuoi corsi")
#    st.markdown("- INTRODUZIONE AL DATA MINING")
#    st.markdown("- BASI DI DATI A - L")
#    st.markdown("- ALGEBRA LINEARE E GEOMETRIA A - E")
#
#    st.divider()
#    
#    st.markdown("Chiedimi i contatti della segreteria!")


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
    # st.chat_message("assistant").markdown(response)
