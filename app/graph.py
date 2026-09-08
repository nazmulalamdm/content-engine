import asyncio
import os
from typing import TypedDict, List
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END
from app.config import settings

# ChatOpenAI-এর বদলে ChatGroq ব্যবহার করা হয়েছে
llm = ChatGroq(
    model="openai/gpt-oss-20b",
    temperature=0.3,
    api_key=getattr(settings, "GROQ_API_KEY", os.getenv("GROQ_API_KEY"))
)

class OrchestrationState(TypedDict):
    job_id: str
    user_id: str
    raw_content: str
    key_points: List[str]
    linkedin_post: str
    twitter_thread: List[str]
    newsletter: str

# ১. এক্সট্রাক্টর নোড
async def extract_keypoints_node(state: OrchestrationState) -> dict:
    prompt = [
        SystemMessage(content="Extract exactly 5 core key points from the text. Respond with bullet points only."),
        HumanMessage(content=state["raw_content"])
    ]
    res = await llm.ainvoke(prompt)
    points = [p.strip("- ").strip() for p in res.content.split("\n") if p.strip()]
    return {"key_points": points}

# ২. সমান্তরাল ওয়ার্কার নোডসমূহ (Fan-out)
async def generate_linkedin_node(state: OrchestrationState) -> dict:
    prompt = [
        SystemMessage(content="You are an expert LinkedIn ghostwriter. Create a professional, hook-driven post with clear line breaks and hashtags."),
        HumanMessage(content="\n".join(state["key_points"]))
    ]
    res = await llm.ainvoke(prompt)
    return {"linkedin_post": res.content}

async def generate_twitter_node(state: OrchestrationState) -> dict:
    prompt = [
        SystemMessage(content="Create a 5-tweet viral thread from the key points. Separate each tweet with '---'."),
        HumanMessage(content="\n".join(state["key_points"]))
    ]
    res = await llm.ainvoke(prompt)
    tweets = [t.strip() for t in res.content.split("---") if t.strip()]
    return {"twitter_thread": tweets}

async def generate_newsletter_node(state: OrchestrationState) -> dict:
    prompt = [
        SystemMessage(content="Write a structured, deep-dive newsletter issue with Subtitles, Executive Summary, and Takeaways."),
        HumanMessage(content="\n".join(state["key_points"]))
    ]
    res = await llm.ainvoke(prompt)
    return {"newsletter": res.content}

# ৩. গ্রাফ কম্পাইলেশন
def build_orchestrator_graph():
    builder = StateGraph(OrchestrationState)
    builder.add_node("extractor", extract_keypoints_node)
    builder.add_node("linkedin", generate_linkedin_node)
    builder.add_node("twitter", generate_twitter_node)
    builder.add_node("newsletter", generate_newsletter_node)

    builder.add_edge(START, "extractor")
    
    # সমান্তরাল প্রসেসিং (Fan-out)
    builder.add_edge("extractor", "linkedin")
    builder.add_edge("extractor", "twitter")
    builder.add_edge("extractor", "newsletter")

    # একত্রীকরণ (Fan-in)
    builder.add_edge("linkedin", END)
    builder.add_edge("twitter", END)
    builder.add_edge("newsletter", END)

    return builder.compile()

orchestrator_app = build_orchestrator_graph()