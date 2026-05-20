import streamlit as st
import redis
import json
import asyncio
import nats

st.set_page_config(page_title="Credit Scoring Monitor")
st.title("Credit Scoring System Monitor")

rdb = redis.Redis(host='redis', port=6379, decode_responses=True)

st.subheader("Recent Applications")
recent_ids = rdb.lrange("recent_results", 0, 19)
if recent_ids:
    for cid in recent_ids:
        data = rdb.get(f"result:{cid}")
        if data:
            result = json.loads(data)
            with st.expander(f"Client {cid}"):
                st.json(result)
else:
    st.write("No applications yet")

st.subheader("Submit Test Application")
client_id = st.text_input("Client ID", "test123")
if st.button("Submit"):
    async def send_req():
        nc = await nats.connect("nats://nats:4222")
        msg = await nc.request("scoring.request", json.dumps({"client_id": client_id}).encode(), timeout=15)
        result = json.loads(msg.data)
        await nc.close()
        return result
    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(send_req())
    st.json(result)