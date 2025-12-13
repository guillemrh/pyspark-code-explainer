import streamlit as st
import requests
import time

BACKEND_BASE = "http://backend:8000"  # internal when running in docker-compose

st.title("PySpark Code Explainer (LLM)")

code = st.text_area("PySpark code")

if st.button("Explain"):
    # 1. Submit job
    r = requests.post(
        f"{BACKEND_BASE}/explain/pyspark",
        json={"code": code},
        timeout=10,
    )
    data = r.json()
    job_id = data["job_id"]

    st.info(f"Job queued: {job_id}")

    # 2. Poll status
    with st.spinner("Waiting for result..."):
        while True:
            resp = requests.get(
                f"{BACKEND_BASE}/status/{job_id}",
                timeout=10,
            )
            j = resp.json()
            status = j.get("status")

            if status in ("finished", "failed"):
                llm_result = j.get("result")

                if llm_result:
                    st.write("**Explanation:**")
                    st.code(llm_result.get("explanation"))
                    st.write(
                        f"Tokens: {llm_result.get('tokens_used')}, "
                        f"Duration: {j.get('duration_ms')} ms"
                    )
                else:
                    st.error("No result returned.")

                break  # 🔴 CRITICAL: stop polling

            time.sleep(1)