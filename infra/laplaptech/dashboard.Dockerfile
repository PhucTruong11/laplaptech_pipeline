FROM laplaptech-runtime:local-v1
COPY --chown=pipeline:pipeline streamlit_app ./streamlit_app
EXPOSE 8501
ENTRYPOINT ["python", "-m", "streamlit", "run", "streamlit_app/app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true", "--browser.gatherUsageStats=false"]
