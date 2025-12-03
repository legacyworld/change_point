FROM python:bullseye
USER root
WORKDIR /src
RUN apt update
RUN pip install elasticsearch python-dotenv　
