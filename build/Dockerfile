FROM python:3.9-alpine
ENV FLASK_APP=controller
RUN addgroup -g 1000 eo4eu && \
    adduser -D -u 1000 -G eo4eu eo4eu
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8080
RUN chown -R eo4eu:eo4eu /app
USER eo4eu
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]