FROM python:3.9-alpine
ENV FLASK_APP=controller
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 8080
ENTRYPOINT [ "flask" ]
CMD [ "run","--port","8080","--host","0.0.0.0" ]
