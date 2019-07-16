FROM tensorflow/tensorflow:1.12.3

MAINTAINER Siscale "dev@siscale.com"

RUN mkdir -p /opt/arcanna
WORKDIR /opt/arcanna 

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app.config.dist ./app.config
# Copy application to image 
COPY arcanna/ .


ENTRYPOINT ["python"]

CMD ["app.py"] 

