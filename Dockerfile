FROM tensorflow/tensorflow

MAINTAINER Siscale "dev@siscale.com"

RUN mkdir -p /opt/arcanna
WORKDIR /opt/arcanna 

COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application to image 
COPY arcanna/ .


ENTRYPOINT["python"]

CMD ["app.py"] 

